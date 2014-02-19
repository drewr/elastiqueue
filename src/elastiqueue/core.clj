(ns elastiqueue.core
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]
            [elastiqueue.log :as log]
            [elastiqueue.url :as url]))

(defrecord Exchange [uri name])

(defrecord Queue [exchange name])

(def status-field
  :__q_status)

(def control-field
  :__q_control)

(defn consumable-query [queue-name]
  {:query
   {:bool
    {:must [{:constant_score
             {:filter
              {:missing
               {:field status-field
                :existence true
                :null_value true}}}}
            {:prefix
             {:_type queue-name}}]}}
   :sort [{control-field {:order "desc"}}]
   :size 1})

(defn wait-for-health [exch status]
  (http/get (url/health exch status)))

(defn declare-exchange [uri name &
                        {:keys [shards replicas]
                         :or {shards 1
                              replicas 0}
                         :as settings}]
  (let [exch (->Exchange uri name)]
    (try+
     (http/put (format "%s/%s" (:uri exch) (:name exch))
               {:body (json/encode
                       {:settings
                        (merge
                         {:number_of_shards shards
                          :number_of_replicas replicas}
                         settings)})})
     (wait-for-health exch :yellow)
     exch
     (catch [:status 400] _ exch))))

(defn declare-queue [exch qname]
  (let [q (->Queue exch qname)]
    (http/put
     (format "%s/%s/%s/_mapping"
             (:uri exch)
             (:name exch)
             (:name q))
     {:body (json/encode
             {(:name q)
              {:properties
               {control-field {:type :string}}}})
      :throw-entire-message? true})
    q))

(defn delete-exchange [^Exchange exch]
  (http/delete
   (format "%s/%s" (:uri exch) (:name exch))))

(defn delete-queue [^Queue q]
  (http/delete
   (format "%s/%s/%s"
           (-> q :exchange :uri)
           (-> q :exchange :name)
           (-> q :name))))

(defn post-message [^Queue queue payload]
  (http/post (url/publish queue)
             {:body (json/encode payload)}))

(defn post-bulk [^Queue queue bulk]
  (http/post (url/bulk queue) {:body bulk}))

(defn delete-control [^Queue queue]
  (http/delete (url/delete-control queue control-field)))

(defn publish [^Queue queue payload]
  (let [resp (post-message queue payload)]
    (wait-for-health (:exchange queue) :yellow)
    resp))

(defn make-indexable-bulk [coll]
  (str
   (->> (interleave
         (map json/encode (repeat {:index {}}))
         (map json/encode coll))
        (interpose "\n")
        (apply str))
   "\n"))

(defn take-bytes [bytes coll]
  (lazy-seq
   (when (pos? bytes)
     (when-let [s (seq coll)]
       (cons (first s) (take-bytes
                        (- bytes (count (first s)))
                        (rest coll)))))))

(defn partition-bytes [bytes coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (let [seg (doall (take-bytes bytes s))]
       (cons seg (partition-bytes bytes (nthrest s (count seg))))))))

(defn make-bulk-op [doc]
  (format "%s\n%s\n"
          (json/encode {:index {}})
          (json/encode doc)))

(defn partition-indexable-bulk [bytes coll]
  (->> coll
       (map make-bulk-op)
       (partition-bytes bytes)))

(defn bulk-summary [response]
  (->> (json/decode (:body response) true)
       :items
       (map vals)
       (map first)
       (reduce (fn [res item]
                 (if (:ok item)
                   (update-in res [:success] inc)
                   (update-in res [:errors] inc)))
               {:success 0 :errors 0})))

(defn publish-seq [^Queue queue coll & {:keys [bytes]
                                        :or {bytes 1024}}]
  (let [resps (->> coll
                   (partition-indexable-bulk bytes)
                   (map (partial apply str))
                   (pmap (partial post-bulk queue))
                   (map bulk-summary))]
    (reduce (fn [acc resp]
              (merge-with + (update-in acc [:bulks] (fnil inc 0))
                          resp))
            {} resps)))

(defn get-msg [queue id]
  (json/decode (:body (http/get (url/get queue id))) true))

(defn consumables [^Queue queue]
  (let [response (http/get (url/search queue)
                           {:body (json/encode (consumable-query
                                                (:name queue)))
                            :throw-exceptions true})]
    (json/decode (:body response) true)))

(defn queue-size [^Queue queue]
  (-> (consumables queue) :hits :total))

(defn disable-queue-and-kill-consumers [^Queue queue]
  (publish queue {control-field :die}))

(defn pause-queue [^Queue queue]
  (publish queue {control-field :pause}))

(defn enable-queue [^Queue queue]
  (delete-control queue))

(defn paused? [^Queue queue]
  (let [response (http/post (url/count queue)
                            {:body
                             (json/encode
                              {:match
                               {control-field "pause"}})})
        total (-> response :body (#(json/decode % true)) :count)]
    (pos? total)))

(defn disabled? [^Queue queue]
  (let [response (http/post (url/count queue)
                            {:body
                             (json/encode
                              {:match
                               {control-field "die"}})})
        total (-> response :body (#(json/decode % true)) :count)]
    (pos? total)))

(defn unack-count [^Queue queue]
  (let [response (http/post (url/count queue)
                            {:body
                             (json/encode
                              {:match
                               {status-field "unack"}})})]
    (-> response :body (#(json/decode % true)) :count)))

(defn update-status [msg status]
  (let [payload (assoc (:_source msg) status-field status)
        queue (->Queue
               (->Exchange (:_uri msg) (:_index msg))
               (:_type msg))
        response (http/put (url/update queue (:_id msg) (:_version msg))
                           {:body (json/encode payload)})]
    (json/decode (:body response) true)))

(defn unack [msg]
  (update-status msg :unack))

(defn ack [msg]
  (let [msg (assoc msg
              :_version (inc (:_version msg)))]
    (update-status msg :ack)))

(defn head [^Queue queue]
  (when-let [msg (-> (consumables queue) :hits :hits first)]
    (assoc msg
      :_uri (-> queue :exchange :uri))))

(defn control-msg? [msg]
  (-> msg :_source control-field))

(defn consume-msg [^Queue queue]
  (try+
   (when-let [msg (head queue)]
     (if (control-msg? msg)
       (-> msg :_source control-field)
       (do
         (unack msg)
         msg)))
   (catch [:status 409] _)
   (catch [:status 404] _)))

(defn consume-wait [^Queue queue wait retry]
  #_(log/log 'retry retry)
  (when (pos? retry)
    (if-let [msg (consume-msg queue)]
      msg
      (do
        (Thread/sleep wait)
        (recur queue wait (dec retry))))))

(defn consume
  ([^Queue queue]
     (consume queue 150 20))
  ([^Queue queue f]
     (f (consume queue)))
  ([^Queue queue wait retry]
     (consume-wait queue wait retry))
  ([^Queue queue wait retry f]
     (f (consume queue wait retry))))

(defn sleep [ms]
  (log/log 'sleep)
  (.sleep java.util.concurrent.TimeUnit/MILLISECONDS ms))

(defn consume-poll
  ([^Queue queue f]
     (consume-poll queue 5000 f))
  ([^Queue queue ^Integer poll-ms f]
     (let [msg (consume-msg queue)]
       (when-not (= msg "die")
         (if (or (nil? msg) (= msg "pause"))
           (sleep poll-ms)
           (f msg))
         (recur queue poll-ms f)))))

(defn handle-msg [msg])

(comment
  (def exch
    (declare-exchange "http://localhost:9200" "queuetest"))

  (def q
    (declare-queue exch "test.foo"))

  (do
    (delete-exchange exch))

  (publish-seq q (map #(hash-map :n %) (range 10)))
  (queue-size q)
  (consume-msg q)

  (def worker
    (let [c (atom 0)]
      (.start
       (Thread.
        (fn []
          (consume-poll q 5000
                        (fn [msg]
                          (handle-msg msg)
                          (swap! c inc)))
          (log/log 'die c))))
      c))


  )
