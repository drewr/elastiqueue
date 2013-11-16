(ns elastiqueue.core
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]
            [elastiqueue.log :as log]))

(defrecord Queue [uri exchange name])

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

(defn search-url [queue]
  (format (str "%s/%s/%s/_search?"
               "preference=_primary"
               "&version=true")
          (:uri queue) (:exchange queue) (:name queue)))

(defn count-url [queue]
  (format (str "%s/%s/%s/_count?"
               "preference=_primary")
          (:uri queue) (:exchange queue) (:name queue)))

(defn publish-url [^Queue queue]
  (format "%s/%s/%s" (:uri queue) (:exchange queue) (:name queue)))

(defn bulk-url [^Queue queue]
  (format "%s/%s/%s/_bulk" (:uri queue) (:exchange queue) (:name queue)))

(defn get-url [^Queue queue id]
  (format "%s/%s/%s/%s"
          (:uri queue) (:exchange queue) (:name queue) id))

(defn update-url [^Queue queue id version]
  (format "%s/%s/%s/%s?version=%d&refresh=true"
          (:uri queue) (:exchange queue) (:name queue) id version))

(defn health-url [q status]
  (format "%s/_cluster/health/%s?wait_for_status=%s"
          (:uri q) (:exchange q) (name status)))

(defn delete-control-url [^Queue queue]
  (format "%s/%s/%s/_query?q=%s:die OR %s:pause"
          (:uri queue)
          (:exchange queue)
          (:name queue)
          (name control-field)
          (name control-field)))

(defn wait-for-health [q status]
  (http/get (health-url q status)))

(defn declare-exchange [q & {:keys [shards replicas]
                             :or {shards 1
                                  replicas 0}
                             :as settings}]
  (http/put (format "%s/%s" (:uri q) (:exchange q))
            {:body (json/encode
                    {:settings
                     (merge
                      {:number_of_shards shards
                       :number_of_replicas replicas}
                      settings)
                     :mappings
                     {(:name q)
                      {:properties
                       {control-field {:type :string}}}}})})
  (wait-for-health q :yellow)
  q)

(defn delete-exchange [q]
  (http/delete (format "%s/%s" (:uri q) (:exchange q))))

(defn delete-queue [q]
  ;; todo
  )

(defn post-message [^Queue queue payload]
  (http/post (publish-url queue)
             {:body (json/encode payload)}))

(defn post-bulk [^Queue queue bulk]
  (http/post (bulk-url queue) {:body bulk}))

(defn delete-control [^Queue queue]
  (http/delete (delete-control-url queue)))

(defn publish [^Queue queue payload]
  (let [resp (post-message queue payload)]
    (wait-for-health queue :yellow)
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
  (json/decode (:body (http/get (get-url queue id))) true))

(defn consumables [^Queue queue]
  (let [response (http/get (search-url queue)
                           {:body (json/encode (consumable-query
                                                (:name queue)))})]
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
  (let [response (http/post (count-url queue)
                            {:body
                             (json/encode
                              {:match
                               {control-field "pause"}})})
        total (-> response :body (#(json/decode % true)) :count)]
    (pos? total)))

(defn disabled? [^Queue queue]
  (let [response (http/post (count-url queue)
                            {:body
                             (json/encode
                              {:match
                               {control-field "die"}})})
        total (-> response :body (#(json/decode % true)) :count)]
    (pos? total)))

(defn unack-count [^Queue queue]
  (let [response (http/post (count-url queue)
                            {:body
                             (json/encode
                              {:match
                               {status-field "unack"}})})]
    (-> response :body (#(json/decode % true)) :count)))

(defn update-status [msg status]
  (let [payload (assoc (:_source msg) status-field status)
        queue (->Queue (:_uri msg) (:_index msg) (:_type msg))
        response (http/put (update-url queue (:_id msg) (:_version msg))
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
      :_uri (:uri queue))))

(defn control-msg? [msg]
  (-> msg :_source control-field))

(defn consume-msg [queue]
  (try+
    (when-let [msg (head queue)]
      (if (control-msg? msg)
        (-> msg :_source control-field)
        (do
          (unack msg)
          msg)))
    (catch [:status 409] _)))

(defn consume-wait [queue wait retry]
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
  (def q (->Queue "http://localhost:9200"
                  "queuetest"
                  "test.foo"))

  (do
    (delete-exchange q)
    (declare-exchange q))

  (publish-seq q (map #(hash-map :n %) (range 10)))

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
