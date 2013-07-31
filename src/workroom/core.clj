(ns workroom.core
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]
            [workroom.log :as log]))

(defrecord Queue [uri exchange name])

(def status-field
  :__q_status)

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
   :size 1})

(defn consumable-url [queue]
  (format (str "%s/%s/%s/_search?"
               "preference=_primary"
               "&version=true")
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

(defn health-url [es status]
  (format "%s/_cluster/health?wait_for_status=%s"
          es (name status)))

(defn wait-for-health [es status]
  (http/get (health-url es status)))

(defn post-message [^Queue queue payload]
  (http/post (publish-url queue)
             {:body (json/encode payload)}))

(defn post-bulk [^Queue queue bulk]
  (http/post (bulk-url queue) {:body bulk}))

(defn publish [^Queue queue payload]
  (let [resp (post-message queue payload)]
    (wait-for-health (:uri queue) :yellow)
    resp))

(defn make-indexable-bulk [coll]
  (str
   (->> (interleave
         (map json/encode (repeat {:index {}}))
         (map json/encode coll))
        (interpose "\n")
        (apply str))
   "\n"))

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

(defn publish-seq [^Queue queue coll]
  (let [resp (post-bulk queue (make-indexable-bulk coll))]
    (bulk-summary resp)))

(defn get-msg [queue id]
  (json/decode (:body (http/get (get-url queue id))) true))

(defn consumables [^Queue queue]
  (let [response (http/get (consumable-url queue)
                           {:body (json/encode (consumable-query
                                                (:name queue)))})]
    (json/decode (:body response) true)))

(defn queue-size [^Queue queue]
  (-> (consumables queue) :hits :total))

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

(defn consume-msg [queue]
  (try+
    (when-let [msg (head queue)]
      (unack msg)
      msg)
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

(defn consume-poll
  ([^Queue queue f]
     (consume-poll queue 5000 f))
  ([^Queue queue ^Integer poll-ms f]
     (if-let [msg (consume-msg queue)]
       (f msg)
       (do
         (log/log 'sleep)
         (Thread/sleep poll-ms)))
     (recur queue poll-ms f)))

(comment
  (.start
   (Thread.
    (fn []
      (let [q (->Queue "http://localhost:9200"
                       "queuetest"
                       "test.foo")]
        (consume-poll q 5000
                      (fn [msg]
                        (log/log 'got (-> msg :_id))))))
    (str "worker-" (rand-int 1000))))


  )
