(ns newsroom.core
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]
            [newsroom.log :as log]))

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

(defn publish [^Queue queue payload]
  (let [resp (post-message queue payload)]
    (wait-for-health (:uri queue) :yellow)
    resp))

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

(defn consume* [queue wait retry]
  #_(log/log 'retry retry)
  (when (pos? retry)
    (if-let [msg (try+
                   (when-let [msg (head queue)]
                     (unack msg)
                     msg)
                   (catch [:status 409] _))]
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
     (consume* queue wait retry))
  ([^Queue queue wait retry f]
     (f (consume queue wait retry))))

(defn consume-poll
  ([^Queue queue f]
     (consume-poll queue 5000 f))
  ([^Queue queue ^Integer poll-ms f]
      (consume queue 0 1 f)
      (Thread/sleep poll-ms)
      (recur queue poll-ms f)))
