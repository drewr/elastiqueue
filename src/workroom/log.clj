(ns workroom.log)

(def logger
  (agent nil))

(defn log [& msgs]
  (let [msg (with-out-str
              (apply print (.getName (Thread/currentThread)) msgs))]
    (send logger
          (fn [_]
            (println msg)))))
