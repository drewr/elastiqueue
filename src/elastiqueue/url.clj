(ns elastiqueue.url
  (:refer-clojure :exclude [get count]))

(defn prefix [q]
  (format "%s/%s/%s"
          (-> q :exchange :uri)
          (-> q :exchange :name)
          (-> q :name)))

(defn search [queue]
  (format (str "%s/_search?"
               "version=true") (prefix queue)))

(defn count [queue]
  (format (str "%s/_count?"
               "preference=_primary") (prefix queue)))

(defn publish [queue]
  (prefix queue))

(defn bulk [queue]
  (format "%s/_bulk" (prefix queue)))

(defn get [queue id]
  (format "%s/%s" (prefix queue) id))

(defn update [queue id version]
  (format "%s/%s?version=%d&refresh=true"
          (prefix queue) id version))

(defn health [exch status]
  (format "%s/_cluster/health/%s?wait_for_status=%s"
          (:uri exch) (:name exch) (name status)))

(defn delete-control [queue ctrl]
  (format "%s/_query?q=%s:die OR %s:pause"
          (prefix queue)
          (name ctrl)
          (name ctrl)))
