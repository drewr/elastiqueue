(ns elastiqueue.date
  (:import (java.util Date)
           (java.text SimpleDateFormat)))

(defn now
  ([]
     (now (Date.)))
  ([date]
     (.format
      (SimpleDateFormat.
       "yyyy-MM-dd'T'HH:mm:ss.SSSZ") date)))
