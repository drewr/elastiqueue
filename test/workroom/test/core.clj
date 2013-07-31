(ns workroom.test.core
  (:require [workroom.core :as work]
            [workroom.log :as log]
            [clojure.test :refer :all])
  (:import (java.util.concurrent Executors TimeUnit)))

(defn rand-queue []
  (format "test-%d" (System/currentTimeMillis)))

(deftest integrate!
  (let [q (work/->Queue "http://localhost:9200" (rand-queue) "foo")
        msgs 75
        pool (Executors/newFixedThreadPool
              (.availableProcessors (Runtime/getRuntime)))
        published (java.util.concurrent.CountDownLatch. msgs)
        consumed (java.util.concurrent.CountDownLatch. msgs)
        n (atom 0)
        xs (atom (sorted-set))]
    (dotimes [x msgs]
      (.execute pool
                (fn []
                  (work/publish q {:n 1 :x x})
                  (.countDown published))))
    (.await published)
    (dotimes [_ msgs]
      #_(log/log 'remain (work/queue-size q))
      (.execute pool
                (fn []
                  (work/consume q (fn [msg]
                                    (when msg
                                      #_(log/log 'consume (-> msg :_source :x))
                                      (swap! n + (-> msg :_source :n))
                                      (swap! xs conj (-> msg :_source :x))
                                      (.countDown consumed))))))
      #_(Thread/sleep (rand-int 100)))
    (.await consumed)
    (log/log pool)
    (is (= msgs @n))
    (is (= msgs (count @xs)))
    (is (= (apply sorted-set (range msgs)) @xs))))
