(ns newsroom.test.core
  (:require [newsroom.core :as news]
            [newsroom.log :as log]
            [clojure.test :refer :all])
  (:import (java.util.concurrent Executors TimeUnit)))

(def q
  (news/->Queue "http://localhost:9200" "queuetest" "foo"))

(deftest integrate!
  (let [msgs 100
        publisher (Executors/newFixedThreadPool
                   (.availableProcessors (Runtime/getRuntime)))
        published (java.util.concurrent.CountDownLatch. msgs)
        consumer (Executors/newFixedThreadPool
                  (.availableProcessors (Runtime/getRuntime)))
        consumed (java.util.concurrent.CountDownLatch. msgs)
        n (atom 0)
        xs (atom (sorted-set))]
    (dotimes [x msgs]
      (.execute publisher
                (fn []
                  (news/publish q {:n 1 :x x})
                  (.countDown published))))
    (.await published)
    (dotimes [_ msgs]
      (.execute consumer
                (fn []
                  (news/consume q (fn [msg]
                                    #_(log/log 'consume msg)
                                    (when msg
                                      (swap! n + (-> msg :_source :n))
                                      (swap! xs conj (-> msg :_source :x)))
                                    (.countDown consumed))))))
    (.await consumed)
    (is (= msgs @n))
    (is (= (set (range msgs)) @xs))))
