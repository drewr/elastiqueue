(ns workroom.test.core
  (:require [workroom.core :as work]
            [workroom.log :as log]
            [clojure.test :refer :all])
  (:import (java.util.concurrent Executors TimeUnit)))

(defn rand-queue []
  (format "test-%d" (System/currentTimeMillis)))

(defmacro time* [op & body]
  `(let [start# (System/currentTimeMillis)]
     ~@body
     (log/log ~op (- (System/currentTimeMillis) start#) "ms")))

(deftest integrate!
  (let [q (work/declare-queue
           (work/->Queue "http://localhost:9200" (rand-queue) "test.foo")
           :store :ram)
        msgs 500
        pool (Executors/newFixedThreadPool
              (.availableProcessors (Runtime/getRuntime)))
        consumed (java.util.concurrent.CountDownLatch. msgs)
        n (atom 0)
        xs (atom (sorted-set))
        go (fn [latch]
             (fn [msg]
               (when msg
                 #_(log/log 'consume (-> msg :_source))
                 (swap! n + (-> msg :_source :n))
                 (swap! xs conj (-> msg :_source :x))
                 (.countDown latch))))]
    (time* 'publish
      (work/publish-seq q (for [x (range msgs)]
                            {:n 1 :x x})))
    (time* 'consume
      (dotimes [_ msgs]
        #_(log/log 'remain (work/queue-size q))
        (.execute pool
                  (fn []
                    (work/consume q 10 500 (go consumed))))
        #_(Thread/sleep (rand-int 5)))
      (.await consumed))
    (log/log pool)
    (is (= msgs @n))
    (is (= msgs (count @xs)))
    (is (= (apply sorted-set (range msgs)) @xs))
    (work/delete-queue q)))
