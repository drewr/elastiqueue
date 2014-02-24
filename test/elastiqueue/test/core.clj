(ns elastiqueue.test.core
  (:require [elastiqueue.core :as q]
            [elastiqueue.log :as log]
            [clojure.test :refer :all])
  (:import (java.util.concurrent Executors TimeUnit)))

(defn rand-exch []
  (let [n (format "test-%d" (System/currentTimeMillis))]
    (q/declare-exchange "http://localhost:9200" n
                        :store :ram)))

(defmacro time* [op & body]
  `(let [start# (System/currentTimeMillis)]
     ~@body
     (log/log ~op (- (System/currentTimeMillis) start#) "ms")))

(defn t [msgs]
  (testing msgs
    (let [exch (rand-exch)
          q (q/declare-queue exch "test.foo")
          pool (Executors/newFixedThreadPool
                (int (/ (.availableProcessors (Runtime/getRuntime)) 2)))
          consumed (java.util.concurrent.CountDownLatch. msgs)
          n (atom 0)
          xs (atom (sorted-set))
          ms (atom [])
          go (fn [latch]
               (fn [msg]
                 (when msg
                   #_(log/log 'consume msg)
                   (swap! ms conj msg)
                   (swap! n + (-> msg :_source :n))
                   (swap! xs conj (-> msg :_source :x))
                   (.countDown latch))))]
      (time* 'publish
             (q/publish-seq q (for [x (range msgs)]
                                {:n 1 :x x})))
      (time* 'consume
             (dotimes [n msgs]
               #_(log/log 'remain n (q/queue-size q))
               (.execute pool
                         (fn []
                           (q/consume q 10 500 (go consumed))))
               (q/sleep (rand-int 5)))
             (.await consumed))
      (is (= 0 (q/queue-size q)))
      (log/log 'COMPLETE msgs (.getCompletedTaskCount pool))
      (is (= msgs (.getCompletedTaskCount pool)))
      (is (= msgs @n))
      (is (= msgs (count @xs)))
      (is (= (apply sorted-set (range msgs)) @xs))
      (if (= msgs (count @xs))
        (doseq [m @ms]
          (q/ack m)))
      (is (= 0 (q/queue-size q)))
      (q/delete-queue q)
      (q/delete-exchange exch))))

(deftest integrate!
  (t 1)
  (t 10)
  (t 100))
