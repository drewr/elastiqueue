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

(defn test [msgs]
  (testing msgs
    (let [q (work/declare-exchange
             (work/->Queue "http://localhost:9200" (rand-queue) "test.foo")
             :store :ram)
          pool (Executors/newFixedThreadPool
                (int (/ (.availableProcessors (Runtime/getRuntime)) 2)))
          consumed (java.util.concurrent.CountDownLatch. msgs)
          n (atom 0)
          xs (atom (sorted-set))
          ms (atom [])
          go (fn [latch]
               (fn [msg]
                 (when msg
                   #_(log/log 'consume (-> msg :_source))
                   (swap! ms conj msg)
                   (swap! n + (-> msg :_source :n))
                   (swap! xs conj (-> msg :_source :x))
                   (.countDown latch))))]
      (time* 'publish
             (work/publish-seq q (for [x (range msgs)]
                                   {:n 1 :x x})))
      (time* 'consume
             (dotimes [n msgs]
               '(log/log 'remain n (work/queue-size q))
               (.execute pool
                         (fn []
                           (work/consume q 10 500 (go consumed))))
               (Thread/sleep (rand-int 5)))
             (.await consumed))
      (log/log 'COMPLETE (.getCompletedTaskCount pool))
      (is (= msgs @n))
      (is (= msgs (count @xs)))
      (is (= (apply sorted-set (range msgs)) @xs))
      (if (= msgs (count @xs))
        (doseq [m @ms]
          (work/ack m)))
      (is (zero? (work/queue-size q)))
      (work/delete-queue q))))

(deftest integrate!
  (test 1)
  (test 10)
  (test 100)
  (test 500))
