(ns com.jd.bdp.magpie.util.timer
  (:require [com.jd.bdp.magpie.util.utils :as utils])
  (:import [java.util PriorityQueue Comparator])
  (:import [java.util.concurrent Semaphore]))

;; The timer defined in this file is very similar to java.util.Timer
(defn mk-timer [ & {:keys [kill-fn] :or {kill-fn (fn [ & _ ])}}]
  (let [queue (PriorityQueue. 10
                              (reify Comparator
                                (compare [this o1 o2]
                                  (- (first o1) (first o2)))
                                (equals [this obj]
                                  true)))
        active (atom true)
        lock (Object.)
        notifier (Semaphore. 0)
        timer-thread (Thread.
                      (fn []
                        (while @active
                          (try
                            (let [[time-millis _ _ :as elem] (locking lock (.peek queue))]
                              (if (and elem (>= (utils/current-time-millis) time-millis))
                                ;; imperative to not run the function inside the timer lock
                                ;; otherwise, it's possible to deadlock if function deals with other locks
                                ;; (like the submit lock)
                                (let [afn (locking lock (second (.poll queue)))]
                                  (afn))
                                (if time-millis
                                  ;; if any events are scheduled sleep until event generation
                                  ;; note that if any recurring events are scheduled then we will always go through
                                  ;; this branch, sleeping only the exact necessary amount of time
                                  (Thread/sleep (- time-millis (utils/current-time-millis)))
                                  ;; else poll to see if any new event was scheduled
                                  ;; this is in essence the response time for detecting any new event schedulings when
                                  ;; there are no scheduled events
                                  (Thread/sleep 1000))))
                            (catch Throwable t
                              ;; because the interrupted exception can be wrapped in a runtimeexception
                              (when-not (utils/exception-cause? InterruptedException t)
                                (kill-fn t)
                                (reset! active false)
                                (throw t)))))
                        (.release notifier)))]
    (.setDaemon timer-thread true)
    (.setPriority timer-thread Thread/MAX_PRIORITY)
    (.start timer-thread)
    {:timer-thread timer-thread
     :queue queue
     :active active
     :lock lock
     :cancel-notifier notifier}))

(defn- check-active! [timer]
  (when-not @(:active timer)
    (throw (IllegalStateException. "Timer is not active"))))

(defn schedule [timer delay-secs afn & {:keys [check-active] :or {check-active true}}]
  (when check-active (check-active! timer))
  (let [id (utils/uuid)
        ^PriorityQueue queue (:queue timer)]
    (locking (:lock timer)
      (.add queue [(+ (utils/current-time-millis) (* 1000 (long delay-secs))) afn id]))))

(defn schedule-recurring [timer delay-secs recur-secs afn]
  (schedule timer
            delay-secs
            (fn this []
              (afn)
              ;; this avoids a race condition with cancel-timer
              (schedule timer recur-secs this :check-active false))))

(defn cancel-timer [timer]
  (check-active! timer)
  (locking (:lock timer)
    (reset! (:active timer) false)
    (.interrupt (:timer-thread timer)))
  (.acquire (:cancel-notifier timer)))
