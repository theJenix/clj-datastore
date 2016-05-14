(ns clj-datastore.util
  (:require [clojure.set :refer [rename-keys]]))

(defn do-random-wait [max-wait-ms]
  (Thread/sleep (* max-wait-ms (Math/random)))) ; Small random throttle to help avoid collisions

(defn concatv [coll other & others]
  (-> (apply concat coll other others)
      vec))
;; copied from discovery-capture TODO: find a better place for this stuff
(defn or-else [d x]
  (if x x d))

; TODO copied from ical.clj
(defn date-to-calendar [date]
  (let [cal (java.util.Calendar/getInstance)]
    (.setTime cal date)
    cal))

(defn seq-or-bust [v]
  (if-not (sequential? v) (vector v) v))

(defn find-first [pred coll]
  (reduce #(when (pred %2) (reduced %2)) nil coll))

(def simple-split-by (juxt filter remove))
(defn split-by
  ([pred coll]
   (simple-split-by pred coll)))
;; TODO: move to utils
;(defn json-response [data & [status]]
;  {:status (or status 200)
;   :headers {"Content-Type" "application/json"}
;   :body (json/generate-string data)})

(defn rename-all-keys [maps kmap]
  (map #(rename-keys % kmap) maps))

(defn =val?
  "Returns a predicate that tests if a value in a collection equals a value passed in"
  [k v]
  #(= (get % k) v))

(defn <->
  "Calls a function with arguments in reverse order.  If no arguments are supplied, returns a function that will call the original function with the arguments in reversed order.  This is useful for when you want to thread a value through a function using -> or ->> and the function isn't written to take the threaded argument as the first or last argument (respectively)."
  ([f]
    (fn[& args]
      (apply <-> f args)))
  ([f & args]
    (->> (reverse args)
         (apply f))))


(defn update-if-there
  "Updates a value in a map only if the key already exists (does not assume a value of nil if key does not exist)."
  [m k f & args]
  (if (contains? m k)
    (apply update m k f args)
    m))

(defn parse-int
  "Wraps Java parseInt function in clojure function, so we can use it with higher-order fns"
  [s]
  (Integer/parseInt s))

(defn apply-values [f coll & ks]
  (->> (map (partial get coll) ks)
       (apply f)))
