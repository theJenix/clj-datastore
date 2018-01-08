(ns clj-datastore.file
  (:require [clojure.java.io :as io]
            [clojure.set :refer [intersection rename-keys]]
            [clojure.tools.logging :as log]
            [clj-datastore.datastore :as d]
            [clj-datastore.util :refer [find-first =val? <-> or-else do-random-wait]]))

(defn replace-store [ds new-store & [revision date]]
  (let [last-modified (or-else (java.util.Date.) date)
        revision      (or-else (:revision @ds) revision)]
    (swap! ds assoc :store new-store :revision revision :last-modified last-modified)))

;(defn -swap-in! [ds ks f & args]
;  (swap! ds assoc :store (f)))

(defn- -get-max-id [data]
  (->> (map :id data)
       (<-> conj 0)
       (apply max)))

(defn- -next-id [recs]
  (-> recs
      -get-max-id
      inc))

(defrecord DataStore [model-keys nspace storage last-modified store revision])

;; StorageService lets us support data stores backed by google drive, s3, databases, file systems, etc
;; TODO: Can even implement esoteric datastores, like google calendar for calendar data, maybe? 
(defprotocol StorageService
  (get-last-modified-date [_ ds])
  (read-records  [_ ds])
  (write-records [_ ds]))

  ; TODO watch for file changes in a separate process
(def local-storage
  (let [build-filename (fn [ds] (-> (:nspace @ds)
                                    (str ".edn")))
        monitor (Object.)]
    (reify
      StorageService
      (get-last-modified-date [_ ds]
        (let [fname (build-filename ds)]
          (-> (.. (io/file fname) (lastModified))
              (java.util.Date.))))
      (read-records [this ds]
        (let [fname (build-filename ds)
              new-recs (if (.exists (io/as-file fname))
                         (->> fname    
                              slurp
                              read-string)
                         [])]
          (replace-store ds new-recs nil (get-last-modified-date this ds))))
      (write-records [this ds]
        (let [fname (build-filename ds)
              recs  (:store @ds)]
          (locking monitor
            (when (neg? (compare (:last-modified @ds) (get-last-modified-date this ds)))
              (throw (d/make-retry-write-exception)))
            (spit fname recs)))))))

(defn- -can-reload-records
  "Tests if we should reload the records in a data store by testing the
  last modified date of the resource managed by the StorageService
  implementation"
  [ds]
  (let [ds-last-modified (-> (:storage @ds)
                             (get-last-modified-date ds))]
    (or (nil? ds-last-modified)
        (-> (compare (:last-modified @ds) ds-last-modified)
            neg?))))

(defn- -do-reload-records!
  "Reloads the records inside the data store, using the StorageService implementation
  to retrieve and set the records (using replace-store)"
  [ds]
  (-> (:storage @ds)
      (read-records ds)))

(defn- -reload-records!
  "Conditionally reloads the records inside the data store.  This returns nothing
  important but the datastore will contain up-to-date data after the function exits."
  [ds]
  (when (-can-reload-records ds)
    (-do-reload-records! ds)))

(defn -update-and-write-records
  "Takes an update function and args and repeatedly applies that to the latest store result
   until the update sticks.  Similar to 'update', but for data stores"
  [ds f & args]
  (time 
  (loop [retries 10]
    (-do-reload-records! ds)
    ;; TODO race condition here...
    (let [towrite (atom (replace-store ds (apply f (concat [(:store @ds)] args))))
          result
            (try 
              (-> (:storage @towrite)
                  (write-records towrite))
              (catch clojure.lang.ExceptionInfo e (-> (ex-data e)
                                                      :cause)))]
      (if (not= result :retry-write)
        result
        (if-not (pos? retries)
          (do
            (log/warn "write unsuccessful in 10 retries.  Throwing exception to caller")
            (throw (d/make-write-failed-exception)))
          (do
            (log/warn "retry-write exception caught.  Retrying the write...")
            (do-random-wait 100)
            (recur (dec retries)))))))))

(defn -do-update-in-place [recs model-keys id updates]
  (let [f (juxt filter remove)
        [[e] rest] (f (=val? :id id) recs)
        updated (-> (merge e updates)
                    (select-keys model-keys)
                    (assoc :id id))]
    (conj (vec rest) updated)))

(defn -do-logical-delete [recs model-keys id]
  (-do-update-in-place recs (conj model-keys :deleted) id {:deleted true}))

(defn do-list-records [ds]
  (-reload-records! ds)
  (->> @ds
       :store
       (remove :deleted)))

(defn do-select-records [ds & [kvs]]
  (let [pred (reduce (fn [p [k v]] #(and (= (get %1 k) v) (p %1))) (fn[x] true) kvs)]
    (->> (do-list-records ds)
         (filter pred))))

(defn do-add-record [ds attrs]
  (let [model-keys (:model-keys @ds)
        e-with-id (atom {})
        e (-> attrs
              (select-keys model-keys))]
    (-update-and-write-records
      ds
      (fn [recs]
        (reset! e-with-id (assoc e :id (-next-id recs)))
        (conj recs @e-with-id)))
    @e-with-id))

(defn do-get-record [ds id]
  (-reload-records! ds)
  (->> @ds 
       :store
       (filter (=val? :id id))
        first))

(defn -deconflict-key-fn
  "Returns a function that will deconflict keys in a record by adding the namespace of the second datasource"
  [ds1 ds2]
  (let [ks (-> (intersection
                 (:model-keys @ds1)
                 (:model-keys @ds2))
               (conj :id))
        kmap (reduce #(assoc %1 %2 (keyword (:nspace @ds2) (name %2))) {} ks)]
       
    #(rename-keys %1 kmap)))

(defn -deconflict-keys
  "Deconflicts keys in a sequence of records about to be merged by adding a namespace"
  [ds1 ds2 rec]
  (let [f (-deconflict-key-fn ds1 ds2)]
    (f rec)))

(defn- make-join-fn
  "Returns a function that takes in two maps and compares the values in those maps, as specified by the pairs.  Missing values are considered not equal under any circumstances (i.e. nil != nil)"
  [pair & others]
  (fn [x y]
    (->> (concat [pair] others)
         (map      (fn [[k1 k2]] [(get x k1) (get y k2)]))
         (not-any? (fn [[v1 v2]]
                     (or (nil? v1)
                         (nil? v2)
                         (not= v1 v2)))))))

(defn do-join-records
  "Joins the records of two datasources on the condition specified by f.  This performs a left join
  with respect to the first datasource.  If duplicate matches exist in the second datasource, this will pick the first one it finds."
  [ds1 ds2 f-or-pair & pairs]
  (let [recs1 (do-list-records ds1)
        recs2 (do-list-records ds2)
        df    (-deconflict-key-fn ds1 ds2)
        f     (if (fn? f-or-pair) f-or-pair (make-join-fn f-or-pair pairs))]
    (println "Enter join-records...")
;    ;;TODO define separate mergefn and joinfn, so we can swap in different strategies
    (map (fn [e] (merge e (->> recs2
                               (find-first (partial f e))
                               df)))
         recs1)))

(defn do-map-records
  "Maps a function over the records in a datasource and optionally saves the datasource to persistence"
  [f ds & save]
  (if save
    (-update-and-write-records ds #(vec (map f %)))
    (let [recs (:store @ds) ; TODO: API to get all records (even logically deleted ones)
          new-recs (vec (map f recs))]
      (replace-store ds new-recs))))

(defn do-update-record [ds id attrs]
  (let [fixed (assoc attrs :id id)]
   (-update-and-write-records ds -do-update-in-place (:model-keys @ds) id fixed)
   (do-get-record ds id)))

(defn do-delete-record [ds id]
  (let [rec (do-get-record ds id)]
    (some?
      (when (and rec (not (:deleted rec)))
        (-update-and-write-records ds -do-logical-delete (:model-keys @ds) id)
        ""))))

;; TODO currently on supports edn...should also support json, other files
(defn make-file-data-store [mks ns storage]
  (let [mk-set (set mks)
        ds (atom (DataStore. mk-set ns storage nil nil nil))
        kwspace (keyword ns)]
    (-reload-records! ds)
    (reify
      d/IDatastore
      (model-keys [_] mk-set)
      (nspace [_] kwspace)
      (select-records [_ kvs]
        (do-select-records ds kvs))
      (list-records [_]
        (do-list-records ds))
      (add-record [_ kvs]
        (do-add-record ds kvs))
      (get-record [_ id]
        (do-get-record ds id))
      (update-record [_ id attrs]
        (do-update-record ds id attrs))
      (delete-record [_ id]
        (do-delete-record ds id))
      (join-record [_ other f-or-pair & pairs]
        ;; TODO may not work, but not needed for right now.  WIP
        ;     (when (and (satisfies? ISqlDatastore)
        ;                (same-database? other connargs)
        ;                (not (fn? f-or-pair)))
        ;       (do-join-records connargs
        ;                        (concat mks (model-keys other))
        ;                        [ns (nspace other)]
        ;                        (concat [f-or-pair] pairs))))
        nil)
      ;; can default back to old join-records...maybe

      )))



  ;; TODO: split-records in datastore, split into two temporary stores, so we can use map-records and join-records here
