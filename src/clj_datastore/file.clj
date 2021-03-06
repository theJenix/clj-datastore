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

;; NOTE: last-modified is the last modified date of the underlying storage resource, from when we last read the records
(defrecord DataStore [model-keys nspace storage last-modified store revision])

;; StorageService lets us support data stores backed by google drive, s3, databases, file systems, etc
;; TODO: Can even implement esoteric datastores, like google calendar for calendar data, maybe?
(defprotocol StorageService
  (get-last-modified-date [_ ds])
  (read-records  [_ ds])
  (write-records
    [_ ds]
    "Writes the data store to storage, and refreshes the data store attributes such as revision and last-modified"
    ))

(defn get-current-datetime []
  "Gets the current datetime.  This should remain it's own method, so we can inject test times with with-redefs"
  (java.util.Date.))

(defn- retry [retries f & args]
  (loop [retries retries]
    (let [retry (atom false)
          ret (try
                (apply f args)
              (catch Throwable e
                (log/debug "Caught exception!")
                (do-random-wait 10)
                (if (< 0 retries)
                  (do
                    (log/debug "Retrying, " retries "times left")
                    (reset! retry true))
                  (do
                    (log/fatal e)
                    (throw e)))))]
      (if @retry
        (recur (dec retries))
        ret))))


(defn- read-file [fname]
  (log/debug "in read-file" fname)
  (if (.exists (io/as-file fname))
    (let [string (slurp fname)
          len (count string)
          contents (read-string string)
          ]
      (log/debug "returning from read-file: " (subs string (max 0 (- len 50))))
      contents)
    {:recs [] :last-modified 0}))

  ; TODO watch for file changes in a separate process
(def local-storage
  "A generic local file storage mechanism.  Because this relies on the local filesystem, it is susceptable to latencies and other quirks
   (e.g. when flushing the data after spit, the file may be empty or incomplete on the next read).  Care has been taken to mitigate
  these issues, but you should test in your environment before deploying to production."
  (let [build-filename (fn [ds] (-> (:nspace @ds)
                                    (str ".edn")))]
    ;; NOTE: https://bugs.openjdk.java.net/browse/JDK-8177809 means we need to keep our own last-modified time....frowny face
    (reify
      StorageService
      (get-last-modified-date [_ ds]
        (let [fname (build-filename ds)
              ;; Retry a bunch...this mitigates an issue where a read immediately after another thread writes will result in
              ;; a zero length file (which causes an exception in read-string)
              {:keys [_ last-modified]} (retry 50 read-file fname)]
          (when last-modified
            (java.util.Date. last-modified))))
      (read-records [this ds]
        (let [fname (build-filename ds)
              ;; Retry a bunch...this mitigates an issue where a read immediately after another thread writes will result in
              ;; a zero length file (which causes an exception in read-string)
              {:keys [recs last-modified]} (retry 50 read-file fname)]
          (replace-store ds recs nil (java.util.Date. last-modified))))
      (write-records [this ds]
        (locking ds
          (let [fname (build-filename ds)
                recs  (:store @ds)
                now (get-current-datetime)]
            ;; When the last modified date of the data store is not equal to
            ;; that of the underlying file, something's out of sync; in this case
            ;; throw an exception to let the caller figure it out
            ;; Note: this should only happen if two or more write processes are
            ;; sitting at the locking statement (because the calling code should manage this for us otherwise)
            ;; Further note: really, the case we're worried about is when the last-modified[ds] < last-modified[file]
            ;; becasue thats the case that will overwrite someone elses update.  Since last-modified[ds] > last-modified[file]
            ;; should not be possible, we want to throw in this case as well to let the calling code get things straight.
            (when (not= (:last-modified @ds) (get-last-modified-date this ds))
              (throw (d/make-retry-write-exception)))
            (spit fname {:recs recs :last-modified (.. now getTime)})
            (replace-store ds recs nil now)
            ))))))

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

(defn make-data-store-update [ds f & args]
  "Constructs a data store update, which is a complete data store that is the result of applying
  f with args to the store in ds.
  NOTE: this will NOT update ds; it is up to the caller to do so once the update is used successfully"
  (let [update (atom @ds)
        {:keys [store revision last-modified]} @ds]
    (log/debug "in make-data-store-update: " last-modified)
    (replace-store update (apply f (concat [store] args)) revision (or-else (java.util.Date. 0) last-modified))
    update))

(defn -update-and-write-records
  "Takes an update function and args and repeatedly applies that to the latest store result
   until the update sticks.  Similar to 'update', but for data stores"
  [ds f & args]
  (time
    (loop [retries 10]
      (-reload-records! ds)
      ;; TODO race condition here...
      (let [towrite (apply make-data-store-update ds f args)
            result
            (try
              (-> (:storage @towrite)
                  (write-records towrite))
              (catch clojure.lang.ExceptionInfo e (-> (ex-data e)
                                                      :cause)))]
        (if (not= result :retry-write)
          ;; Swap in the result (new datastore values and return them
          (reset! ds result)
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
    (log/debug "Enter join-records...")
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
