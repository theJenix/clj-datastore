(ns clj-datastore.datastore
  (:require [clojure.java.io :as io]
            [clojure.set :refer [intersection rename-keys]]
            [clj-datastore.util :refer [find-first =val? <-> or-else]]))

;; TODO: we should have transactions, and they should be defined as a block wherein the datastores
;; are refreshed at the beginning, and changes are committed at the end (unless an exception is thrown past the commit) and all code in between works in memory...but this is gonna be a bit of work to get right, so im punting on it for now...
;(defmacro with-transaction [datastores & body]
;  (doseq [ds datastores] (list-records ds))
;  body)

;; TODO: maintain per record "dirty" flag, so the StorageService can identify which records need to be updated


(defn replace-store [ds new-store & [date]]
  (let [last-modified (or-else (java.util.Date.) date)]
    (swap! ds assoc :store new-store :last-modified last-modified)))

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

(defrecord DataStore [model-keys nspace storage last-modified])

;; StorageService lets us support data stores backed by google drive, s3, databases, file systems, etc
;; TODO: Can even implement esoteric datastores, like google calendar for calendar data, maybe? 
(defprotocol StorageService
  (get-last-modified-date [_ ds])
  (read-records  [_ ds])
  (write-records [_ ds]))

  ; TODO watch for file changes in a separate process
(def local-storage
  (let [build-filename (fn [ds] (-> (:nspace @ds)
                                    (str ".edn")))]
    (reify
      StorageService
      (get-last-modified-date [_ ds]
        nil)
      (read-records [_ ds]
        (let [fname (build-filename ds)
              new-recs (if (.exists (io/as-file fname))
                         (->> fname    
                              slurp
                              read-string)
                         [])]
          (replace-store ds new-recs)))
      (write-records [_ ds]
        (let [fname (build-filename ds)
              recs  (:store @ds)]
          ;(println "-----")
          ;(println (type recs))
          (spit fname recs))))))

(defn- -can-reload-records [ds]
  (let [ds-last-modified (-> (:storage @ds)
                             (get-last-modified-date ds))]
    (or (nil? ds-last-modified)
        (-> (compare (:last-modified @ds) ds-last-modified)
            neg?))))

(defn- -do-reload-records [ds]
  (println "Doin the reload!")
  (-> (:storage @ds)
      (read-records ds)))

(defn- -reload-records [ds]
  (if (-can-reload-records ds)
    (-do-reload-records ds)
    @ds))

(defn -write-records [ds]
  (-> (:storage @ds)
      (write-records ds)))

(defn -do-update-in-place [recs id updates]
  (let [f (juxt filter remove)
        [[e] rest] (f (=val? :id id) recs)]
    (conj (vec rest) (merge e updates))))

(defn -do-logical-delete [recs id]
  (-do-update-in-place recs id {:deleted true}))

(defn make-data-store [model-keys nspace storage]
  (let [ds (atom (DataStore. (set model-keys) nspace storage nil))]
    (-reload-records ds)
    ds))

(defn list-records [ds]
  (->> (-reload-records ds)
       :store
       (remove :deleted)))

(defn select-records [ds kvs]
  (let [pred (reduce (fn [p [k v]] #(and (= (get %1 k) v) (p %1))) (fn[x] true) kvs)]
    (->> (list-records ds)
         (filter pred))))

(defn add-record [ds attrs]
  (let [model-keys (:model-keys @ds)
        recs (:store @ds)
        e (-> attrs
              (select-keys model-keys)
              (assoc :id (-next-id recs)))]
    (replace-store ds (conj recs e))
    (-write-records ds)
    e))

(defn get-record [ds id]
  (->> (-reload-records ds)
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

(defn join-records
  "Joins the records of two datasources on the condition specified by f.  This performs a left join
  with respect to the first datasource.  If duplicate matches exist in the second datasource, this will pick the first one it finds."
  [ds1 ds2 f]
  (let [recs1 (list-records ds1)
        recs2 (list-records ds2)
        df    (-deconflict-key-fn ds1 ds2)]
    (println "Enter join-records...")
;    ;;TODO define separate mergefn and joinfn, so we can swap in different strategies
    (map (fn [e] (merge e (->> recs2
                               (find-first (partial f e))
                               df)))
         recs1)))

(defn map-records
  "Maps a function over the records in a datasource and optionally saves the datasource to persistence"
  [f ds & save]
  (let [recs (:store @ds) ; TODO: API to get all records (even logically deleted ones)
        new-recs (vec (map f recs))]
    (replace-store ds new-recs)
    (when save (-write-records ds))))

(defn update-record [ds id attrs]
  (let [recs (:store @ds)
        fixed (assoc attrs :id id)]
   ; TODO: wish I had a swap-in function 
   ; so i can do: (swap! data do-update-in-place id fixed)
  (->> (-do-update-in-place recs id fixed)
       (replace-store ds))
  (-write-records ds)
  (get-record ds id)))

(defn delete-record [ds id]
  (let [recs (:store @ds)]
    (->> (-do-logical-delete recs id)
         (replace-store ds))
    (-write-records ds)))



  ;; TODO: split-records in datastore, split into two temporary stores, so we can use map-records and join-records here
