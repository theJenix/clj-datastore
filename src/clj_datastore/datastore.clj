(ns clj-datastore.datastore)

;; TODO: we should have transactions, and they should be defined as a block wherein the datastores
;; are refreshed at the beginning, and changes are committed at the end (unless an exception is thrown past the commit) and all code in between works in memory...but this is gonna be a bit of work to get right, so im punting on it for now...
;(defmacro with-transaction [datastores & body]
;  (doseq [ds datastores] (list-records ds))
;  body)

;; TODO: maintain per record "dirty" flag, so the StorageService can identify which records need to be updated


;(declare map-records [f ds & safe])

(defn make-retry-write-exception []
  (ex-info "Write failed, possibly due to conflicting writes or stale data.  Retry your write." {:cause :retry-write}))

(defn make-write-failed-exception []
  (ex-info "Write failed, no more retries." {:cause :write-failed}))

;; built in select keys that are used to control output collection
(def limit ::limit)
(def offset ::offset)
(def order-by ::order-by)
(def order ::order)
(def order-desc "DESC")
(def order-asc "ASC")

(defprotocol IDatastore
  (model-keys [_] mk)
  (nspace [_] nspace)
  (select-records [_ kvs])
  (list-records [_])
  (add-record [_ kvs])
  (get-record [_ id])
  (delete-record [_ id])
  (join-record [_ other f-or-pair & pairs])
  (update-record [_ id attrs]))
;  (apply-to [_ f & save]))
