(ns clj-datastore.sql
  (:require [clojure.string :as s]
            [clj-datastore.datastore :as d]
            [clojure.java.jdbc :as j]
            [clj-datastore.sql-spec :refer :all]
            [clj-datastore.util :refer [<-> seq-or-bust]]))

;; FIXME: makes a lot of assumptions about an id primary key, but will accept
;; a table spec (model keys) that uses a different primary key.  Should reconcile

;  (def mysql-db {:subprotocol "mysql"
;               :subname "//127.0.0.1:3306/clojure_test"
;               :user "clojure_test"
;               :password "clojure_test"})

(defn- build-where-clause [kvs]
  (if (empty? kvs)
    [nil []]
    (->> (map (fn[[k v]] [(str (name k) "= ?") v]) kvs)
         (apply mapv vector)
         (<-> update #(s/join " and " %) 0))))

(defn- do-select-records [db fields table & [kvs]]
  (let [[wstr wargs] (build-where-clause kvs)
        fieldnames   (->> fields 
                          (map (comp quote-string-with-dash name))
                          (s/join ","))
        tablename    (name table)
        qstr         (str "select " fieldnames " from " tablename " where " (or wstr "true"))]
    (println (concat [qstr wargs]))
    (j/query db (concat [qstr] wargs))))

(defn- build-join-clause [[table1 table2] conds]
  (let [tn1 (name table1)
        tn2 (name table2)]
    (->> conds
         (map (fn [[x y]] [(name x) (name y)]))
         (map (fn [[x y]] (str tn1 "." x "=" tn2 "." y)))
         (s/join " and "))))

(defn- make-field-name [x]
  (let [kw (keyword x)]
    (if-let [n (namespace kw)]
      (str n "." (name kw))
      (name kw))))

;;TODO: not a bug, but something we should handle here:
;;(do-join-records db [:roles/id :actors/id :name] [:roles :actors] [[:id :role_id]])
;;(select roles.id,actors.id,name from roles,actors where roles.id=actors.role_id and true [])
;;({:id 4, :id_2 1, :name "Pre Op Nurse"})
;; the select returns non scoped fields, and resolves duplicates by appending _#.  we should
;; line this back up with the fields that were requested.
;; for now, it's ok, the consumer can deal with it...
(defn- do-join-records [db fields tables conds & [kvs]]
  (let [[wstr wargs] (build-where-clause kvs)
        jclause      (build-join-clause tables conds)
        fieldnames   (->> (seq-or-bust fields)
                          (map (comp quote-string-with-dash make-field-name))
                          (s/join ","))
        tablenames   (->> (map name tables)
                          (s/join ","))
        qstr         (str "select " fieldnames " from " tablenames " where " jclause " and " (or wstr "true"))]
    (println (concat [qstr wargs]))
    (j/query db (concat [qstr] wargs) {:identifiers (comp quote-string-with-dash s/lower-case)})))
 
(defn- do-get-record [db fields table id]
  {:pre (some? id)}
  (-> (do-select-records db fields table {:id id})
      first))

(defn- get-inserted-row
  "Takes in the response from an insert! call and returns the row that was inserted into the database.  This is needed because the return value of insert! is different for some databases (looking at you postgresql) than others."
  [db fields table res]
  (condp = (:subprotocol db)
    "postgresql" res ; Postgresql returns the whole row
    (do-get-record db fields table (:generated_key res))))

(defn- do-add-record [db fields table kvs]
  (println "In do-add-record: " fields table kvs)
  (let [tablekw (keyword table)
        kvs     (select-keys kvs fields)]
    (println kvs)
    (if-let [res (-> (j/insert! db tablekw kvs {:entities quote-string-with-dash})
                     first)]
      (get-inserted-row db fields tablekw res)
      nil)))

(defn- do-delete-record [db table id]
  {:pre (some? id)}
  (let [tablekw (keyword table)
        wclause (->> (build-where-clause {:id id})
                     flatten)] ;; delete! needs this sequence to be flattened
    (assert (first wclause)) ;; Protection to make sure we don't delete the world!
    ;; TODO: test if dashed strings mess up where clause
    (-> (j/delete! db tablekw wclause)
        first
        (= 1))))

(defn- do-update-record [db fields table id kvs]
  {:pre (some? id)}
  (let [tablekw (keyword table)
        kvs     (select-keys kvs fields)
        wclause (->> (build-where-clause {:id id})
                     flatten)] ;; update! needs this sequence to be flattened
    (assert (first wclause)) ;; Protection to make sure we don't update the world!
    (-> (j/update! db tablekw kvs wclause {:entities quote-string-with-dash})
        first
        (= 1))))

(defprotocol ISqlDatastore
  (same-database? [db args]))

(defn make-sql-data-store [mks ns connargs]
  ;; First thing..make sure we have a table, and it conforms to our spec
  ;; FOR NOW: create a table if it doesnt exist, but throw an exception if the spec
  ;; doesnt match
  ;; NOTE: the query set of fields may be different than the modify set, if
  ;; we add an ID in
  (check-table connargs ns mks true false)

  (let [query-set  (query-keys  mks)
        modify-set (modify-keys mks)
        kwspace (keyword ns)]
    

    (reify
      ISqlDatastore
      (same-database? [_ args]
        (= args connargs))
      d/IDatastore 
      (model-keys [_] mks)
      (nspace [_] kwspace)
      (select-records [_ kvs]
        (do-select-records connargs query-set kwspace kvs))
      (list-records [_]
        (do-select-records connargs query-set kwspace))
      (add-record [_ kvs]
        (do-add-record connargs modify-set kwspace kvs))
      (get-record [_ id]
        (do-get-record connargs query-set kwspace id))
      (update-record [ds id kvs]
        (when (do-update-record connargs modify-set kwspace id kvs)
          (do-get-record connargs query-set kwspace id)))
      (delete-record [_ id]
        (do-delete-record connargs kwspace id))
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

