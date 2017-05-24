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

(defn- build-one-where-condition [k v]
  (let [qk (str "\"" (name k) "\"")]
    (if (or (set? v) (sequential? v))
      (let [in-places (s/join "," (repeat (count v) "?"))]
        (vector (str qk " in (" in-places ")") v))
      (vector (str qk "= ?") v)
      )))

(defn- build-where-clause [kvs]
  (if (empty? kvs)
    [nil []]
    ; First transpose, then build each clause, then transpose, then convert the first vector to a string clause
    (->> (apply map vector kvs)
         (apply map build-one-where-condition)
         (apply mapv vector)
         (<-> update #(s/join " and " %) 0)
         (<-> update flatten 1))))

(defn- do-select-records [db field-map table & [kvs]]
  (let [[wstr wargs] (build-where-clause kvs)
        fieldnames   (->> (keys field-map) 
                          (map (comp quote-string-with-dash name))
                          (s/join ","))
        tablename    (name table)
        qstr         (str "select " fieldnames " from " tablename " where " (or wstr "true"))]
    (println (concat [qstr wargs]))
    ;; We need to correct for the fact that the DB may strip a field name of it's casedness (make it all lowercase) by mapping the results back to the actual fields requested
    (->> (j/query db (concat [qstr] wargs))
         (map #(fix-field-names field-map %)))))

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
(defn- do-join-records [db field-map tables conds & [kvs]]
  (let [[wstr wargs] (build-where-clause kvs)
        jclause      (build-join-clause tables conds)
        fieldnames   (->> (seq-or-bust (keys field-map))
                          (map (comp quote-string-with-dash make-field-name))
                          (s/join ","))
        tablenames   (->> (map name tables)
                          (s/join ","))
        qstr         (str "select " fieldnames " from " tablenames " where " jclause " and " (or wstr "true"))]
    (println (concat [qstr wargs]))
    (->> (j/query db (concat [qstr] wargs))
         (map #(fix-field-names field-map %)))))
 
(defn- do-get-record [db field-map table id]
  {:pre (some? id)}
  (-> (do-select-records db field-map table {:id id})
      first))

(defn- get-inserted-row
  "Takes in the response from an insert! call and returns the row that was inserted into the database.  This is needed because the return value of insert! is different for some databases (looking at you postgresql) than others."
  [db field-map table res]
  (condp = (:subprotocol db)
    "postgresql" res ; Postgresql returns the whole row
    (do-get-record db field-map table (:generated_key res))))

(defn- do-add-record [db field-set table kvs]
  (println "In do-add-record: " field-set table kvs)
  (let [tablekw (keyword table)
        kvs     (filter-keys field-set kvs)]
    (println kvs)
    (-> (j/insert! db tablekw kvs {:entities quote-string-with-dash})
        first)))

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

(defn- do-update-record [db field-set table id kvs]
  {:pre (some? id)}
  (let [tablekw (keyword table)
        kvs     (filter-keys field-set kvs)
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

  (let [mk-set (set mks)
        query-field-map  (make-query-map  mks)
        modify-set       (-> (make-modify-map mks)
                             keys
                             set)
        kwspace (keyword ns)]
    (reify
      ISqlDatastore
      (same-database? [_ args]
        (= args connargs))
      d/IDatastore 
      (model-keys [_] mks)
      (nspace [_] kwspace)
      (select-records [_ kvs]
        (do-select-records connargs query-field-map kwspace kvs))
      (list-records [_]
        (do-select-records connargs query-field-map kwspace))
      (add-record [_ kvs]
        (when-let [res (do-add-record connargs modify-set kwspace kvs)]
          (get-inserted-row connargs query-field-map kwspace res)))
      (get-record [_ id]
        (do-get-record connargs query-field-map kwspace id))
      (update-record [ds id kvs]
        (when (do-update-record connargs modify-set kwspace id kvs)
          (do-get-record connargs query-field-map kwspace id)))
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

