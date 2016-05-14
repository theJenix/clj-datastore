(ns clj-datastore.sql
  (:require [clojure.string :as s]
            [clj-datastore.datastore :as d]
            [clojure.java.jdbc :as j]))

;  (def mysql-db {:subprotocol "mysql"
;               :subname "//127.0.0.1:3306/clojure_test"
;               :user "clojure_test"
;               :password "clojure_test"})

(defn nthapply [n f args]
  (let [applyfn (fn [e]
                  (->> (get e n)
                       f
                       (assoc e n)))]
    (apply applyfn args)))

(defn seq-or-bust [v]
  (if-not (sequential? v) (vector v) v))

(defn apply-in [ks f coll]
  (let [ks (seq-or-bust ks)]
    (->> (get-in coll ks)
         f
         (assoc-in coll ks))))

(defn build-where-clause [kvs]
  (if (empty? kvs)
    [nil []]
    (->> (map (fn[[k v]] [(str (name k) "= ?") v]) kvs)
         (apply mapv vector)
         (apply-in [0] #(s/join " and " %)))))

(defn do-select-records [db fields table & [kvs]]
  (let [[wstr wargs] (build-where-clause kvs)
        fieldnames   (->> fields 
                          (map name)
                          (s/join ","))
        tablename    (name table)
        qstr         (str "select " fieldnames " from " tablename " where " (or wstr "true"))]
    (println (concat [qstr wargs]))
    (j/query db (concat [qstr] wargs))))

(defn build-join-clause [[table1 table2] conds]
  (let [tn1 (name table1)
        tn2 (name table2)]
    (->> conds
         (map (fn [[x y]] [(name x) (name y)]))
         (map (fn [[x y]] (str tn1 "." x "=" tn2 "." y)))
         (s/join " and "))))

(defn make-field-name [x]
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
(defn do-join-records [db fields tables conds & [kvs]]
  (let [[wstr wargs] (build-where-clause kvs)
        jclause      (build-join-clause tables conds)
        fieldnames   (->> (seq-or-bust fields)
                          (map make-field-name)
                          (s/join ","))
        tablenames   (->> (map name tables)
                          (s/join ","))
        qstr         (str "select " fieldnames " from " tablenames " where " jclause " and " (or wstr "true"))]
    (println (concat [qstr wargs]))
    (j/query db (concat [qstr] wargs))))
 
(defn do-get-record [db fields table id]
  {:pre (some? id)}
  (-> (do-select-records db fields table {:id id})
      first))

(defn- get-inserted-row
  "Takes in the response from an insert! call and returns the row that was inserted into the database.  This is needed because the return value of insert! is different for some databases (looking at you postgresql) than others."
  [db fields table res]
  (condp = (:subprotocol db)
    "postgresql" res ; Postgresql returns the whole row
    (do-get-record db fields table (:generated_key res))))

(defn do-add-record [db fields table kvs]
  (let [tablekw (keyword table)]
    (if-let [res (-> (j/insert! db tablekw kvs)
                     first)]
      (get-inserted-row db fields tablekw res)
      nil)))

(defn do-delete-record [db table id]
  {:pre (some? id)}
  (let [tablekw (keyword table)
        wclause (->> (build-where-clause {:id id})
                     flatten)] ;; delete! needs this sequence to be flattened
    (assert (first wclause)) ;; Protection to make sure we don't delete the world!
    (-> (j/delete! db tablekw wclause)
        first
        (= 1))))

(defn do-update-record [db fields table id attrs]
  {:pre (some? id)}
  (let [tablekw (keyword table)
        wclause (->> (build-where-clause {:id id})
                     flatten)] ;; update! needs this sequence to be flattened
    (assert (first wclause)) ;; Protection to make sure we don't update the world!
    (-> (j/update! db tablekw attrs wclause)
        first
        (= 1))))

(defprotocol ISqlDatastore
  (same-database? [db args]))

(defn make-sql-data-store [mks ns connargs]
  (let [mk-set (set mks)
        kwspace (keyword ns)]
    ;ds (atom (DataStore. (set model-keys) nspace connargs nil nil nil))]
    (reify
      ISqlDatastore
      (same-database? [_ args]
        (= args connargs))
      d/IDatastore 
      (model-keys [_] mk-set)
      (nspace [_] kwspace)
      (select-records [_ kvs]
        (do-select-records connargs mk-set kwspace kvs))
      (list-records [_]
        (do-select-records connargs mk-set kwspace))
      (add-record [_ kvs]
        (do-add-record connargs mk-set kwspace kvs))
      (get-record [_ id]
        (do-get-record connargs mk-set kwspace id))
      (update-record [ds id attrs]
        (when (do-update-record connargs mk-set kwspace id attrs)
          (get-record ds id)))
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

