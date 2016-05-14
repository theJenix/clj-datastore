(ns clj-datastore.sql-spec
  "Contains functions to the specification for tables, and check/create/update tables as needed"
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as s]
            [clj-datastore.util :refer [split-by concatv]]))

(def primary-key-modifier [:primary :key])
(def not-null-modifier    [:not :null])

(def default-type :text)
(def default-primary-key-type :integer)

(defn quote-string-with-dash [x]
  (if (.contains x "-") (str \" x \") x))

(defn- normalize-thing
  "Normalizes a thing (name, data type, other things) to be an unscoped lowercase keyword"
  [nm]
  ;(println nm (type nm))
  (-> (name nm)
      s/lower-case
      keyword))

(defn- normalize-data-type [dt char-max-length column-default]
  (let [kw (normalize-thing dt)]
    (condp = kw
      :int :integer
      :integer (if (.contains column-default "nextval") :serial :integer)
      (keyword "character varying") (keyword (str "varchar(" char-max-length ")"))
      kw)))


(defn- make-primary-key-spec []
  (concatv [:id :serial] not-null-modifier primary-key-modifier))

;; TODO: very Postgre centric...need to test with other databases
(defn- is-primary-key?
  "Returns true if the column name represents the primary key for the datastore namespace"
  [db ns col]
  (let [stmt "select 1
              from   information_schema.table_constraints c,
                     information_schema.key_column_usage k
              where c.constraint_name = k.constraint_name
                and c.constraint_type = 'PRIMARY KEY'
                and c.table_name = ?
                and k.column_name = ?"]
    (-> (j/query db [stmt (name ns) (name col)])
        first
        some?)))

(defn- get-primary-key-name
  [db ns]
  (let [stmt "select k.column_name
              from   information_schema.table_constraints c,
                     information_schema.key_column_usage k
              where c.constraint_name = k.constraint_name
                and c.constraint_type = 'PRIMARY KEY'
                and c.table_name = ?"]
    (when-let [res (-> (j/query db [stmt (name ns)])
                       first)]
      (-> res
          :column_name
          normalize-thing))))


(defn- info-schema-row-to-column-spec
  "Helper function takes a row from an information_schema.CATALOG query and converts it to our column spec"
  [{:keys [column_name data_type is_nullable character_maximum_length column_default]}]
  (let [namekw   (normalize-thing column_name) 
        dtkw     (normalize-data-type data_type character_maximum_length column_default)
        nullable (if (not= is_nullable "YES") [:not :null] [])]
    (concatv [namekw dtkw] nullable)))

(defn- get-nspace-spec-from-db
  "Gets the table specification for the datastore namespace in the datastore format."
  [db ns]
  (let [stmt "select COLUMN_NAME,DATA_TYPE,IS_NULLABLE,CHARACTER_MAXIMUM_LENGTH,COLUMN_DEFAULT
              from   information_schema.COLUMNS
              where  table_name = ?"

        res (->> (j/query db [stmt (name ns)])
                 (mapv info-schema-row-to-column-spec))]
    (when-not (empty? res)
      (let [pkname    (get-primary-key-name db ns)
            [pk rest] (split-by #(= pkname (first %)) res)]
        (if pk
          (-> pk
              vec
              (update 0 concatv [:primary :key])
              (concatv rest))
          rest)))))

(defn- is-modifier-found? [mod mods]
  (let [ms (set mod)]
    (some #(every? ms %) mods)))

(defn- get-modifier-if-found [mod mods]
  (if (is-modifier-found? mod mods)
    mod
    []))

(defn- model-key-to-column-spec [mk]
  (if (even? (count mk))
    (let [[[nm tp] & mods] (partition 2 mk)]
      (concatv 
        [(normalize-thing nm) (normalize-data-type tp nil nil)]
         (get-modifier-if-found not-null-modifier mods) 
         (get-modifier-if-found primary-key-modifier mods)))
    (let [decl (first mk)
          mods (partition 2 (rest mk))
          tp   (if (is-modifier-found? primary-key-modifier mods)
                 default-primary-key-type
                 default-type)]
      (concatv 
        [(normalize-thing decl) tp]
        (get-modifier-if-found not-null-modifier mods) 
        (get-modifier-if-found primary-key-modifier mods)))))

(defn- get-nspace-spec-from-model-keys
  [mks]
  (let [mk-seqs (map (fn[x] (if (sequential? x) (vec x) (vector x))) mks)]
    (map model-key-to-column-spec mk-seqs)))

(defn- make-spec-different-exception []
  (ex-info "Table spec on database is different than what is expected." {:cause :spec-different}))

(defn- make-table-missing-exception []
  (ex-info "Table missing on database." {:cause :table-missing}))

(defn- is-spec-different? [s1 s2]
  (println s1)
  (println s2)
  ;; Simple test, since same specs should be the identical, and any change indicates a difference
  (not= (sort s1) (sort s2)))

(defn check-table
  [db ns mks create-if-missing update-if-different]
  (let [mspec (get-nspace-spec-from-model-keys mks)
        _ (println mspec)
        mspec-with-pk (if (some #(->> (partition 2 %)
                                      (is-modifier-found? primary-key-modifier))
                               mspec)
                       mspec
                       (->> (make-primary-key-spec)
                            (conj mspec)))]
    (if-let [tspec (get-nspace-spec-from-db db ns)]
      (when (is-spec-different? mspec-with-pk tspec)
        (if update-if-different
          ;; NYI
          (do)
          (throw (make-spec-different-exception))))
      (if create-if-missing
        (do (println mspec-with-pk)
            (println (j/create-table-ddl (keyword ns) mspec-with-pk {:entities quote-string-with-dash}))
        (->> (j/create-table-ddl
                (keyword ns)
                mspec-with-pk
                {:entities quote-string-with-dash})
             (j/execute! db)))
        (throw (make-table-missing-exception))))))


(defn query-keys [mks]
  (let [mspec (get-nspace-spec-from-model-keys mks)
        mspec-with-pk (if (some #(->> (partition 2 %)
                                      (is-modifier-found? primary-key-modifier))
                               mspec)
                       mspec
                       (->> (make-primary-key-spec)
                            (conj mspec)))]
    (-> (map first mspec-with-pk)
        set)))

(defn modify-keys [mks]
  (set mks))
