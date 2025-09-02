(ns fhir-pogs.db
  (:require [next.jdbc :as jdbc]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]
            [clojure.set :as set]))
;;Execution functions
(defn execute!
  "Execute a SQL statement against the database."
  [db-spec sql-statement]
  {:pre [(some? db-spec) (vector? sql-statement)]}
  (try
    (jdbc/execute! db-spec sql-statement)
    (catch Exception e
      (throw (ex-info "Database execution failed"
                      {:db-spec db-spec
                       :sql sql-statement
                       :error (.getMessage e)})))))

(defn execute-one!
  "Execute a SQL statement and return only the first result."
  [db-spec sql-statement]
  {:pre [(some? db-spec) (vector? sql-statement)]}
  (try
    (jdbc/execute-one! db-spec sql-statement)
    (catch Exception e
      (throw (ex-info "Database execution failed"
                      {:db-spec db-spec
                       :sql sql-statement
                       :error (.getMessage e)})))))

(defn transact!
  "Execute multiple SQL statements in a transaction.
   All statements succeed or all fail."
  [db-spec sql-statements]
  {:pre [(some? db-spec) (sequential? sql-statements) (every? vector? sql-statements)]}
  (try
    (jdbc/with-transaction [tx db-spec]
      (mapv #(jdbc/execute-one! tx %) sql-statements))
    (catch Exception e
      (throw (ex-info "Transaction failed"
                      {:db-spec db-spec
                       :statements sql-statements
                       :error (.getMessage e)})))))

;; Database introspection functions
(defn get-tables
  "Returns a set of table names that exist in the public schema."
  [db-spec]
  (try
    (->> (execute! db-spec
                   (-> (help/select :table-name)
                       (help/from :information-schema.tables)
                       (help/where [:= :table-schema "public"]
                                   [:= :table-type "BASE TABLE"])
                       sql/format))
         (map :tables/table_name)
         (map keyword)
         set)
    (catch Exception e
      (throw (ex-info "Failed to get tables" {:db-spec db-spec :error (.getMessage e)})))))

(defn get-columns
  "Returns a set of column names for the specified table."
  [db-spec table-name]
  {:pre [(some? table-name)]}
  (try
    (->> (execute! db-spec
                   (-> (help/select :column-name)
                       (help/from :information-schema.columns)
                       (help/where [:= :table-schema "public"]
                                   [:= :table-name (name table-name)])
                       (help/order-by :ordinal-position)
                       sql/format))
         (map :columns/column_name)
         (map keyword)
         set)
    (catch Exception e
      (throw (ex-info "Failed to get columns"
                      {:db-spec db-spec
                       :table-name table-name
                       :error (.getMessage e)})))))

;; Table management functions
(defn drop-tables!
  "Drop one or more tables from the database."
  [db-spec table-names]
  {:pre [(some? db-spec) (sequential? table-names) (every? some? table-names)]}
  (let [existing-tables (get-tables db-spec)
        tables-to-drop (if (= [:all] table-names)
                         existing-tables
                         (set (map keyword table-names)))
        missing-tables (set/difference tables-to-drop existing-tables)]

    (when (seq missing-tables)
      (throw (ex-info "Some tables don't exist"
                      {:missing-tables missing-tables
                       :existing-tables existing-tables})))

    (when (seq tables-to-drop)
      (execute! db-spec
                (-> (apply help/drop-table tables-to-drop)
                    sql/format)))))

(defn table-exists?
  "Check if a table exists in the database."
  [db-spec table-name]
  (contains? (get-tables db-spec) (keyword table-name)))

(defn tables-exist?
  "Check if all specified tables exist in the database."
  [db-spec table-names]
  (let [existing-tables (get-tables db-spec)
        requested-tables (set (map keyword table-names))]
    (set/subset? requested-tables existing-tables)))
