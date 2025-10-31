(ns fhir-pogs.db
  (:require [next.jdbc :as jdbc]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]
            [fhir-pogs.validator :refer [validate-db-uri]]
            [clojure.set :as set]
            [fhir-pogs.db :as db]))
;;Execution functions
(defn execute!
  "Execute a SQL statement against the database."
  [db-uri sql-statement]
  {:pre [(validate-db-uri db-uri) (vector? sql-statement)]}
  (try
    (jdbc/execute! db-uri sql-statement)
    (catch Exception e
      (throw (ex-info "Database execution failed"
                      {:db-uri db-uri
                       :sql sql-statement
                       :error (.getMessage e)})))))

(defn execute-one!
  "Execute a SQL statement and return only the first result."
  [db-uri sql-statement]
  {:pre [(validate-db-uri db-uri) (vector? sql-statement)]}
  (try
    (jdbc/execute-one! db-uri sql-statement)
    (catch Exception e
      (throw (ex-info "Database execution failed"
                      {:db-uri db-uri
                       :sql sql-statement
                       :error (.getMessage e)})))))

(defn transact!
  "Execute multiple SQL statements in a transaction.
   All statements succeed or all fail."
  [db-uri sql-statements]
  {:pre [(validate-db-uri db-uri) (sequential? sql-statements) (every? vector? sql-statements)]}
  (try
    (jdbc/with-transaction [tx db-uri]
      (mapv #(jdbc/execute-one! tx %) sql-statements))
    (catch Exception e
      (throw (ex-info "Transaction failed"
                      {:db-uri db-uri
                       :statements sql-statements
                       :error (.getMessage e)})))))

;; Database introspection functions
(defn get-tables
  "Returns a set of table names that exist in the public schema."
  [db-uri]
  {:pre [(validate-db-uri db-uri)]}
  (try
    (->> (execute! db-uri
                   (-> (help/select :table-name)
                       (help/from :information-schema.tables)
                       (help/where [:= :table-schema "public"]
                                   [:= :table-type "BASE TABLE"])
                       sql/format))
         (map :tables/table_name)
         (map keyword)
         set)
    (catch Exception e
      (throw (ex-info "Failed to get tables" {:db-uri db-uri :error (.getMessage e)})))))

(defn get-columns
  "Returns a set of column names for the specified table."
  [db-uri table-name]
  {:pre [(validate-db-uri db-uri) (some? table-name)]}
  (try
    (->> (execute! db-uri
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
                      {:db-uri db-uri
                       :table-name table-name
                       :error (.getMessage e)})))))

;; Table management functions
(defn drop-tables!
  "Drop one or more tables from the database."
  [db-uri table-names]
  {:pre [(validate-db-uri db-uri) (sequential? table-names) (every? some? table-names)]}
  (let [existing-tables (get-tables db-uri)
        tables-to-drop (if (= [:all] table-names)
                         existing-tables
                         (set (map keyword table-names)))
        missing-tables (set/difference tables-to-drop existing-tables)]

    (when (seq missing-tables)
      (throw (ex-info "Some tables don't exist"
                      {:missing-tables missing-tables
                       :existing-tables existing-tables})))

    (when (seq tables-to-drop)
      (execute! db-uri
                (-> (apply help/drop-table tables-to-drop)
                    sql/format)))))

(defn table-exists?
  "Check if a table exists in the database."
  [db-uri table-name]
  {:pre [(validate-db-uri db-uri)]}
  (contains? (get-tables db-uri) (keyword table-name)))

(defn tables-exist?
  "Check if all specified tables exist in the database."
  [db-uri table-names]
  {:pre [(validate-db-uri db-uri)]}
  (let [existing-tables (get-tables db-uri)
        requested-tables (set (map keyword table-names))]
    (set/subset? requested-tables existing-tables)))
