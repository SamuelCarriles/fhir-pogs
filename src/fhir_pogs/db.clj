(ns fhir-pogs.db
  (:require [next.jdbc :as jdbc]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))
;;Execution functions
(defn execute!
  "Execute a SQL statement against the database."
  [connectable sql-statement]
  (try
    (jdbc/execute! connectable sql-statement)
    (catch Exception e
      (throw (ex-info "Database execution failed"
                      {:connectable connectable
                       :sql sql-statement
                       :error (.getMessage e)})))))

(defn execute-one!
  "Execute a SQL statement and return only the first result."
  [connectable sql-statement]
  (try
    (jdbc/execute-one! connectable sql-statement)
    (catch Exception e
      (throw (ex-info "Database execution failed"
                      {:connectable connectable
                       :sql sql-statement
                       :error (.getMessage e)})))))

(defn transact!
  "Execute multiple SQL statements in a transaction.
   All statements succeed or all fail."
  [connectable sql-statements]
  (try
    (jdbc/with-transaction [tx connectable]
      (mapv #(jdbc/execute-one! tx %) sql-statements))
    (catch Exception e
      (throw (ex-info "Transaction failed"
                      {:connectable connectable
                       :statements sql-statements
                       :error (.getMessage e)})))))

;; Database introspection functions
(defn get-tables
  "Returns a set of table names that exist in the public schema."
  [connectable]
  (try
    (->> (execute! connectable
                   (-> (help/select :table-name)
                       (help/from :information-schema.tables)
                       (help/where [:= :table-schema "public"]
                                   [:= :table-type "BASE TABLE"])
                       sql/format))
         (map :tables/table_name)
         (map keyword)
         set)
    (catch Exception e
      (throw (ex-info "Failed to get tables" {:connectable connectable :error (.getMessage e)})))))

(defn get-columns
  "Returns a set of column names for the specified table."
  [connectable table-name]
  (try
    (->> (execute! connectable
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
                      {:connectable connectable
                       :table-name table-name
                       :error (.getMessage e)})))))

;; Table management functions

(defn table-exists?
  "Check if a table exists in the database."
  [connectable table-name]
  {:pre [(keyword? table-name)
         (some? connectable)]}
  (contains? (get-tables connectable) table-name))



(comment
  
  :.)
