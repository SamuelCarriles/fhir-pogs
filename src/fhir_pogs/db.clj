(ns fhir-pogs.db
  (:require [next.jdbc :as jdbc]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))

(defn jdbc-execute! "Obtains the datasource corresponding to the `db-spec`, connects to the database, and executes a `jdbc/execute!` within a `with-open` block."
  [db-spec sentence]
  (let [my-datasource (jdbc/get-datasource db-spec)]
    (with-open [connection (jdbc/get-connection my-datasource)]
      (jdbc/execute! connection sentence))))

(defn table-remove! [db-spec & n]
  (if (and (= 1 (count n)) (= :all (first n)))
    (let [sentence
          (->> (map #(:tables/table_name %)
                    (jdbc-execute! db-spec (-> (help/select :table-name)
                                               (help/from :information_schema.tables)
                                               (help/where [:= :table_schema "public"]
                                                           [:= :table-type "BASE TABLE"])
                                               sql/format)))
               (map keyword)
               (apply help/drop-table)
               sql/format)]
      (jdbc-execute! db-spec sentence))
    (jdbc-execute! db-spec (-> (apply help/drop-table n)
                               sql/format))))

