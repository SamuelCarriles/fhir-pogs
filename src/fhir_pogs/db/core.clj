(ns fhir-pogs.db.core
  (:require [next.jdbc :as jdbc]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))

(defn jdbc-execute! "Obtains the datasource corresponding to the `db-spec`, connects to the database, and executes a `jdbc/execute!` within a `with-open` block."
  [db-spec sentence]
  (let [my-datasource (jdbc/get-datasource db-spec)]
    (with-open [connection (jdbc/get-connection my-datasource)]
      (jdbc/execute! connection sentence))))


(defn get-tables! "Retorna una seq de keywords que son las tablas que existen en una base de datos especificada."
  [db-spec]
  (->> (map #(:tables/table_name %)
            (jdbc-execute! db-spec (-> (help/select :table-name)
                                       (help/from :information-schema.tables)
                                       (help/where [:= :table-schema "public"]
                                                   [:= :table-type "BASE TABLE"])
                                       sql/format)))
       (map keyword)))

(defn table-remove! "Borra una o varias tablas en una base de datos especificada."
  [db-spec n]
  (if (and (= 1 (count n)) (= :all (first n)))
    (let [sentence
          (->> (get-tables! db-spec)
               (apply help/drop-table)
               sql/format)]
      (if (seq (get-tables! db-spec)) (jdbc-execute! db-spec sentence) nil))
    (when (and (seq (get-tables! db-spec))
               (every? true?
                       (reduce (fn [o i]
                                 (if (some #(= % i) n)
                                   (conj o true)
                                   (conj o false)))
                               #{} (get-tables! db-spec))))
      (jdbc-execute! db-spec (-> (apply help/drop-table n)
                                 sql/format)))))
