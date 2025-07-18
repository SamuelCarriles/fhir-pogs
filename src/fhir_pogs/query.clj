(ns fhir-pogs.query
  (:require [fhir-pogs.db.core :refer [jdbc-execute!]]
            [fhir-pogs.mapper :refer [parse-resource]]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))

(defn get-resource! "Obtiene un recurso específico en la base de datos
                     y lo lo devuelve como un mapa."
  [db-spec ^String table-prefix ^String id]
  (let [table (keyword (str table-prefix "_main"))
        sentence (-> (help/select :content)
                     (help/from table)
                     (help/where [:= :id id])
                     sql/format)
        content (first (jdbc-execute! db-spec sentence))]
    (when content (->> content vals first .getValue (parse-resource)))))

(comment
  (def db-spec {:dbtype "postgresql"
                :dbname "resources"
                :host "localhost"
                :user "postgres"
                :port "5432"
                :password "postgres"})

  (jdbc-execute! db-spec (-> (help/select :*)
                             (help/from :testing_main)
                             (help/join :testing_encounter [:= :testing_encounter.id :testing_main.resource_id])
                             (help/where [:= :resource_id "example"])
                             sql/format))

  (fhir-pogs.db.core/get-tables! db-spec)

  (get-resource! db-spec "testing" "exampl")
  :.)