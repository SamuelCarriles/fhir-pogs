(ns fhir-pogs.query
  (:require [fhir-pogs.db :refer [jdbc-execute! get-columns-of! get-tables! table-remove!]]
            [fhir-pogs.mapper :refer [parse-resource]]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))

(defn parse-pg-obj [obj]
  (->> obj .getValue (parse-resource)))

(defn get-resource! "Obtiene el recurso asociado a un id específico en la base de datos
                     y lo lo devuelve como un mapa."
  [db-spec ^String table-prefix ^String id]
  (when-not (map? db-spec) (throw (IllegalArgumentException. "db-spec must be a map.")))
  (let [table (keyword (str table-prefix "_main"))
        sentence (-> (help/select :content)
                     (help/from table)
                     (help/where [:= :resource-id id])
                     sql/format)
        content (first (jdbc-execute! db-spec sentence))]
    (when content (->> content vals first parse-pg-obj))))

(defn search-resource! "Retorna una seq con los recursos encontrados"
  [db-spec ^String table-prefix ^String restype conditions]
  (cond
    (not (map? db-spec)) (throw (IllegalArgumentException. "db-spec must be a map."))
    (not (and (vector? conditions) (every? vector? conditions))) (throw (IllegalArgumentException. "conditions must be a vector of vectors.")))
  (let [table (keyword (str table-prefix "_" (.toLowerCase restype)))
        main (keyword (str table-prefix "_main"))]
    (->>
     (jdbc-execute! db-spec
                    (-> (help/select :content)
                        (help/from main)
                        (help/join table [:= :resource-id :id])
                        (help/where conditions)
                        sql/format))
     (mapcat vals)
     (map parse-pg-obj))))


(comment
  (def db-spec {:dbtype "postgresql"
                :dbname "resources"
                :host "localhost"
                :user "postgres"
                :port "5432"
                :password "postgres"})

  (get-resource! db-spec "testing" "exampl")


  



  :.)