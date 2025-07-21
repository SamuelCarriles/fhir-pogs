(ns fhir-pogs.query
  (:require [fhir-pogs.db :refer [jdbc-execute! get-tables!]]
            [fhir-pogs.mapper :refer [parse-resource]]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))

(defn parse-pg-obj [obj]
  (->> obj .getValue (parse-resource)))

(defn search-resource! "Retorna una seq con los recursos encontrados."
  [db-spec ^String table-prefix ^String restype conditions]
  (cond
    (not (map? db-spec)) (throw (IllegalArgumentException. "db-spec must be a map."))
    (not (vector? conditions)) (throw (IllegalArgumentException. "conditions must be a vector."))) 
  (let [table (keyword (str table-prefix "_" (.toLowerCase restype)))
        main (keyword (str table-prefix "_main"))]
    (when-not (contains? (get-tables! db-spec) table) (throw (IllegalArgumentException. (str "The resource type " restype " does not exist into database."))))
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
  
  :.
  )