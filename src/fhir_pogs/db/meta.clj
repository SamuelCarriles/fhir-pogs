(ns fhir-pogs.db.meta
  (:require [fhir-pogs.db :refer [jdbc-execute!]]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))

(defn get-columns-info [db-spec table-name]
  (->> (jdbc-execute! db-spec (-> (help/select :column-name :data-type)
                             (help/from :information-schema.columns)
                             (help/where [:= :table-schema "public"]
                                         [:= :table-name (name table-name)])
                             (help/order-by :ordinal-position)
                             sql/format))
       (mapv (fn [col] 
              (into {} (map (fn [[k v]] 
                              [(keyword (name k)) (keyword v)])
                            col))))))

