(ns fhir-pogs.query
  (:require [fhir-pogs.db.meta :refer [get-columns-info]]
            [fhir-pogs.db :refer [jdbc-execute! get-tables]] 
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))


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
(get-tables db-spec)
  :.)