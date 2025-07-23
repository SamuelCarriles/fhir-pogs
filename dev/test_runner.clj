(ns test-runner
  (:require [clojure.test :refer [run-tests]] 
            [fhir-pogs.db :refer [table-remove!]]
            [fhir-pogs.mapper-test]
            [fhir-pogs.core]))

(def db-spec {:dbtype "postgresql"
              :dbname "resources"
              :host "localhost"
              :user "postgres"
              :port "5432"
              :password "postgres"})

(defn -main []
  (run-tests 'fhir-pogs.mapper-test
             'fhir-pogs.core)
  (table-remove! db-spec [:all]))
