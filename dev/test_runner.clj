(ns test-runner
  (:require [clojure.test :refer [run-tests]] 
            [fhir-pogs.mapper-test]
            [fhir-pogs.core-test]))

(def db-spec {:dbtype "postgresql"
              :dbname "resources"
              :host "localhost"
              :user "postgres"
              :port "5432"
              :password "postgres"})

(defn -main []
  (run-tests 'fhir-pogs.mapper-test
             'fhir-pogs.core-test))
