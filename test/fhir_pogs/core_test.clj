(ns fhir-pogs.core-test
  (:require [test-tools.core :refer [read-json-files]]
            [clojure.test :refer [deftest is]]
            [fhir-pogs.core :as crud]
            [fhir-pogs.mapper :as mapper]
            [fhir-pogs.db :as db]
            [honey.sql.helpers :as h]
            [honey.sql :as sql]))

(def db-spec {:dbtype "postgresql"
              :dbname "resources"
              :host "localhost"
              :user "postgres"
              :port "5432"
              :password "postgres"})

(deftest testing-save-resources!
  (let [select (-> (h/select :*) (h/from :testing_main) sql/format)
        resources (map mapper/parse-resource (read-json-files))
        res-count (count resources)]
    (crud/save-resources! db-spec "testing" resources)
    (is (= res-count (count (db/jdbc-execute! db-spec select))))
    (db/table-remove! db-spec [:all])
    (crud/save-resource! db-spec "testing" (rand-nth resources))
    (is (= res-count (count (db/jdbc-execute! db-spec select))))))



