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
  (let [resources (map mapper/parse-resource (read-json-files))
        res-count (count resources)]
    (is (= 1 (count (crud/save-resource! db-spec "testing" (rand-nth resources)))))
    (db/table-remove! db-spec [:all])
    (is (= res-count (count (crud/save-resources! db-spec "testing" resources))))
    (db/table-remove! db-spec [:all])
    (is (= res-count (count (crud/save-resources! db-spec "testing" :specialized {:all [:defaults]} resources))))
    (db/table-remove! db-spec [:all])
    (is (= res-count (count (crud/save-resources! db-spec "testing" :specialized {:all [:defaults :id :resourceType]} resources))))
    (db/table-remove! db-spec [:all])))
