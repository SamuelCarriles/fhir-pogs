(ns fhir-pogs.core-test
  (:require [test-tools.core :refer [read-json-files]]
            [clojure.test :refer [deftest is]]
            [fhir-pogs.core :as crud]
            [fhir-pogs.mapper :as mapper]))

(def db-spec {:dbtype "postgresql"
              :dbname "resources"
              :host "localhost"
              :user "postgres"
              :port "5432"
              :password "postgres"})

(deftest testing-save-resources!
  (is (crud/save-resources! db-spec "testing" :specialized {:all [:meta :text]} (map mapper/parse-resource (read-json-files)))))