(ns fhir-pogs.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures run-test]]
            [fhir-pogs.db :as db]
            [fhir-pogs.core :as crud]
            [cheshire.core :refer [parse-string]]
            [clojure.java.io :refer [resource]]
            [next.jdbc :refer [get-datasource]]
            [config.core :as cfg]
            [config.dotenv :as dotenv]))

(def load-test-resources
  (-> "fixtures/test-bundle.json"
      resource
      slurp
      (parse-string true)
      :entry
      (->> (mapv :resource))))

(def connectable
  (-> {}
      (cfg/patch (dotenv/parse ".env")
                 [[:uri {:env "TEST_DATABASE_URL" :of-type :cfg/url}]])
      :uri
      get-datasource))


(defn clean-tables-fixture [f]
  (db/drop-tables! connectable :all)
  f)

(use-fixtures :once clean-tables-fixture)

(deftest test-save-resource!
  (testing "Basic save without mapping fields"
    (let [table-prefix "core_testing"
          resource (rand-nth load-test-resources)
          result (crud/save-resource! connectable table-prefix resource)]
      (is (= resource result))
      (is (= 1 (count result))) 

      (let [tables (db/get-tables connectable)]
        (is (= 1 (count tables)))
        (is (= :core_testing_main (first tables)))

        (let [columns (db/get-columns connectable :core_testing_main)
              spec-columns #{:content :resourceType :resource_id}]
          (is (= spec-columns columns))))))
  ;;
  (testing "Advanced save with mapping fields"
    (let [table-prefix "core_testing"
          resource (first load-test-resources)
          result (crud/save-resource! connectable table-prefix [:gender :active :name] resource)]
      (is (= resource result))
      (is (= 1 (count result)))
      
      (let [tables (db/get-tables connectable)
            spec-tables #{:core_testing_main :core_testing_patient}]
        (is (= spec-tables tables))
        (let [special-table-columns (db/get-columns connectable :core_testing_patient)
              spec-columns #{:name :resourceType :active :id :gender}]
          (is (= special-table-columns spec-columns)))))))


