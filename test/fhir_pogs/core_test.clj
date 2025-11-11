(ns fhir-pogs.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures run-test]]
            [fhir-pogs.db :as db]
            [fhir-pogs.core :as crud]
            [cheshire.core :refer [parse-string]]
            [clojure.java.io :refer [resource]]
            [next.jdbc :refer [get-datasource]]
            [config.core :as cfg]
            [config.dotenv :as dotenv]))

(def test-resources
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


(db/drop-tables! connectable :all)

(defn clean-tables-fixture [f]
  (f)
  (db/drop-tables! connectable :all))

(use-fixtures :each clean-tables-fixture)

(deftest test-save-resource!
  (testing "Basic save without mapping fields"
    (let [table-prefix "core_testing"
          resource (rand-nth test-resources)
          result (crud/save-resource! connectable table-prefix resource)
          tables (db/get-tables connectable)
          spec-columns #{:content :resourceType :resource_id}]
      (is (= resource (first result)))
      (is (= 1 (count result)))
      (is (= 1 (count tables)))
      (is (= :core_testing_main (first tables)))
      (is (= spec-columns (db/get-columns connectable :core_testing_main)))))
  ;;
  (testing "Advanced save with mapping fields"
    (let [table-prefix "core_testing"
          resource (first test-resources)
          result (crud/save-resource! connectable table-prefix [:gender :active :name] resource)
          tables (db/get-tables connectable)
          spec-tables #{:core_testing_main :core_testing_patient}
          spec-columns #{:name :resourceType :active :id :gender}]
      (is (= resource (first result)))
      (is (= 1 (count result)))
      (is (= spec-tables tables))
      (is (= spec-columns (db/get-columns connectable :core_testing_patient))))))

(deftest test-table-generation
  (testing "Map the fields correctly for a known resource."
    (let [resource (first (filter #(= (:id %) "obs-2") test-resources))
          spec-tables #{:core_testing_main :core_testing_observation}
          spec-columns #{:id :resourceType :status :code}]
      (crud/save-resource! connectable "core_testing" [:code :status] resource)
      (is (= spec-tables (db/get-tables connectable)))
      (is (= spec-columns (db/get-columns connectable :core_testing_observation))))
    (db/drop-tables! connectable :all))
  (testing "Using :defaults option"
    (let [resource (first (filter #(= (:id %) "condition-2") test-resources))
          spec-tables #{:core_testing_main :core_testing_condition}
          columns #{:id :resourceType :meta :text}]
      (db/drop-tables! connectable :all)
      (crud/save-resource! connectable "core_testing" [:defaults] resource)
      (is (= spec-tables (db/get-tables connectable)))
      (is (= columns (db/get-columns connectable :core_testing_condition))))))

(deftest test-save-resources!
  (testing "Basic resource collection save"
    (crud/save-resources! connectable "core_testing" test-resources)
    (is (= (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main"]))
           (count test-resources)))
    (is (= #{:resource_id :resourceType :content} (db/get-columns connectable :core_testing_main)))
    (db/drop-tables! connectable :all))
  ;;
  (testing "Advanced resource collection save with :single mapping type"
    (let [observations (filter #(= (:resourceType %) "Observation") test-resources)]
      (crud/save-resources! connectable "core_testing" :single [:status :subject] observations)
      (is (= (count observations)
             (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main"]))))
      (is (and (db/table-exists? connectable :core_testing_main)
               (db/table-exists? connectable :core_testing_observation)))
      (is (= #{:resourceType :id :status :subject} (db/get-columns connectable :core_testing_observation))))
    (db/drop-tables! connectable :all))
;;
  (testing "Advanced resource collection save with :specialized mapping type"
    (crud/save-resources!
     connectable
     "core_testing"
     :specialized
     {:patient [:name]
      :observation [:code :status :subject]
      :condition [:defaults :subject]}
     test-resources)
    
    (is (= (count test-resources) 
           (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main"]))))
    (is (= 3 (count (db/execute! connectable ["SELECT id FROM core_testing_patient"]))))
    (is (= 3 (count (db/execute! connectable ["SELECT id FROM core_testing_observation"]))))
    (is (= 2 (count (db/execute! connectable ["SELECT id FROM core_testing_condition"]))))
    
    (is (= #{:resourceType :id :name} (db/get-columns connectable :core_testing_patient)))
    (is (= #{:resourceType :id :code :status :subject} (db/get-columns connectable :core_testing_observation)))
    (is (= #{:resourceType :id :meta :text :subject} (db/get-columns connectable :core_testing_condition)))
    (db/drop-tables! connectable :all))
  ;;
  (testing "Advanced resource collection save with :specialized mapping type using :all option"
    (crud/save-resources!
     connectable
     "core_testing"
     :specialized
     {:all [:defaults]}
     test-resources)
    
    (is (= (count test-resources)
           (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main"]))))
    (is (= 1 (count (db/execute! connectable ["SELECT id FROM core_testing_condition"])))) 
    (db/drop-tables! connectable :all))
  ;;
  (testing "Advanced resource collection save with :specialized mapping type using :others option"
    (crud/save-resources!
     connectable
     "core_testing"
     :specialized
     {:patient [:identifier]
      :others [:defaults]}
     test-resources)
  
    (is (= (count test-resources)
           (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main"]))))
    (is (= 1 (count (db/execute! connectable ["SELECT id FROM core_testing_condition"]))))
    (is (= 2 (count (db/execute! connectable ["SELECT id FROM core_testing_patient"]))))
    (db/drop-tables! connectable :all)))




(comment
  ;;To test database connection
  
  :.)


