(ns fhir-pogs.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [fhir-pogs.test-utils :as test-utils]
            [fhir-pogs.db :as db]
            [fhir-pogs.core :as crud]
            [cheshire.core :refer [parse-string]]
            [clojure.java.io :refer [resource]]
            [next.jdbc :refer [get-datasource]]
            [config.core :as cfg]
            [config.dotenv :as dotenv]
            [clojure.core :as c]))

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

(use-fixtures :once (fn [f] (test-utils/drop-all-tables! connectable) (f)))
(use-fixtures :each (fn [f] (f) (test-utils/drop-all-tables! connectable)))

;;; ============================================================================
;;; SAVE-RESOURCE! TESTS
;;; ============================================================================

(deftest test-save-resource-basic
  (testing "Basic save without mapping fields"
    (let [table-prefix "core_testing"
          resource (rand-nth test-resources)
          result (crud/save-resource! connectable table-prefix resource)
          tables (db/get-tables connectable)
          main-columns (db/get-columns connectable :core_testing_main)
          ;; Verificar datos guardados en BD
          saved (db/execute-one! connectable
                                 ["SELECT * FROM core_testing_main WHERE resource_id = ?" (:id resource)])]

      ;; Verificaciones de resultado
      (is (= resource (first result)))
      (is (= 1 (count result)))

      ;; Verificaciones de estructura de BD
      (is (= #{:core_testing_main} tables))
      (is (= #{:content :resourceType :resource_id} main-columns))

      ;; Verificaciones de datos guardados
      (is (= (:id resource) (:core_testing_main/resource_id saved)))
      (is (= (:resourceType resource) (:core_testing_main/resourceType saved))))))

(deftest test-save-resource-with-mapping
  (testing "Save with mapping fields creates specific table"
    (let [table-prefix "core_testing"
          resource (first (filter #(= (:resourceType %) "Patient") test-resources))
          result (crud/save-resource! connectable table-prefix [:gender :active :name] resource)
          tables (db/get-tables connectable)
          patient-columns (db/get-columns connectable :core_testing_patient)
          ;; Verificar datos en tabla específica
          saved-patient (db/execute-one! connectable
                                         ["SELECT * FROM core_testing_patient WHERE id = ?" (:id resource)])]

      (is (= resource (first result)))
      (is (= 1 (count result)))
      (is (= #{:core_testing_main :core_testing_patient} tables))
      (is (= #{:name :resourceType :active :id :gender} patient-columns))

      ;; Verificar datos guardados correctamente
      (is (= (:id resource) (:core_testing_patient/id saved-patient)))
      (is (= (:gender resource) (:core_testing_patient/gender saved-patient)))
      (is (= (:active resource) (:core_testing_patient/active saved-patient))))))

(deftest test-save-resource-with-defaults
  (testing "Using :defaults option maps meta and text"
    (let [resource (second (filter #(= (:resourceType %) "Condition") test-resources))
          table-prefix "core_testing"]
      (crud/save-resource! connectable table-prefix [:defaults] resource)

      (is (= #{:core_testing_main :core_testing_condition} (db/get-tables connectable)))
      (is (= #{:id :resourceType :meta :text} (db/get-columns connectable :core_testing_condition)))

      ;; Verificar datos
      (let [saved (db/execute-one! connectable
                                   ["SELECT * FROM core_testing_condition WHERE id = ?" (:id resource)])]
        (is (= (:id resource) (:core_testing_condition/id saved)))))))

(deftest test-save-resource-error-handling
  (testing "Throws on resource without :id"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"The key :id is required"
                          (crud/save-resource! connectable "test" {:resourceType "Patient" :name "Test"}))))

  (testing "Throws on resource without :resourceType"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"The key :resourceType is required"
                          (crud/save-resource! connectable "test" {:id "123" :name "Test"}))))

  (testing "Throws on invalid connectable"
    (is (thrown? Exception
                 (crud/save-resource! "not-a-db" "test" (first test-resources)))))

  (testing "Throws on invalid mapping-fields (not a vector)"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"mapping-fields param must be a vector"
                          (crud/save-resource! connectable "test" {:invalid "map"} (first test-resources)))))

  (testing "Throws on empty mapping-fields"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Mapping-fields param is empty"
                          (crud/save-resource! connectable "test" [] (first test-resources))))))

;;; ============================================================================
;;; SAVE-RESOURCES! TESTS
;;; ============================================================================

(deftest test-save-resources-basic
  (testing "Basic resource collection save - only main table"
    (let [result (crud/save-resources! connectable "core_testing" test-resources)
          tables (db/get-tables connectable)
          saved-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main"]))]

      (is (= 8 (count result)))
      (is (= 8 saved-count))
      (is (= #{:core_testing_main} tables))
      (is (= #{:resource_id :resourceType :content} (db/get-columns connectable :core_testing_main))))))

(deftest test-save-resources-single-mapping
  (testing "Single mapping type with same resource type"
    (let [observations (filter #(= (:resourceType %) "Observation") test-resources)
          _ (crud/save-resources! connectable "core_testing" :single [:status :subject] observations)
          obs-with-status-or-subj (filter #(or (:status %) (:subject %)) observations)
          tables (db/get-tables connectable)
          obs-columns (db/get-columns connectable :core_testing_observation)
          saved-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main"]))]

      (is (= 3 saved-count))
      (is (= #{:core_testing_main :core_testing_observation} tables))
      (is (= #{:resourceType :id :status :subject} obs-columns))

      ;; Verificar que todos los observations tienen sus datos
      (doseq [obs obs-with-status-or-subj]
        (let [saved (db/execute-one! connectable
                                     ["SELECT * FROM core_testing_observation WHERE id = ?" (:id obs)])]
          (is (some? saved))
          (is (= (:id obs) (:core_testing_observation/id saved))))))))

(deftest test-save-resources-single-mapping-errors
  (testing "Single mapping throws on different resource types"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"don't have .* resourceType"
                          (crud/save-resources! connectable "core_testing" :single [:status]
                                                [(first (filter #(= (:resourceType %) "Patient") test-resources))
                                                 (first (filter #(= (:resourceType %) "Observation") test-resources))]))))

  (testing "Single mapping throws when mapping-fields is not a vector"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"mapping-fields param must be a vector"
                          (crud/save-resources! connectable "core_testing" :single {:bad "map"} test-resources)))))

(deftest test-save-resources-specialized-mapping
  (testing "Specialized mapping with specific resource configurations"
    (crud/save-resources!
     connectable
     "core_testing"
     :specialized
     {:patient [:name :gender]
      :observation [:code :status :subject]
      :condition [:defaults :subject]}
     test-resources)

    (let [tables (db/get-tables connectable)
          main-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main"]))
          patient-count (count (db/execute! connectable ["SELECT id FROM core_testing_patient"]))
          obs-count (count (db/execute! connectable ["SELECT id FROM core_testing_observation"]))
          cond-count (count (db/execute! connectable ["SELECT id FROM core_testing_condition"]))]

      (is (= 8 main-count))
      (is (= 3 patient-count))
      (is (= 3 obs-count))
      (is (= 2 cond-count))

      (is (= #{:core_testing_main :core_testing_patient
               :core_testing_observation :core_testing_condition}
             tables))

      (is (= #{:resourceType :id :name :gender}
             (db/get-columns connectable :core_testing_patient)))
      (is (= #{:resourceType :id :code :status :subject}
             (db/get-columns connectable :core_testing_observation)))
      (is (= #{:resourceType :id :meta :text :subject}
             (db/get-columns connectable :core_testing_condition))))))

(deftest test-save-resources-specialized-with-all
  (testing "Specialized mapping using :all option applies to all resources"
    (crud/save-resources!
     connectable
     "core_testing"
     :specialized
     {:all [:defaults]}
     test-resources)

    (let [tables (db/get-tables connectable)
          main-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main"]))
          patient-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main WHERE \"resourceType\"= ?" "Patient"]))
          obs-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main WHERE \"resourceType\"= ?" "Observation"]))
          cond-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main WHERE \"resourceType\"= ?" "Condition"]))]

      (is (= 8 main-count))
      (is (= 3 patient-count))
      (is (= 3 obs-count))
      (is (= 2 cond-count))
      (is (= #{:core_testing_main :core_testing_condition} tables))
      (is (= #{:resourceType :id :meta :text} (db/get-columns connectable :core_testing_condition))))))

(deftest test-save-resources-specialized-with-others
  (testing "Specialized mapping using :others for unspecified types"
    (crud/save-resources!
     connectable
     "core_testing"
     :specialized
     {:patient [:identifier]
      :others [:defaults]}
     test-resources)

    (let [patient-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main WHERE \"resourceType\" = ?" "Patient"]))
          patient-with-identifier-count (count (db/execute! connectable ["SELECT id FROM core_testing_patient"]))
          obs-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main WHERE \"resourceType\" = ?" "Observation"]))
          cond-count (count (db/execute! connectable ["SELECT resource_id FROM core_testing_main WHERE \"resourceType\" = ?" "Condition"]))
          cond-with-defaults-count (count (db/execute! connectable ["SELECT id FROM core_testing_condition"]))]

      (is (= 3 patient-count))
      (is (= 3 obs-count))
      (is (= 2 cond-count))

      (is (= 2 patient-with-identifier-count))
      (is (= 1 cond-with-defaults-count))

      (is (not (db/table-exists? connectable :core_testing_observation)))

      (is (= #{:resourceType :id :identifier} (db/get-columns connectable :core_testing_patient)))
      (is (= #{:resourceType :id :meta :text} (db/get-columns connectable :core_testing_condition))))))

(deftest test-save-resources-error-handling
  (testing "Throws on non-sequential resources"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"resources param must be a coll"
                          (crud/save-resources! connectable "test" {:not "a vector"}))))

  (testing "Throws on invalid mapping-type"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"mapping-type given is invalid"
                          (crud/save-resources! connectable "test" :invalid [:field] test-resources))))

  (testing "Throws when mapping-type is not a keyword"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"mapping-type must be a keyword"
                          (crud/save-resources! connectable "test" "single" [:field] test-resources))))

  (testing "Throws on specialized mapping with non-map mapping-fields"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"mapping-fields param must be a map"
                          (crud/save-resources! connectable "test" :specialized [:field] test-resources)))))

(deftest test-save-resources-transaction-rollback
  (testing "Transaction rolls back on error - nothing is saved"
    (let [bad-resources (conj test-resources {:id "bad" :resourceType "Bad" :invalid true})]
      (is (thrown? Exception
                   (crud/save-resources! connectable "core_testing" bad-resources)))
      ;; Verificar que NO se guardó nada
      (is (empty? (filter #(re-find #"core_testing" (name %)) (db/get-tables connectable)))))))

 ;;; ============================================================================
 ;;; SEARCH-RESOURCES TESTS
 ;;; ============================================================================

(deftest test-search-resources-basic
  (testing "Search resources with simple condition"
    (let [patient (first (filter #(= (:resourceType %) "Patient") test-resources))]
      (crud/save-resource! connectable "test" [:gender :active] patient)

      (let [found (crud/search-resources connectable "test" "Patient"
                                         [[:= :resource_id (:id patient)]])]
        (is (= 1 (count found)))
        (is (= patient (first found)))))))

(deftest test-search-resources-multiple-conditions
  (testing "Search with multiple conditions using AND"
    (let [patients (filter #(= (:resourceType %) "Patient") test-resources)]
      (crud/save-resources! connectable "test" :single [:gender :active] patients)

      ;; Buscar pacientes activos
      (let [active-patients (crud/search-resources connectable "test" "Patient"
                                                   [[:= :active true]])]
        (is (seq active-patients))
        (is (every? #(= true (:active %)) active-patients))))))

(deftest test-search-resources-no-results
  (testing "Search returns empty vector when no matches"
    (crud/save-resource! connectable "test" (first test-resources))

    (let [found (crud/search-resources connectable "test" "Patient"
                                       [[:= :resource_id "non-existent-id"]])]
      (is (empty? found))
      (is (vector? found)))))

(deftest test-search-resources-without-specific-table
  (testing "Search works even when specific resource table doesn't exist"
    (let [patient (first (filter #(= (:resourceType %) "Patient") test-resources))]
      ;; Guardar sin mapping fields (solo main table)
      (crud/save-resource! connectable "test" patient)

      (let [found (crud/search-resources connectable "test" "Patient"
                                         [[:= :resource_id (:id patient)]])]
        (is (= 1 (count found)))
        (is (= patient (first found)))))))

(deftest test-search-resources-error-handling
  (testing "Throws on invalid conditions parameter"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"conditions must be a vector"
                          (crud/search-resources connectable "test" "Patient" {:not "a vector"})))))


;;; ============================================================================
 ;;; DELETE-RESOURCE! TESTS
 ;;; ============================================================================

(deftest test-delete-resources-basic
  (testing "Delete resource and verify removal"
    (let [patient (first (filter #(= (:resourceType %) "Patient") test-resources))]
      (crud/save-resource! connectable "test" [:gender] patient)

      ;; Verificar que existe
      (let [before (crud/search-resources connectable "test" "Patient"
                                          [[:= :resource_id (:id patient)]])]
        (is (= 1 (count before))))

      ;; Eliminar
      (crud/delete-resource! connectable "test" "Patient" (:id patient))

      ;; Verificar que ya no existe
      (let [after (crud/search-resources connectable "test" "Patient"
                                         [[:= :resource_id (:id patient)]])]
        (is (empty? after))))))

(deftest test-delete-resources-multiple
  (testing "Delete multiple resources matching conditions"
    (let [patients (filter #(= (:resourceType %) "Patient") test-resources)]
      (crud/save-resources! connectable "test" :single [:active] patients)

      ;; Eliminar todos los pacientes activos
      (let [active-patients (crud/search-resources connectable "test" "Patient" [[:= :active true]])]
        (doseq [patient active-patients]
          (crud/delete-resource! connectable "test" "Patient" (:id patient))))


      ;; Verificar que se eliminaron
      (let [remaining (crud/search-resources connectable "test" "Patient" [])]
        (is (every? #(not= true (:active %)) remaining))))))

(deftest test-delete-resources-no-match
  (testing "Delete returns nil when no resources match"
    (crud/save-resource! connectable "test" (first test-resources))

    (let [result (crud/delete-resource! connectable "test" "Patient" "non-existent")]
      (is (nil? result)))))

(deftest test-delete-resources-error-handling
  (testing "Throws on invalid id parameter"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"id must be a non-empty string"
                          (crud/delete-resource! connectable "test" "Patient" "")))))


;;; ============================================================================
;;; UPDATE-RESOURCE! TESTS
;;; ============================================================================

(deftest test-update-resource-basic
  (testing "Update resource and verify changes"
    (let [patient (first (filter #(= (:resourceType %) "Patient") test-resources))
          updated-patient (assoc patient :gender "other" :active false)]

      (crud/save-resource! connectable "test" [:gender :active] patient)

      ;; Actualizar
      (let [result (crud/update-resource! connectable "test" "Patient"
                                          (:id patient) updated-patient)]
        (is (= updated-patient (first result))))

      ;; Verificar en BD
      (let [saved (db/execute-one! connectable
                                   ["SELECT * FROM test_patient WHERE id = ?" (:id patient)])]
        (is (= "other" (:test_patient/gender saved)))
        (is (= false (:test_patient/active saved)))))))

(deftest test-update-resource-main-table-only
  (testing "Update resource with no specific table"
    (let [patient (first (filter #(= (:resourceType %) "Patient") test-resources))
          updated-patient (assoc patient :name [{:family "Updated"}])]

      ;; Guardar sin campos específicos
      (crud/save-resource! connectable "test" patient)

      ;; Actualizar
      (let [result (crud/update-resource! connectable "test" "Patient"
                                          (:id patient) updated-patient)]
        (is (= updated-patient (first result))))

      ;; Verificar en main table
      (let [found (crud/search-resources connectable "test" "Patient"
                                         [[:= :resource_id (:id patient)]])]
        (is (= updated-patient (first found)))))))

(deftest test-update-resource-error-handling
  (testing "Throws on mismatched resourceType"
    (let [patient (first (filter #(= (:resourceType %) "Patient") test-resources))
          wrong-type (assoc patient :resourceType "Observation")]
      (crud/save-resource! connectable "test" patient)

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"must have the same :id and :resourceType"
                            (crud/update-resource! connectable "test" "Patient"
                                                   (:id patient) wrong-type))))
    (test-utils/drop-all-tables! connectable))

  (testing "Throws on mismatched id"
    (let [patient (first (filter #(= (:resourceType %) "Patient") test-resources))
          wrong-id (assoc patient :id "different-id")]
      (crud/save-resource! connectable "test" patient)

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"must have the same :id and :resourceType"
                            (crud/update-resource! connectable "test" "Patient"
                                                   (:id patient) wrong-id))))
    (test-utils/drop-all-tables! connectable))

  (testing "Throws on invalid new-content"
    (let [patient (first (filter #(= (:resourceType %) "Patient") test-resources))]
      (crud/save-resource! connectable "test" patient)

      (is (thrown? Exception
                   (crud/update-resource! connectable "test" "Patient"
                                          (:id patient) {:invalid "resource"}))))
    (test-utils/drop-all-tables! connectable)))

(deftest test-update-resource-non-existent
  (testing "Returns nil when updating non-existent resource"
    (let [patients (filter #(= (:resourceType %) "Patient") test-resources)]
      (crud/save-resources! connectable "test" patients)
      (is (nil? (crud/update-resource! connectable "test" "Patient"
                                       "non-existent-id" (assoc (first patients) :id "non-existent-id")))))))

;;; ============================================================================
;;; INTEGRATION TESTS
;;; ============================================================================

(deftest test-full-crud-cycle
  (testing "Complete CRUD cycle: save, search, update, delete"
    (let [patient (first (filter #(= (:resourceType %) "Patient") test-resources))
          updated-patient (assoc patient :active false)]

      ;; CREATE
      (crud/save-resource! connectable "test" [:active :gender] patient)

      ;; READ
      (let [found (crud/search-resources connectable "test" "Patient"
                                         [[:= :resource_id (:id patient)]])]
        (is (= 1 (count found)))
        (is (= patient (first found))))

      ;; UPDATE
      (crud/update-resource! connectable "test" "Patient" (:id patient) updated-patient)
      (let [found (crud/search-resources connectable "test" "Patient"
                                         [[:= :resource_id (:id patient)]])]
        (is (= false (:active (first found)))))

      ;; DELETE
      (crud/delete-resource! connectable "test" "Patient" (:id patient))
      (let [found (crud/search-resources connectable "test" "Patient"
                                         [[:= :resource_id (:id patient)]])]
        (is (empty? found))))))

(deftest test-multiple-resources-different-types
  (testing "Handle multiple resource types in same database"
    (crud/save-resources! connectable "test" :specialized
                          {:patient [:active]
                           :observation [:status]
                           :condition [:defaults]}
                          test-resources)

    ;; Verificar cada tipo
    (let [patients (crud/search-resources connectable "test" "Patient" [])
          observations (crud/search-resources connectable "test" "Observation" [])
          conditions (crud/search-resources connectable "test" "Condition" [])]

      (is (= 3 (count patients)))
      (is (= 3 (count observations)))
      (is (= 2 (count conditions)))

      ;; Verificar que cada uno tiene su estructura correcta
      (is (every? #(contains? % :active) patients))
      (is (= 2 (count (filter :status observations))))
      (is (= 1 (count (filter :meta conditions)))))))


