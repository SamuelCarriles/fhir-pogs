(ns fhir-pogs.validator-test
  (:require [clojure.test :refer [deftest is testing]]
            [fhir-pogs.validator :as validator]
            [fhir-pogs.db :as db]))

;; Sample FHIR resources
(def valid-patient
  {:id "patient-123"
   :resourceType "Patient"
   :active true
   :name [{:given ["John"] :family "Doe"}]
   :gender "male"})

(def valid-observation
  {:id "obs-456"
   :resourceType "Observation"
   :status "final"
   :code {:coding [{:system "http://loinc.org" :code "15074-8"}]}})

(def invalid-resource-no-id
  {:resourceType "Patient"
   :active true
   :name [{:given ["Jane"] :family "Smith"}]})

(def invalid-resource-no-type
  {:id "patient-789"
   :active false
   :name [{:given ["Bob"] :family "Johnson"}]})

;; Tests for basic resource validation
(deftest test-validate-resource-basics
  (testing "valid resource passes validation"
    (is (nil? (validator/validate-resource-basics valid-patient nil))))

  (testing "resource without id throws exception"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid resource. The key :id is required."
         (validator/validate-resource-basics invalid-resource-no-id nil))))

  (testing "resource without resourceType throws exception"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid resource. The key :resourceType is required."
         (validator/validate-resource-basics invalid-resource-no-type nil))))

  (testing "non-map resource throws exception"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"The resource must be a map."
         (validator/validate-resource-basics "not-a-map" nil))))

  (testing "exception includes resource index when provided"
    (try
      (validator/validate-resource-basics invalid-resource-no-id 2)
      (catch clojure.lang.ExceptionInfo e
        (is (= 2 (:resource-index (ex-data e))))))))

;; Tests for multiple resources validation
(deftest test-validate-resources-basics
  (testing "valid resources pass validation"
    (is (nil? (validator/validate-resources-basics [valid-patient valid-observation]))))

  (testing "resources with missing id fail"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid resource. The key :id is required."
         (validator/validate-resources-basics [valid-patient invalid-resource-no-id]))))

  (testing "resources with missing resourceType fail"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid resource. The key :resourceType is required."
         (validator/validate-resources-basics [valid-patient invalid-resource-no-type]))))

  (testing "exception includes correct resource index"
    (try
      (validator/validate-resources-basics [valid-patient invalid-resource-no-id])
      (catch clojure.lang.ExceptionInfo e
        (is (= 1 (:resource-index (ex-data e))))))))

;; Tests for database spec validation
(deftest test-validate-db-connectable
  (testing "valid connectable passes"
    (is (nil? (validator/validate-db-connectable {:dbtype "postgresql" :host "localhost"})))
    (is (nil? (validator/validate-db-connectable "jdbc:postgresql://localhost:5432/resources?user=postgres&password=postgres")))))

;; Tests for mapping fields basic validation
(deftest test-validate-mapping-fields-basic
  (testing "valid mapping fields pass"
    (is (nil? (validator/validate-mapping-fields-basic [:active :name :gender]))))

  (testing "non-vector mapping fields fail"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"The mapping-fields param must be a vector."
         (validator/validate-mapping-fields-basic {:meta :name}))))

  (testing "empty mapping fields fail"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Mapping-fields param is empty."
         (validator/validate-mapping-fields-basic [])))))

;; Tests for mapping fields schema validation
(deftest test-validate-mapping-fields
  (testing "valid single mapping passes"
    (is (nil? (validator/validate-mapping-fields [:meta :name {:active :boolean}] :single))))

  (testing "valid specialized mapping passes"
    (is (nil? (validator/validate-mapping-fields {:patient [:active :name]
                                                  :observation [:status :code]} :specialized))))

  (testing "empty mapping fields fail"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid mapping-fields schema."
         (validator/validate-mapping-fields [] :single))))

  (testing "invalid single mapping fails - non-keyword/map elements"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid mapping-fields schema."
         (validator/validate-mapping-fields [:active "invalid-string" :name] :single))))

  (testing "invalid specialized mapping fails - non-keyword keys"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid mapping-fields schema."
         (validator/validate-mapping-fields {"invalid-key" [:meta :name]} :specialized))))

  (testing "invalid specialized mapping fails - empty vectors"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid mapping-fields schema."
         (validator/validate-mapping-fields {:patient []} :specialized))))

  (testing "nested validation works for specialized mapping"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid mapping-fields schema."
         (validator/validate-mapping-fields {:patient [:name "invalid-string"]} :specialized)))))

;; Tests for table columns validation (mocking db functions)
(deftest test-validate-table-columns
  (testing "FHIR fields compatible with existing columns pass"
    (with-redefs [db/get-columns (fn [_ _] #{:id :resourcetype :active :text :deceased})]
      (is (nil? (validator/validate-table-columns {} "fhir_resources_patient" [:active :text :deceased])))))

  (testing "mapping fields incompatible with existing columns fail"
    (with-redefs [db/get-columns (fn [_ _] #{:id :resourcetype :active :text})]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"The table fhir_resources_patient already exists"
           (validator/validate-table-columns {} "fhir_resources_patient" [:active :text :gender])))))

  (testing "defaults keyword gets expanded to [:meta :text]"
    (with-redefs [db/get-columns (fn [_ _] #{:id :resourcetype :meta :text :active})]
      (is (nil? (validator/validate-table-columns {} "fhir_resources_patient" [:defaults :active])))))

  (testing "field type definitions work correctly"
    (with-redefs [db/get-columns (fn [_ _] #{:id :resourcetype :active :some-field})]
      (is (nil? (validator/validate-table-columns {} "fhir_resources_patient" [:active {:some-field :text}])))))

  (testing "empty columns set allows any mapping"
    (with-redefs [db/get-columns (fn [_ _] #{})]
      (is (nil? (validator/validate-table-columns {} "fhir_resources_patient" [:active :text :gender]))))))

;; Tests for valid-resource? function
(deftest test-valid-resource
  (testing "valid-resource? returns boolean"
    ;; Mocking validate-resource to avoid actual FHIR schema validation
    (with-redefs [validator/validate-resource (fn [_] true)]
      (is (true? (validator/valid-resource? valid-patient))))

    (with-redefs [validator/validate-resource (fn [_] (throw (Exception. "Invalid")))]
      (is (false? (validator/valid-resource? {:invalid "resource"}))))))

;; Error data structure tests
(deftest test-error-structures
  (testing "resource validation errors have correct structure"
    (try
      (validator/validate-resource-basics invalid-resource-no-id 1)
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (is (= :resource-validation (:type data)))
          (is (= :id (:param data)))
          (is (nil? (:value data)))
          (is (= "non-blank string" (:expected data)))
          (is (= "nil value" (:got data)))
          (is (= 1 (:resource-index data)))))))

  (testing "mapping fields validation errors have correct structure"
    (try
      (validator/validate-mapping-fields [] :single)
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (is (= :schema-validation (:type data)))
          (is (vector? (:errors data))))))))