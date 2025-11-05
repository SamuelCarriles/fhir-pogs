(ns fhir-pogs.mapper-test
   (:require [clojure.test :refer [deftest is testing run-tests]]
             [fhir-pogs.mapper :as mapper]
             [cheshire.core :as json])
   (:import [org.postgresql.util PGobject]))

 ;; Test fixtures - datos de ejemplo
 (def sample-patient
   {:id "patient-123"
    :resourceType "Patient"
    :birthDate "1990-05-15"
    :gender "male"
    :active true
    :name [{:given ["John"] :family "Doe"}]
    :meta {:versionId "1" :lastUpdated "2023-01-01T10:00:00Z"}
    :text {:status "generated" :div "<div>John Doe</div>"}})

 (def sample-json-patient
   (json/generate-string sample-patient))

 ;; Helper functions para testing
 (defn pg-object? [obj]
   (instance? PGobject obj))

 (defn pg-object-type [obj]
   (.getType obj))

 (defn pg-object-value [obj]
   (.getValue obj))

 ;; Tests para parse-resource
 (deftest test-parse-resource
   (testing "Parse valid JSON"
     (let [result (mapper/parse-resource sample-json-patient)]
       (is (map? result))
       (is (= "patient-123" (:id result)))
       (is (= "Patient" (:resourceType result)))))

   (testing "Parse invalid JSON throws exception"
     (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Failed to parse JSON"
          (mapper/parse-resource "{invalid json}"))))

   (testing "Non-string input throws precondition error"
     (is (thrown? AssertionError
                  (mapper/parse-resource {:not "a string"})))))

 ;; Tests para parse-jsonb-obj
 (deftest test-parse-jsonb-obj
   (testing "Parse valid PGobject"
     (let [pg-obj (doto (PGobject.)
                    (.setType "jsonb")
                    (.setValue sample-json-patient))
           result (mapper/parse-jsonb-obj pg-obj)]
       (is (map? result))
       (is (= "patient-123" (:id result)))))

   (testing "Handle nil object"
     (is (nil? (mapper/parse-jsonb-obj nil)))))

 ;; Tests para to-pg-obj
 (deftest test-to-pg-obj
   (testing "Create PGobject with string value"
     (let [result (mapper/to-pg-obj "text" "hello")]
       (is (pg-object? result))
       (is (= "text" (pg-object-type result)))
       (is (= "hello" (pg-object-value result)))))

   (testing "Create PGobject with collection value"
     (let [data {:key "value"}
           result (mapper/to-pg-obj "jsonb" data)]
       (is (pg-object? result))
       (is (= "jsonb" (pg-object-type result)))
       (is (= (json/generate-string data) (pg-object-value result)))))

   (testing "Nil type throws precondition error"
     (is (thrown? AssertionError (mapper/to-pg-obj nil "value"))))

   (testing "Empty type throws precondition error"
     (is (thrown? AssertionError (mapper/to-pg-obj "" "value"))))

   (testing "Nil value throws precondition error"
     (is (thrown? AssertionError (mapper/to-pg-obj "text" nil)))))

 ;; Tests para type-of
 (deftest test-type-of
   (testing "Collection type"
     (let [[type value] (mapper/type-of {:key "value"})]
       (is (= :jsonb type))
       (is (pg-object? value))
       (is (= "jsonb" (pg-object-type value)))))

   (testing "Boolean true"
     (let [[type value] (mapper/type-of "true")]
       (is (= :boolean type))
       (is (= true value))))

   (testing "Boolean false"
     (let [[type value] (mapper/type-of "false")]
       (is (= :boolean type))
       (is (= false value))))

   (testing "Date type"
     (let [[type value] (mapper/type-of "2023-05-15")]
       (is (= :date type))
       (is (pg-object? value))
       (is (= "date" (pg-object-type value)))))

   (testing "Full datetime"
     (let [[type value] (mapper/type-of "2023-01-01T10:00:00Z")]
       (is (= :timestamptz type))
       (is (pg-object? value))))

   (testing "Time only"
     (let [[type value] (mapper/type-of "14:30:00")]
       (is (= :timestamptz type))
       (is (pg-object? value))))

   (testing "Numeric decimal"
     (let [[type value] (mapper/type-of "123.45")]
       (is (= :numeric type))
       (is (instance? BigDecimal value))
       (is (= 123.45M value))))

   (testing "Integer"
     (let [[type value] (mapper/type-of "123")]
       (is (= :integer type))
       (is (= 123 value))))

   (testing "Large integer handles gracefully" 
     (let [[type value] (mapper/type-of "2147483648")]
       (is (= :bigint type))
       (is (instance? Long value))))

   (testing "UUID"
     (let [[type value] (mapper/type-of "urn:uuid:123e4567-e89b-12d3-a456-426614174000")]
       (is (= :uuid type))
       (is (pg-object? value))))

   (testing "Base64"
     (let [[type value] (mapper/type-of "SGVsbG8gV29ybGQ=")]
       (is (= :bytea type))
       (is (pg-object? value))))

   (testing "Default text type"
     (let [[type value] (mapper/type-of "random string")]
       (is (= :text type))
       (is (= "random string" value))))

   (testing "Nil value throws exception"
     (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Cannot determine type of nil value"
          (mapper/type-of nil)))))

 ;; Tests para create-table
 (deftest test-create-table
   (testing "Create main table"
     (let [result (mapper/create-table "fhir_resources")]
       (is (vector? result))
       (is (string? (first result)))
       (is (.contains (first result) "fhir_resources_main"))
       (is (.contains (first result) "resource_id"))
       (is (.contains (first result) "resourceType"))
       (is (.contains (first result) "content"))))

   (testing "Create resource-specific table"
     (let [result (mapper/create-table "fhir_resources" "Patient" {:birthDate :date :gender :text})]
       (is (vector? result))
       (is (string? (first result)))
       (is (.contains (first result) "fhir_resources_patient"))
       (is (.contains (first result) "birthDate"))
       (is (.contains (first result) "gender")))))

 ;; Tests para template
 (deftest test-template
   (testing "Create template with basic fields"
     (let [result (mapper/template "fhir_resources" [:birthDate :gender] sample-patient)]
       (is (map? result))
       (is (= "fhir_resources" (:table result)))
       (is (vector? (:fields result)))
       (is (>= (count (:fields result)) 4)))) ; id, resourceType, birthDate, gender + content

   (testing "Template includes content field"
     (let [result (mapper/template "fhir_resources" [:birthDate] sample-patient)
           content-field (first (filter #(= :content (:name %)) (:fields result)))]
       (is (some? content-field))
       (is (pg-object? (:value content-field)))))

   (testing "Template processes field values correctly"
     (let [result (mapper/template "fhir_resources" [:active :birthDate] sample-patient)
           active-field (first (filter #(= :active (:name %)) (:fields result)))
           birthdate-field (first (filter #(= :birthDate (:name %)) (:fields result)))]
       (is (boolean? (:value active-field)))
       (is (pg-object? (:value birthdate-field))))))

 ;; Tests para insert-to-sentence
 (deftest test-insert-to-sentence
   (testing "Generate insert statements"
     (let [template (mapper/template "fhir_resources" [:birthDate :gender] sample-patient)
           result (mapper/insert-to-sentence template "Patient")]
       (is (vector? result))
       (is (>= (count result) 1))
       (is (every? vector? result))
       (is (every? string? (map first result)))))

   (testing "Main table insert includes RETURNING clause"
     (let [template (mapper/template "fhir_resources" [:birthDate] sample-patient)
           statements (mapper/insert-to-sentence template "Patient")
           main-statement (first (filter #(.contains (first %) "_main") statements))]
       (is (some? main-statement))
       (is (.contains (first main-statement) "RETURNING"))))

   (testing "Resource table created when extra fields present"
     (let [template (mapper/template "fhir_resources" [:birthDate :gender :active] sample-patient)
           statements (mapper/insert-to-sentence template "Patient")]
       (is (>= (count statements) 1))
      ; Should have both main and resource table inserts when there are extra fields
       (is (some (fn [st] (some #(= "Patient" %) st)) statements)))))

 ;; Tests para fields-types
 (deftest test-fields-types
   (testing "Infer types from resource"
     (let [result (mapper/fields-types [:birthDate :active :name] sample-patient)]
       (is (= :date (:birthDate result)))
       (is (= :boolean (:active result)))
       (is (= :jsonb (:name result)))))

   (testing "Merge explicit types"
     (let [result (mapper/fields-types [:birthDate {:customField :text}] sample-patient)]
       (is (= :date (:birthDate result)))
       (is (= :text (:customField result)))))

   (testing "Handle defaults keyword"
     (let [result (mapper/fields-types [:defaults :birthDate] sample-patient)]
       (is (contains? result :meta))
       (is (contains? result :text))
       (is (contains? result :birthDate))
       (is (not (contains? result :defaults)))))

   (testing "Skip missing fields"
     (let [result (mapper/fields-types [:nonExistentField :birthDate] sample-patient)]
       (is (not (contains? result :nonExistentField)))
       (is (= :date (:birthDate result))))))

 ;; Tests para return-value-process
 (deftest test-return-value-process
   (testing "Process database return values"
     (let [pg-obj1 (doto (PGobject.) (.setType "jsonb") (.setValue "{\"test\": \"value1\"}"))
           pg-obj2 (doto (PGobject.) (.setType "jsonb") (.setValue "{\"test\": \"value2\"}"))
           db-result [[{:content pg-obj1} {:content pg-obj2}]]
           result (mapper/return-value-process db-result)]
       (is (vector? result))
       (is (= 2 (count result)))
       (is (every? map? result))))

   (testing "Filter out next.jdbc metadata"
     (let [pg-obj (doto (PGobject.) (.setType "jsonb") (.setValue "{\"test\": \"value\"}"))
           db-result [[{:content pg-obj}]]  ; Sin next.jdbc metadata
           result (mapper/return-value-process db-result)]
       (is (vector? result))
       (is (= 1 (count result)))
       (is (map? (first result)))))

   (testing "Handle empty input gracefully"
     (let [result (mapper/return-value-process [])]
       (is (vector? result))
       (is (empty? result))))

   (testing "Filter out next.jdbc metadata in realistic scenario"
     (let [pg-obj (doto (PGobject.) (.setType "jsonb") (.setValue "{\"test\": \"value\"}"))
           ;; Estructura más realista: metadata separada del contenido
           db-result [[{:content pg-obj} {:next.jdbc/update-count 1}]]
           result (mapper/return-value-process db-result)]
       (is (vector? result))
       (is (= 1 (count result))) ; Solo el contenido, metadata filtrada
       (is (map? (first result))))))

;; Integration tests
(deftest test-integration-workflow
  (testing "Complete workflow: parse -> template -> insert"
    (let [;; Parse JSON resource
          parsed-resource (mapper/parse-resource sample-json-patient)

          ;; Create template
          template (mapper/template "fhir_test" [:birthDate :gender :active] parsed-resource)

          ;; Generate insert statements
          statements (mapper/insert-to-sentence template "Patient")]

      (is (map? parsed-resource))
      (is (= "patient-123" (:id parsed-resource)))

      (is (map? template))
      (is (= "fhir_test" (:table template)))

      (is (vector? statements))
      (is (pos? (count statements)))
      (is (every? #(and (vector? %) (string? (first %))) statements))))

  (testing "Fields-types integration"
    (let [parsed-resource (mapper/parse-resource sample-json-patient)
          field-types (mapper/fields-types [:birthDate :active :name :defaults] parsed-resource)]
      (is (= :date (:birthDate field-types)))
      (is (= :boolean (:active field-types)))
      (is (= :jsonb (:name field-types)))
      (is (contains? field-types :meta))
      (is (contains? field-types :text)))))

;; Performance test (basic)
(deftest test-type-of-performance
  (testing "Type-of with regex patterns performs consistently"
    (let [test-values ["true" "2023-01-01" "123" "random text" "14:30:00"]
          start-time (System/nanoTime)]
      (dotimes [_ 1000]
        (doseq [value test-values]
          (mapper/type-of value)))
      (let [end-time (System/nanoTime)
            duration-ms (/ (- end-time start-time) 1000000.0)]
        (is (< duration-ms 1000) "Type detection should be fast")))))

;; Helper function to run all tests
(defn run-all-tests []
  (run-tests 'fhir-pogs.mapper-test))

;; Example usage comment
(comment
  ;; Para correr los tests:
  ;; (run-all-tests)
(run-all-tests)
  ;; Para correr un test específico:
  ;; (run-tests #'test-type-of)

  ;; Para correr tests desde la línea de comandos:
  ;; clojure -M:test -m clojure.test fhir-pogs.mapper-test
  )