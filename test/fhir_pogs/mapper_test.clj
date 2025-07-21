(ns fhir-pogs.mapper-test
  (:require [clojure.test :refer [deftest is testing]] 
            [fhir-pogs.mapper :refer [parse-resource
                                       template
                                       insert-to-sentence
                                       create-table
                                       save-resources!]] 
            [test-tools.core :refer [read-json-files]]))

(deftest test-parse-resource
  (doseq [resource (read-json-files)]
    (testing "Parse a json file to a clojure map"
      (is (map? (parse-resource resource)) (str "The json was not parse correctly. This is the json string: " resource)))))


(deftest test-template-and-insert-to-sentence
  (testing "Create a map template to generate insert-to SQL sentences"
    (doseq [resource (mapv parse-resource (read-json-files))]
      (let [tem (template "example" [:meta :text] resource)
            sentence (insert-to-sentence tem (:resourceType resource))]
        (is (map? tem) (str "Resource " resource " can't convert to a map template."))
        (is (and (seq? sentence) (every? vector? sentence)))))))

(deftest test-create-table
  (testing "Make a SQL sentence to create a table"
    (doseq [n ["fhir" "resources" "server" "table" "example" "asdf" "qwerty" "fdsa" "test" "clojure01"]
            t (:resourceType (mapv parse-resource (read-json-files)))]
      (let [m (create-table n)
            s (create-table n t {:meta :jsonb :text :jsonb})]
        (is (and (vector? m) (= 1 (count m)) (string? (first m)) (re-find #"_main" m)))
        (is (and (vector? s) (= 1 (count s)) (string? (first s))))))))


(def db-spec {:dbtype "postgresql"
              :dbname "resources"
              :host "localhost"
              :user "postgres"
              :port "5432"
              :password "postgres"})

(deftest test-db-resources-mapping
  (testing "Map resources to Postgres db"
    (is (save-resources! db-spec "testing" :specialized {:all [:defaults]} (map parse-resource (read-json-files))))))
(save-resources! db-spec "testing" :specialized {:all [:defaults]} (map parse-resource (read-json-files)))