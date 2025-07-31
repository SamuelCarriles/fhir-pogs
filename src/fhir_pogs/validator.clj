(ns fhir-pogs.validator
  (:require [json-schema.core :refer [validate]]
            [cheshire.core :refer [parse-string]]))

(def fhir-schema (parse-string (slurp "resources/fhir-schema.json") true))

(defn validate-resource [resource]
  (validate fhir-schema resource))

(defn valid? [resource]
  (try (validate-resource resource) true 
       (catch Exception _ nil)))
