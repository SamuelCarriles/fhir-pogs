(ns fhir-pogs.validator
  (:require [json-schema.core :refer [validate]]
            [cheshire.core :refer [parse-string]]))

(def fhir-schema (parse-string (slurp "resources/fhir-schema.json") true))

(defn validate-resource [resource] 
  (try (validate fhir-schema resource)
       (catch clojure.lang.ExceptionInfo e 
         (let [errors (->> (ex-data e) :errors set vec)]
           (throw (ex-info (str "Invalid resource schema\n" (.getMessage e)) {:type :schema-validation
                                                                      :errors errors}))))))

(defn valid? [resource]
  (try (validate-resource resource) true 
       (catch Exception _ nil)))

