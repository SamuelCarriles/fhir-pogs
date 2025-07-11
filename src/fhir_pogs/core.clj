(ns fhir-pogs.core
  (:require [cheshire.core :refer [parse-string]]))

(defn parse-resource "Parse a json resource to a clojure map."
  [json]
  (parse-string json true))
