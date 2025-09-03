(ns test-runner
  (:require [clojure.test :refer [run-tests]] 
            [fhir-pogs.mapper-test]
            [fhir-pogs.core-test]
            [fhir-pogs.validator-test]))

(defn -main []
  (run-tests 'fhir-pogs.mapper-test
             'fhir-pogs.core-test
             'fhir-pogs.validator-test))
