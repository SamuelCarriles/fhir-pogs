(ns test-tools.core
  (:require [clojure.java.io :refer [file]]))

(defn read-json-files []
  (->> (file "test/resources/json")
       .listFiles
       (filter #(and (.isFile %) (.endsWith (.getName %) ".json")))
       (mapv slurp)))