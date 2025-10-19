(ns fhir-pogs.search.param-data
  (:require [clojure.string :as str]
            [cheshire.core :refer [parse-string]]))

(def search-params (:entry (parse-string (slurp "resources/search-parameters.json") true)))
(def compartment-definitions (:entry (parse-string (slurp "resources/compartment-definitions.json") true)))

(defn get-data [restype search-param]
  (let [parse-where #(if-not (nil? %) (re-find #"^.*?(?=\.where.*)|^.*$" %) nil)
        res (first (filter #(and (= search-param (get-in % [:resource :code]))
                                 (contains? (set (get-in % [:resource :base])) restype)) search-params))
        path (when res (->> (str/split (get-in res [:resource :expression]) #" \| ")
                            (map #(re-find (re-pattern (str "(?<=" restype "\\.).*")) %))
                            (remove nil?)
                            (mapv #(parse-where %))))
        type (get-in res [:resource :type])]
    (when res {:param search-param
               :paths path
               :data-type (keyword type)})))

(defn compartment-paths [type compartment]
  (when-let [refres (:type compartment)]
    (->> compartment-definitions
         (map :resource)
         (filter #(= (:code %) refres))
         (mapcat :resource)
         (filter #(= (:code %) type))
         (mapcat :param)
         (mapv #(str % ".reference")))))