(ns fhir-pogs.search
  (:require [clojure.string :as str]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [fhir-pogs.db :as db]
            [fhir-pogs.core :refer [search-resources!]]
            [cheshire.core :refer [parse-string generate-string]]))

(def search-params (:entry (parse-string (slurp "resources/search-parameters.json") true)))
(def compartment-definitions (:entry (parse-string (slurp "resources/compartment-definitions.json") true)))


(defn param-path [restype param]
  (let [paths (map #(get-in % [:resource :expression]) (filter #(= param (get-in % [:resource :code])) search-params))]
    (->> (map #(re-find (re-pattern (str "(?<=" restype "\\.).*")) %) (mapcat #(str/split % #" \| ") paths))
         (remove nil?)
         first)))

(defn compartment-paths [type compartment]
  (let [refres (:type compartment)]
    (->> compartment-definitions
         (map :resource)
         (filter #(= (:code %) refres))
         (mapcat :resource)
         (filter #(= (:code %) type))
         (mapcat :param))))


(comment
  ;; Esta es la forma de buscar en jsonb: [:jsonb_path_exists :content [:cast "$.**.given.**? (@ == \"Isabel\" )" :jsonpath]]

  ;;Tenemos que saber que las clausulas condicionales van a tener este formato:
  ;; [:operador :campo :valor]

  (def db-spec {:dbtype "postgresql"
                :dbname "resources"
                :host "localhost"
                :user "postgres"
                :port "5432"
                :password "postgres"})


  (def example {:type "Condition"
                :compartment {:type "Patient"
                              :id "p123"}})


  (defn jsonb-path-exists [path]
    [:jsonb_path_exists :content [:cast (str "$." path) :jsonpath]])

  (defn gen-cond-clauses [ast]
    (let [type (:type ast) 
          compartment (:compartment ast)]
      [[:= :resourceType type]]))

  (gen-cond-clauses example)
  (defn find-val [path m v]
    (when (nil? m) (str "? (@ ==\"" v "\")"))
    (let [op (-> m name keyword)
          sv (str "\"" v "\"")
          values (str/split v #"\|")
          jsonpath (->> (case op
                          :exact (str "? (@ ==" sv ")")
                          :contains (str "? (@ like %" v "%)")
                          :missing ""
                          :not (str "? (@ !=" sv ")")
                          :text (str "? (@ like %" v "%)")
                          :in (reduce #(str %1 "@ == \"" %2 "\"" (if (= %2 (last values)) ")" " || ")) "? (" values)
                          :not-in (reduce #(str %1 "@ != \"" %2 "\"" (if (= %2 (last values)) ")" " && ")) "? (" values)
                          :identifier (str " ? (@.system==" (first values) "&& @.value==" (second values) ")")
                          :code-text (str "? (@ like %" v "%)")
                          :iterate (str "? (@ ==" sv ")")
                          :of-type (str " ? (@.type == " sv ")"))
                        (str "$." path ".**"))]
      [:jsonb_path_exists :content [:cast jsonpath :jsonpath]]))
  ;;ajustar esto para cuando sea :above, :below, :text-advanced


  





  :.)


