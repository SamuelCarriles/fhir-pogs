(ns fhir-pogs.search
  (:require [clojure.string :as str]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [fhir-pogs.db :as db]
            [fhir-pogs.core :refer [search-resources!]]
            [cheshire.core :refer [parse-string generate-string]]
            [next.jdbc :as jdbc]
            [fhir-search.uri-query :refer [parse]]))

(def search-params (:entry (parse-string (slurp "resources/search-parameters.json") true)))
(def compartment-definitions (:entry (parse-string (slurp "resources/compartment-definitions.json") true)))


(defn param-path [restype param]
  (let [paths (map #(get-in % [:resource :expression]) (filter #(= param (get-in % [:resource :code])) search-params))] 
    (->> (map #(re-find (re-pattern (str "(?<=" restype "\\.).*")) %) (mapcat #(str/split % #" \| ") paths))
         (remove nil?)
         first
         (re-find #"^.*?(?=\.where.*)|^.*$"))))

(defn compartment-paths [type compartment]
  (when-let [refres (:type compartment)]
    (->> compartment-definitions
         (map :resource)
         (filter #(= (:code %) refres))
         (mapcat :resource)
         (filter #(= (:code %) type))
         (mapcat :param)
         (map #(str % ".reference")))))

(defn gen-comparator
  ([v]
   (str " ? (@ == \"" v "\")"))
  ([k v]
   (str k v)))

(defn jsonb-paths-exists [path comp]
  (when-not (or (nil? path) (not (seq path)) (nil? comp) (not (string? comp)) (str/blank? comp))
    (let [p (if (coll? path) path (vector path))
          conds (reduce
                 #(conj %1
                        [:jsonb_path_exists :content [:cast (str "$." %2 ".**" comp) :jsonpath]])
                 '() p)]
      (if (> (count p) 1) (vec (conj conds :or)) (first conds)))))


(defn gen-param-cond [path m v]
  (if (nil? m) (jsonb-paths-exists path (str "? (@ ==\"" v "\")"))
      (let [op (-> m name keyword)
            sv (str "\"" v "\"")
            values (str/split v #"\|")
            comp  (case op
                    :exact (str " ? (@ ==" sv ")")
                    :contains (str " ? (@ like_regex \".*" v ".*\")")
                    :missing ""
                    :not (str " ? (@ !=" sv ")")
                    :text (str " ? (@ like_regex \".*" v ".*\")")
                    :in (reduce #(str %1 "@ == \"" %2 "\"" (if (= %2 (last values)) ")" " || ")) " ? (" values)
                    :not-in (reduce #(str %1 "@ != \"" %2 "\"" (if (= %2 (last values)) ")" " && ")) " ? (" values)
                    :identifier (str " ? (@.system ==" (first values) "&& @.value ==" (second values) ")")
                    :code-text (str " ? (@ like %" v "%)")
                    :iterate (str " ? (@ ==" sv ")")
                    :of-type (str " ? (@.type == " sv ")")
                    (throw (IllegalArgumentException. "Incorrect modifier.")))]
        (jsonb-paths-exists path comp))))
;;ajustar esto para cuando sea :above, :below, :text-advanced

(defn gen-params-cond [restype join params]
  (when (and restype join params)
    (let [join (-> join name keyword)
          conditions (map (fn [{:keys [name params modifier join value] :as param}]
                            (cond
                              (and name value (nil? params))
                              (gen-param-cond (param-path restype name) modifier value)
                              ;;
                              (and name params (nil? value)) (str "params work" "bro")
                              :else nil)) params)]
      (if (> (count params) 1) (vec (conj conditions :and)) (first conditions)))))



(defn gen-cond-clauses [ast]
  (let [type (:type ast)
        id (:id ast)
        join (:join ast)
        params (:params ast)
        compartment (:compartment ast)
        compartment-cond (jsonb-paths-exists
                          (compartment-paths type compartment)
                          (gen-comparator (str (:type compartment) "/" (:id compartment))))
        params-cond (gen-params-cond type join params)]
    (-> [[:= :resourceType type]]
        (conj (when id [:= :resource-id id]) compartment-cond params-cond))))

(defn search-fhir! [db-spec table-prefix uri]
  (let [ast (parse uri)
        conditions (gen-cond-clauses ast)]
    (search-resources! db-spec table-prefix (:type ast) conditions))) 

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
  
  (search-fhir! db-spec "testing" "/Patient/example/ClinicalAssessment?finding-code:exact=850.0")
  (search-fhir! db-spec "testing" "/Patient?given=Juan&family=Pérez")
 
  :.
  )


