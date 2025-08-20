(ns fhir-pogs.search.core
  (:require [clojure.string :as str]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [fhir-pogs.db :as db]
            [fhir-pogs.core :refer [search-resources!]]
            [cheshire.core :refer [parse-string generate-string]]
            [fhir-search.uri-query :refer [parse]]
            [fhir-search.complex :refer [clean]]
            [fhir-pogs.core :as crud]))

(def search-params (:entry (parse-string (slurp "resources/search-parameters.json") true)))
(def compartment-definitions (:entry (parse-string (slurp "resources/compartment-definitions.json") true)))

(defn param-path [restype param]
  (let [paths (map #(get-in % [:resource :expression]) (filter #(= param (get-in % [:resource :code])) search-params))
        parse-where #(if-not (nil? %) (re-find #"^.*?(?=\.where.*)|^.*$" %) nil)]
    (->> (map #(re-find (re-pattern (str "(?<=" restype "\\.).*")) %) (mapcat #(str/split % #" \| ") paths))
         (remove nil?)
         first
         parse-where)))

(defn compartment-paths [type compartment]
  (when-let [refres (:type compartment)]
    (->> compartment-definitions
         (map :resource)
         (filter #(= (:code %) refres))
         (mapcat :resource)
         (filter #(= (:code %) type))
         (mapcat :param)
         (map #(str % ".reference")))))

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
            comp (case op
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
                   :above (str " ? (@ starts with " sv ")")
                   :below (str " ? (" sv " starts with @)") ;;Revisar soporte para below
                   (throw (IllegalArgumentException. (str "Incorrect modifier: " m))))]
        (jsonb-paths-exists path comp))))
;;ajustar esto para cuando sea :above, :below, :text-advanced

(defn param-process
  ([restype param]
   (let [base (if-let [join (:join param)]
                [(-> join name keyword)] [])
         {:keys [name params modifier value]} param
         parse-param (fn [n m v] (gen-param-cond (param-path restype n) m v))]
     (if params
       (reduce (fn [o {:keys [modifier value] :as full-param}]
                 (conj o (parse-param n modifier value)))
               base params)
       (parse-param name modifier value))))
  ([restype type param]
   (cond
     (= :composite type)
     (let [base (if-let [join (:join param)]
                  [(-> join name keyword)] [])
           {:keys [name params modifier value]} param
           parse-param (fn [n m v] (gen-param-cond (param-path restype n) m v))]
       (if params
         (reduce (fn [o {:keys [modifier value] :as full-param}]
                   (conj o (parse-param n modifier value)))
                 base params)
         (parse-param name modifier value)))
     ;;
     (= :chained type) [])))



(defn gen-params-cond [restype join prms]
  (when (and restype join prms)
    (let [join (-> join name keyword)
          conditions (map (fn [{:keys [name params value] :as param}]
                            (cond
                              (and name value (nil? params))
                              (param-process restype param)
                              ;;
                              (and name params (nil? value)) (cond
                                                               (:composite param) (param-process restype :composite param)
                                                               (:chained param) (param-process restype :chained param)
                                                               :else (param-process restype param))
                              :else nil)) prms)]
      (if (> (count prms) 1) (vec (conj conditions join)) (first conditions)))))



(defn gen-cond-clauses [ast]
  (let [type (:type ast)
        id (:id ast)
        join (:join ast)
        params (:params ast)
        compartment (:compartment ast)
        compartment-cond (jsonb-paths-exists
                          (compartment-paths type compartment)
                          (str " ? (@ == \"" (:type compartment) "/" (:id compartment) "\")"))
        params-cond (gen-params-cond type join params)]
    (-> (conj [[:= :resourceType type]] (when id [:= :resource-id id]) compartment-cond params-cond)
        clean)))

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



  {:type "Patient"
   :join :fhir.search.join/and
   :params [{:name "family"
             :join :fhir.search.join/or
             :params [{:value "Doe"}
                      {:value "Carriles"}]}
            {:name "given"
             :join :fhir.search.join/or
             :params [{:value "Jhon"}
                      {:value "Sam"}]}]}
  ;;URI: "Patient?family=Doe,Carriles&given=John,Sam"
  
  (search-fhir! db-spec "testing" "Patient?family=Pereh,Pérez&given=John,Juan")

  (param-process "Observation" {:name "code-quantity",
                                :join :fhir.search.join/or,
                                :params [{:name "code", :value "loinc|12907-2"} {:name "value", :value "150"}],
                                :composite true}) 
  :.
  )


