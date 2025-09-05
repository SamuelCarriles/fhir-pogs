 (ns fhir-pogs.search.core
  (:require [clojure.string :as str]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [fhir-pogs.db :as db]
            [fhir-pogs.core :refer [search-resources!]]
            [cheshire.core :refer [parse-string generate-string]]
            [fhir-search.uri-query :refer [parse]]
            [fhir-search.complex :refer [clean]]
            [fhir-pogs.search.modifiers :as modifiers]
            [fhir-pogs.core :as crud]))

(def search-params (:entry (parse-string (slurp "resources/search-parameters.json") true)))
(def compartment-definitions (:entry (parse-string (slurp "resources/compartment-definitions.json") true)))

(defn get-param-data [restype param]
  (let [parse-where #(if-not (nil? %) (re-find #"^.*?(?=\.where.*)|^.*$" %) nil)
        res (first (filter #(and (= param (get-in % [:resource :code]))
                                 (contains? (set (get-in % [:resource :base])) restype)) search-params))
        path (when res (->> (str/split (get-in res [:resource :expression]) #" \| ")
                            (map #(re-find (re-pattern (str "(?<=" restype "\\.).*")) %))
                            (remove nil?)
                            (reduce #(conj %1 (parse-where %2)) [])))
        type (get-in res [:resource :type])]
    (when res {:search-param param
               :path path
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

(defn extract-join [join]
  (when (keyword? join) (-> join name keyword)))

(defn string-search-conds  [type path param]
  (let [join (-> (:join param) extract-join)
        base (if join [join] [])]
    (if-let [params (:params param)]
      (reduce #(->> (modifiers/get-string-data-cond path (:modifier %2) (:value %2))
                    (conj %1)) base params)
      (modifiers/get-string-data-cond path (:modifier param) (:value param)))))

(defn gen-params-cond [type join params]
  (let [join (extract-join join)]
    (reduce #(let [param-data (get-param-data type (:name %2))]
             (case (:data-type param-data)
               :string (conj %1 (string-search-conds type (:path param-data) %2))
               :token []
               :date []))
          (if (or (< 1 (count params)) (= :or join)) [join] []) params)))

(defn gen-cond-clauses [ast]
  (let [type (:type ast)
        id (:id ast)
        join (:join ast)
        params (:params ast)
        compartment (:compartment ast)
        compartment-cond (modifiers/jsonb-path-exists
                          (compartment-paths type compartment)
                          (str " ? (@ == \"" (:type compartment) "/" (:id compartment) "\")"))
        params-cond (gen-params-cond type join params)]
    (-> (conj [:and [:= :resourceType type]] (when id [:= :resource-id id]) compartment-cond params-cond)
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



  (let [ast {:type "Patient"
             :join :fhir.search.join/and
             :params [{:name "name"
                       :value "John"}]}
        type (:type ast)
        join (:join ast)
        params (:params ast)]
    (reduce #(let [param-data (get-param-data type (:name %))]
               (case (:data-type param-data)
                 :string (string-search-conds type (:path param-data) %)
                 :token []
                 :date []))
            [(extract-join join)] params))

  (string-search-conds (:path (get-param-data "Patient" "given")) "Patient" {:value "John"})
(gen-cond-clauses (parse "/Patient?given=Sam,alt&family=Smith"))
 (get-param-data "Patient" "family")
  :.)


