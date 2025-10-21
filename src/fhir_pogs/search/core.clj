 (ns fhir-pogs.search.core
  (:require [fhir-pogs.db :as db]
            [fhir-pogs.search.modifiers :as modifiers]
            [fhir-pogs.core :as crud]
            [fhir-pogs.search.param-data :as param]
            [fhir-search.complex :refer [clean]]
            [fhir-search.uri-query :refer [parse]]
            [fhir-pogs.search.token :refer [token-search-conds]]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]))



(defn extract-join [join]
    (when (keyword? join) (-> join name keyword)))

#_(defn string-search-conds  [type path param]
    (let [join (-> (:join param) extract-join)
          base (if join [join] [])]
      (if-let [params (:params param)]
        (reduce #(->> (modifiers/get-string-data-cond path (:modifier %2) (:value %2))
                      (conj %1)) base params)
        (modifiers/get-string-data-cond path (:modifier param) (:value param)))))

(defn gen-params-cond [db-spec table-prefix type join params]
    (let [join (extract-join join)]
      (reduce #(let [param-data (param/get-data type (:name %2))]
                 (->> (case (:data-type param-data)
                        :string [] #_(conj %1 (string-search-conds type (:path param-data) %2))
                        :token (token-search-conds db-spec table-prefix type params)
                        :date [])
                      (into %1)))
              (if (or (< 1 (count params)) (= :or join)) [join] []) params)))

(defn gen-cond-clauses [db-spec table-prefix ast]
    (let [type (:type ast)
          id (:id ast)
          join (:join ast)
          params (:params ast)
          compartment (:compartment ast)
          compartment-cond (modifiers/jsonb-path-exists
                            (param/compartment-paths type compartment)
                            (str " ? (@ == \"" (:type compartment) "/" (:id compartment) "\")"))
          params-cond (gen-params-cond db-spec table-prefix type join params)] 
      (-> (conj [[:= :resourceType type]] (when id [:= :resource-id id]) compartment-cond params-cond)
          clean)))

(defn fhir-search [db-spec table-prefix uri]
    (let [ast (parse uri) 
          conditions (gen-cond-clauses db-spec table-prefix ast)]
      (crud/search-resources db-spec table-prefix (:type ast) conditions)))

(comment
  ;;Tenemos que saber que las clausulas condicionales van a tener este formato:
  ;; [:operador :campo :valor]
  
  (def db-spec {:dbtype "postgresql"
                :dbname "resources"
                :host "localhost"
                :user "postgres"
                :port "5432"
                :password "postgres"}) 

  (fhir-search db-spec "fhir" "/Practitioner?gender=male")
  :.
  )



