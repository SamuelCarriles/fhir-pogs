 (ns fhir-pogs.search.core
  (:require 
            [fhir-pogs.db :as db] 
            [fhir-pogs.search.modifiers :as modifiers]
            [fhir-pogs.core :as crud]))



#_(defn extract-join [join]
  (when (keyword? join) (-> join name keyword)))

#_(defn string-search-conds  [type path param]
  (let [join (-> (:join param) extract-join)
        base (if join [join] [])]
    (if-let [params (:params param)]
      (reduce #(->> (modifiers/get-string-data-cond path (:modifier %2) (:value %2))
                    (conj %1)) base params)
      (modifiers/get-string-data-cond path (:modifier param) (:value param)))))

#_(defn gen-params-cond [type join params]
  (let [join (extract-join join)]
    (reduce #(let [param-data (get-param-data type (:name %2))]
               (case (:data-type param-data)
                 :string (conj %1 (string-search-conds type (:path param-data) %2))
                 :token []
                 :date []))
            (if (or (< 1 (count params)) (= :or join)) [join] []) params)))

#_(defn gen-cond-clauses [ast]
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

#_(defn fhir-search [db-spec table-prefix uri]
  (let [ast (parse uri)
        conditions (gen-cond-clauses ast)]
    (crud/search-resources db-spec table-prefix (:type ast) conditions)))

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
  

  (string-search-conds "Patient" (:path (get-param-data "Patient" "given")) {:value "John"})
  (gen-cond-clauses (parse "/Patient?given=Sam,alt&family=Smith"))
  (get-param-data "Patient" "family")
  
  
  
  
  :.
  )



