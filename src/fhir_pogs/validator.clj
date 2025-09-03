(ns fhir-pogs.validator
  (:require [json-schema.core :refer [validate]]
            [cheshire.core :refer [parse-string]]
            [fhir-pogs.db :as db]))

(def fhir-schema (parse-string (slurp "resources/fhir-schema.json") true))


;; Helper functions
  (defn- type-name [x] (-> x type .getSimpleName))
  
  (defn- create-error
    ([expected got] {:expected expected :got got})
    ([expected got index] {:expected expected :got got :index index}))
  
  (defn validate-resource [resource]
    (try
      (validate fhir-schema resource)
      (catch clojure.lang.ExceptionInfo e
        (let [errors (->> (ex-data e) :errors set vec)]
          (throw (ex-info (str "Invalid resource schema\n" (.getMessage e))
                          {:type :schema-validation :errors errors}))))))
  
  (defn valid-resource? [resource]
    (try
      (validate-resource resource)
      true
      (catch Exception _ false))) 

;; Specific validators
  (defn- validate-keyword-or-map [item idx]
    (when-not (or (keyword? item) (map? item))
      [(create-error "A keyword or a keyword-to-keyword map"
                     (str (type-name item) " element")
                     [idx])]))
  
  (defn- validate-keyword-map [item idx]
    (when (map? item)
      (->> item
           (map-indexed
            (fn [map-idx [k v]]
              (cond-> []
                (not (keyword? k))
                (conj (create-error "A keyword-to-keyword map"
                                    (str (type-name k) " as key")
                                    [idx map-idx 0]))
                (not (keyword? v))
                (conj (create-error "A keyword-to-keyword map"
                                    (str (type-name v) " as value")
                                    [idx map-idx 1])))))
           (apply concat))))
  
  (defn- validate-single-mapping [mf]
    (->> mf
         (map-indexed (fn [idx item]
                        (concat (validate-keyword-or-map item idx)
                                (validate-keyword-map item idx))))
         (apply concat)))
  
  (defn- validate-specialized-entry [[k v] entry-idx]
    (let [key-errors (when-not (keyword? k)
                       [(create-error "A keyword as key"
                                      (str (type-name k) " element")
                                      [entry-idx 0])])
          value-errors (cond
                         (empty? v)
                         [(create-error "non-empty vector as value"
                                        "empty vector"
                                        [entry-idx 1])]
  
                         (not (vector? v))
                         [(create-error "A vector as value"
                                        (str (type-name v) " element")
                                        [entry-idx 1])]
  
                         :else
                         (->> (validate-single-mapping v)
                              (map #(update % :index (partial into [entry-idx 1])))))]
      (concat key-errors value-errors)))
  
  (defn- validate-specialized-mapping [mf]
    (->> mf
         (map-indexed (fn [idx [k v]]
                        (validate-specialized-entry [k v] idx)))
         (apply concat)))
  
  (defn- validate-empty-structure [mf]
    (when (empty? mf)
      [(cond
         (vector? mf) (create-error "non-empty vector" "empty vector")
         (map? mf) (create-error "non-empty map" "empty map")
         :else nil)]))
  
  (defn validate-mapping-fields [mf mt]
    (let [errors (concat (validate-empty-structure mf)
                         (when-not (empty? mf)
                           (case mt
                             :single (validate-single-mapping mf)
                             :specialized (validate-specialized-mapping mf)
                             [])))]
      (when (seq errors)
        (throw (ex-info "Invalid mapping-fields schema."
                        {:type :schema-validation :errors (vec errors)})))))

(defn validate-resource-basics
  "Validates basic resource requirements (id and resourceType)."
  [resource resource-index]
  (cond
    (not (map? resource))
    (throw (ex-info "The resource must be a map."
                    {:type :argument-validation
                     :name :resource
                     :expected "non-empty map"
                     :got (-> resource type .getSimpleName)}))
    
    (not (:id resource))
    (throw (ex-info "Invalid resource. The key :id is required."
                    (cond-> {:type :resource-validation
                             :param :id
                             :value nil
                             :expected "non-blank string"
                             :got "nil value"}
                      resource-index (assoc :resource-index resource-index))))

    (not (:resourceType resource))
    (throw (ex-info "Invalid resource. The key :resourceType is required."
                    (cond-> {:type :resource-validation
                             :param :resourceType
                             :value nil
                             :expected "non-blank string"
                             :got "nil value"}
                      resource-index (assoc :resource-index resource-index))))
))

(defn validate-resources-basics
  "Validates basic requirements for a collection of resources."
  [resources]
  (let [resources-vec (vec resources)]
    (doseq [[idx resource] (map-indexed vector resources-vec)]
      (when-not (:id resource)
        (throw (ex-info "Invalid resource. The key :id is required."
                        {:type :resource-validation
                         :param :id
                         :value nil
                         :expected "non-empty string"
                         :got "nil value"
                         :resource-index idx})))
      (when-not (:resourceType resource)
        (throw (ex-info "Invalid resource. The key :resourceType is required."
                        {:type :resource-validation
                         :param :resourceType
                         :value nil
                         :expected "non-empty string"
                         :got "nil value"
                         :resource-index idx}))))))

(defn validate-resources-schema
  "Validates all resources against schema."
  [resources]
  (let [resources-vec (vec resources)]
    (doseq [[idx resource] (map-indexed vector resources-vec)]
      (when-not (valid-resource? resource)
        (throw (ex-info (str "The resource at index " idx " is invalid.")
                        (assoc {:type :schema-validation}
                               :errors (->> resource validate-resource ex-data :errors))))))))

(defn validate-db-spec
  "Validates database specification."
  [db-spec]
  (when-not (map? db-spec)
    (throw (ex-info "The db-spec must be a map."
                    {:type :argument-validation
                     :name :db-spec
                     :expected "non-empty map"
                     :got (-> db-spec type .getSimpleName)}))))

(defn validate-mapping-fields-basic
  "Basic validation for mapping-fields parameter."
  [mapping-fields]
  (cond
    (not (vector? mapping-fields))
    (throw (ex-info "The mapping-fields param must be a vector."
                    {:type :argument-validation
                     :name :mapping-fields
                     :expected "non-empty vector"
                     :got (-> mapping-fields type .getSimpleName)}))

    (empty? mapping-fields)
    (throw (ex-info "Mapping-fields param is empty."
                    {:type :argument-validation
                     :name :mapping-fields
                     :expected "non-empty vector"
                     :got (-> mapping-fields type .getSimpleName)}))))


(defn- process-fields
  "Processes mapping fields, expanding :defaults and flattening structure."
  [mapping-fields]
  (->> mapping-fields
       (replace {:defaults [:meta :text]})
       flatten
       (reduce #(if (map? %2)
                  (into %1 (keys %2))
                  (conj %1 %2))
               [])))

(defn validate-table-columns
  "Validates that mapping fields are compatible with existing table columns."
  [db-spec table mapping-fields]
  (let [columns (db/get-columns db-spec table)
        fields (process-fields mapping-fields)]
    (when (and (seq columns)
               (not-every? #(contains? columns %) fields))
      (let [valid-fields (vec (remove #{:id :resourcetype} columns))]
        (throw (ex-info (str "The table " table " already exists and you can only map these fields " valid-fields)
                        {:type :argument-validation
                         :name :mapping-fields
                         :value (->> mapping-fields (replace {:defaults [:meta :text]}) flatten vec)
                         :expected valid-fields
                         :got "invalid fields to map"}))))))