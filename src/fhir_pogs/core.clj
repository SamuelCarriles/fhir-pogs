(ns fhir-pogs.core
  (:require [fhir-pogs.mapper :as mapper]
            [fhir-pogs.db :as db]
            [fhir-pogs.validator :as v]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))

(defn- validate-resource-basics
  "Validates basic resource requirements (id and resourceType)."
  [resource resource-index]
  (cond
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

    (not (map? resource))
    (throw (ex-info "The resource must be a map."
                    {:type :argument-validation
                     :name :resource
                     :expected "non-empty map"
                     :got (-> resource type .getSimpleName)}))))

(defn- validate-resources-basics
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

(defn- validate-resources-schema
  "Validates all resources against schema."
  [resources]
  (let [resources-vec (vec resources)]
    (doseq [[idx resource] (map-indexed vector resources-vec)]
      (when-not (v/valid-resource? resource)
        (throw (ex-info (str "The resource at index " idx " is invalid.")
                        (assoc {:type :schema-validation}
                               :errors (->> resource v/validate-resource ex-data :errors))))))))

(defn- validate-db-spec
  "Validates database specification."
  [db-spec]
  (when-not (map? db-spec)
    (throw (ex-info "The db-spec must be a map."
                    {:type :argument-validation
                     :name :db-spec
                     :expected "non-empty map"
                     :got (-> db-spec type .getSimpleName)}))))

(defn- validate-mapping-fields-basic
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

(defn- validate-table-columns
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

(defn- build-insert-sentences
  "Builds SQL insert sentences for a resource."
  [table-prefix fields resource]
  (let [restype (:resourceType resource)
        template (mapper/template table-prefix (keys fields) resource)]
    (mapper/insert-to-sentence template restype)))

(defn save-resource! "Maps and saves a FHIR resource into a database.  
 \n- `db-spec`: the database specifications. For example: \n{:dbtype \"postgresql\", \n:dbname \"resources\", \n:host \"localhost\", \n:user \"postgres\", \n:port \"5432\", \n:password \"postgres\"}  
 \n- `table-prefix`: the prefix to the name of the tables where the resource should be mapped.  
 \n- `mapping-fields`: a vector containing the names of the fields from the resource to be mapped. The names are given in keyword format. Note that the resource must contain every field that is intended to be mapped. If a field is to be added to the table that is not present in the resource—perhaps for future use—it can be written within the vector as a map, where the key is the name of the field and the value is the data type that field will store. Some examples: `[:meta :text :active :deceased]`, `[:defaults {:some-field :type-of-field}]`  
 \n- `resource`: the FHIR resource converted into a Clojure map.  
 \nIf you only want to save the essentials fields (id,resourceType), you don't need to give mapping-fields. Example of usage:\n ```clojure\n (def test-1 (parse-string <json resource> true))

  (def db-spec {:dbtype \"postgresql\"
                :dbname \"resources\"
                :host \"localhost\"
                :user \"postgres\"
                :port \"5432\"
                :password \"postgres\"})

  (save-resource! db-spec \"fhir_resources\" [:defaults :active] test-1)
  ;;or if you only want essentials:
  (save-resource! db-spec \"fhir_resources\" test-1)"
  ([db-spec ^String table-prefix resource]
   ;;Validation
   (validate-db-spec db-spec)
   (validate-resource-basics resource nil) 
   (v/validate-resource resource)

   ;;Operation
   (let [base [(mapper/create-table table-prefix)]
         sentences (into base (build-insert-sentences table-prefix [] resource))]
     (mapper/return-value-process (db/transact! db-spec sentences))))

  ([db-spec ^String table-prefix mapping-fields resource]
   ;; Validation
   (validate-db-spec db-spec)
   (validate-mapping-fields-basic mapping-fields)
   (validate-resource-basics resource nil)

   ;; Validate table columns if table exists
   (let [table (str table-prefix "_" (.toLowerCase (:resourceType resource)))]
     (validate-table-columns db-spec table mapping-fields))

   (v/validate-mapping-fields mapping-fields :single)
   (v/validate-resource resource)
   ;;Operation
   (let [fields (mapper/fields-types mapping-fields resource)
         restype (:resourceType resource)
         base [(mapper/create-table table-prefix)]
         create-specific (when (some #(get resource %) (keys fields))
                           [(mapper/create-table table-prefix restype fields)])
         sentences (-> (concat base create-specific)
                       (into (build-insert-sentences table-prefix (keys fields) resource)))]
     (mapper/return-value-process (db/transact! db-spec sentences)))))


;; Parts of save-resources!

(defn- process-single-mapping
  "Processes resources with single mapping type."
  [db-spec table-prefix mapping-fields resources]
  ;; Validate mapping fields
  (v/validate-mapping-fields mapping-fields :single)

  ;; Validate table columns
  (let [first-restype (:resourceType (first resources))
        table (str table-prefix "_" (.toLowerCase first-restype))]
    (validate-table-columns db-spec table mapping-fields))

  ;; Validate all resources have same type
  (let [expected-type (:resourceType (first resources))]
    (doseq [resource (rest resources)]
      (when-not (= expected-type (:resourceType resource))
        (throw (ex-info (str "Invalid resources given to :single mapping-type. Resource with id \""
                             (:id resource) "\" don't have \"" expected-type "\" resourceType.")
                        {:type :argument-validation
                         :name :resources
                         :expected "coll of resources with the same :resourceType"
                         :got "coll of resources with different :resourceType"})))))

  ;; Build and execute transaction
  (let [fields (mapper/fields-types (remove #{:id :resourceType} mapping-fields) resources)
        base-sentences [(mapper/create-table table-prefix)]
        create-specific (when (some (fn [r] (some #(get r %) (keys fields))) resources)
                          [(mapper/create-table table-prefix (:resourceType (first resources)) fields)])
        insert-sentences (mapcat (fn [resource]
                                   (build-insert-sentences table-prefix (keys fields) resource))
                                 resources)]
    (->> (concat base-sentences create-specific insert-sentences)
         (db/transact! db-spec)
         mapper/return-value-process)))

(defn- get-resource-fields
  "Determines which fields to use for a resource based on mapping configuration."
  [mapping-fields restype-key]
  (or (:all mapping-fields)
      (restype-key mapping-fields)
      (:others mapping-fields)
      []))

(defn- process-specialized-mapping
  "Processes resources with specialized mapping type."
  [db-spec table-prefix mapping-fields resources]
  ;; Validate mapping fields
  (v/validate-mapping-fields mapping-fields :specialized)

  ;; Validate table columns for :all fields
  (when-let [all-fields (:all mapping-fields)]
    (let [existing-tables (->> (db/get-tables db-spec)
                               (filter #(and (re-find (re-pattern (str table-prefix ".*")) (name %))
                                             (not (re-find #"_main$" (name %))))))]
      (doseq [table existing-tables]
        (validate-table-columns db-spec (name table) all-fields))))

  ;; Build and execute transaction
  (let [base-sentences [(mapper/create-table table-prefix)]
        resource-sentences (mapcat (fn [resource]
                                     (let [restype (:resourceType resource)
                                           restype-key (keyword (.toLowerCase restype))
                                           fields-to-map (get-resource-fields mapping-fields restype-key)
                                           fields (mapper/fields-types (remove #{:id :resourceType} fields-to-map) resources)
                                           create-specific (when (some #(get resource %) (keys fields))
                                                             [(mapper/create-table table-prefix restype fields)])]
                                       (concat create-specific
                                               (build-insert-sentences table-prefix (keys fields) resource))))
                                   resources)]
    (->> (concat base-sentences resource-sentences)
         (db/transact! db-spec)
         mapper/return-value-process)))

(defn save-resources! "Works very similarly to `save-resource!`, with the difference that it handles multiple resources instead of just one. All resources are stored within a transaction. The resources can have two types of mapping:  
 \n- `:single`: all resources are of the same type, so they can be inserted into a single table. For this type, the mapping-fields parameter is a vector of fields in keyword format.  
 \n- `:specialized`: resources are of various types, so a separate table is created for each resource type. For this type, mapping-fields is a map where the keys are the resource types and the values are vectors containing the fields in keyword format. There are two reserved keywords: `:all` and `:others`. The first is used when specifying the fields to extract from all resources, and the second when specific resource types and their fields have been defined, and additional fields need to be specified for any other resource types. It is recommended to always include :others, but if omitted and a resource of an unspecified type is encountered, only the basic fields will be mapped in the main or controlling table. By basic fields we mean :id, :resourceType, and :content.  
 \nExample of a function call: \n```clojure \n(save-resources! db-spec \"fhir_reources\" :single [:defaults] <resources>) \n(save-resources! db-spec \"fhir_resources_database\" :specialized {:all [:text]} <resources>)
 (save-resources! db-spec \"fhir_resources_database\" :specialized {:patient [:defaults :name], :others [:defaults]} <resources>)"
  ([db-spec ^String table-prefix resources]
   ;;Validation
   (validate-db-spec db-spec)
   (when-not (sequential? resources)
     (throw (ex-info "The resources param must be a coll that implements the sequential interface."
                     {:type :argument-validation
                      :name :resources
                      :expected "non-empty list, vector or seq."
                      :got (-> resources type .getSimpleName)})))
   (validate-resources-basics resources)
   (validate-resources-schema resources)

   ;;Operation 
   (let [base-sentences [(mapper/create-table table-prefix)]
         insert-sentences (mapcat #(build-insert-sentences table-prefix [] %) resources)]
     (->> (concat base-sentences insert-sentences)
          (db/transact! db-spec)
          mapper/return-value-process)))

  ([db-spec ^String table-prefix mapping-type mapping-fields resources]
   ;; Basic validation
   (validate-db-spec db-spec)
   (when-not (sequential? resources)
     (throw (ex-info "The resources param must be a coll that implements the sequential interface."
                     {:type :argument-validation
                      :name :resources
                      :expected "non-empty list, vector or seq."
                      :got (-> resources type .getSimpleName)})))
   (validate-resources-basics resources)
   (validate-resources-schema resources)

   ;; Mapping type validation
   (when-not (keyword? mapping-type)
     (throw (ex-info "The mapping-type must be a keyword."
                     {:type :argument-validation
                      :name :mapping-type
                      :expected ":single or :specialized"
                      :got mapping-type})))
   ;;
   (case mapping-type
     :single
     (do
       (when-not (vector? mapping-fields)
         (throw (ex-info "Mapping-fields invalid. To do a single mapping, the mapping-fields param must be a vector."
                         {:type :argument-validation
                          :name :mapping-fields
                          :expected "non-empty vector"
                          :got (-> mapping-fields type .getSimpleName)})))
       (process-single-mapping db-spec table-prefix mapping-fields resources))
   
     :specialized
     (do
       (when-not (map? mapping-fields)
         (throw (ex-info "Mapping-fields invalid. To do a specialized mapping, the mapping-fields param must be a map."
                         {:type :argument-validation
                          :name :mapping-fields
                          :expected "non-empty map"
                          :got (-> mapping-fields type .getSimpleName)})))
       (process-specialized-mapping db-spec table-prefix mapping-fields resources))
   
     ;; Invalid mapping type
     (throw (ex-info "The mapping-type given is invalid. Use :single or :specialized."
                     {:type :argument-validation
                      :name :mapping-type
                      :expected ":single or :specialized"
                      :got mapping-type})))))

(defn- build-search-query
  "Builds a search query for resources."
  [table-prefix restype conditions]
  (let [table (keyword (str table-prefix "_" (.toLowerCase restype)))
        main (keyword (str table-prefix "_main"))
        all-cond (into [:and [:= (keyword "m.resourcetype") restype]] conditions)]
    {:table table
     :main main
     :conditions all-cond}))

(defn search-resources! "Return a coll of the resources found.\n This function takes four arguments:
 - `db-spec`: the database config where the search will happen.
 - `table-prefix`: the prefix used for the tables you're working with.
 - `restype`: the type of resource you're looking for.
 - `conditions`: a vector of vectors, each one representing a condition that the resource has to meet to be returned."
  [db-spec ^String table-prefix ^String restype conditions]
  ;; Validation
  (validate-db-spec db-spec)
  (when-not (vector? conditions)
    (throw (ex-info "conditions must be a vector"
                    {:type :argument-validation
                     :name :conditions
                     :expected "non-empty vector"
                     :got (-> conditions type .getSimpleName)})))
  
  (let [{:keys [table main conditions]} (build-search-query table-prefix restype conditions)
        query (if (contains? (db/get-tables db-spec) table)
                (-> (help/select :content)
                    (help/from [main :m])
                    (help/join table [:= :resource-id :id])
                    (help/where conditions)
                    sql/format)
                (-> (help/select :content)
                    (help/from [main :m])
                    (help/where conditions)
                    sql/format))]
  
    ;; Execute and process results
    (->> (db/execute! db-spec query)
         (mapcat vals)
         (map mapper/parse-jsonb-obj))))

(defn delete-resources! "This function works just like `search-resources!`, except instead of returning a sequence of resources, it gives you a fully-realized result set from `next.jdbc` to confirm the operation went through."
  [db-spec ^String table-prefix ^String restype conditions]
  ;;Validation
  (validate-db-spec db-spec)
  (when-not (vector? conditions)
    (throw (ex-info "conditions must be a vector"
                    {:type :argument-validation
                     :name :conditions
                     :expected "non-empty vector"
                     :got (-> conditions type .getSimpleName)})))
  
  (when (seq (search-resources! db-spec table-prefix restype conditions))
    (let [table (keyword (str table-prefix "_main"))
          all-cond (into [:and [:= :resourcetype restype]] conditions)
          sentence (-> (help/delete-from table)
                       (help/where all-cond)
                       sql/format)]
      (db/execute! db-spec sentence))))

(defn update-resource! "Here’s what you need to pass in:
 - `db-spec`: your database config.
 - `table-prefix`: the table prefix you're working with.
 - `restype`: the type of resource you're updating.
 - `id`: the ID of the resource you want to update.
 - `new-content`: the full resource with the updated fields."
  [db-spec ^String table-prefix ^String restype ^String id new-content]
  ;;Validation
  (validate-db-spec db-spec)

  (when (or (not (:id new-content))
            (not (v/valid-resource? new-content)))
    (v/validate-resource new-content))

  (when (or (not= restype (:resourceType new-content))
            (not= id (:id new-content)))
    (throw (ex-info "The new-content arg must have the same :id and :resourceType as the resource to be updated"
                    {:type :argument-validation
                     :name :new-content
                     :expected "A resource with the same :id and :resourceType as the one to be updated"
                     :got (str "A resource with different "
                               (if (not= restype (:resourceType new-content))
                                 ":resourceType"
                                 ":id"))})))
  
  (when (seq (search-resources! db-spec table-prefix restype [[:= :resource_id id] [:= :resourceType restype]]))
    (let [main (keyword (str table-prefix "_main"))
          table (keyword (str table-prefix "_" (.toLowerCase restype)))
          columns (remove #{:resourcetype :id} (db/get-columns db-spec (name table)))
          base-sentence (-> (help/update main)
                             (help/set {:content (mapper/to-pg-obj "jsonb" new-content)})
                             (help/where [:= :resource_id id])
                             (help/returning :content)
                             sql/format)
          full-sentence (if (seq columns)
                          [base-sentence
                                (-> (help/update table)
                                    (help/set (reduce #(assoc %1 %2 (second (mapper/type-of (get new-content %2))))
                                                      {} columns))
                                    (help/where [:= :id id])
                                    sql/format)]
                          [base-sentence])]
      (mapper/return-value-process (db/transact! db-spec full-sentence)))))

