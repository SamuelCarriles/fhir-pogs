(ns fhir-pogs.core
  (:require [fhir-pogs.mapper :as mapper]
            [fhir-pogs.db :as db]
            [fhir-pogs.validator :as v]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))

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
   ;;Validation block
   (cond
     (not (:id resource))
     (throw (ex-info "Invalid resource. The key :id is required."
                     {:type :resource-validation
                      :param :id
                      :value (:id resource)
                      :expected {:type :string
                                 :blank false}
                      :resource resource}))
     ;;
     (not (:resourceType resource))
     (throw (ex-info "Invalid resource. The key :resourceType is required."
                     {:type :resource-validation
                      :param :resourceType
                      :value (:resourceType resource)
                      :expected {:type :string
                                 :blank false}
                      :resource resource}))
     ;;
     (not (map? db-spec))
     (throw (ex-info "The db-spec must be a map."
                     {:type :argument-validation
                      :name :db-spec
                      :value db-spec
                      :expected {:type :map
                                 :empty false}}))
     ;;
     (not (map? resource))
     (throw (ex-info "The resource must be a map."
                     {:type :argument-validation
                      :name :resource
                      :value resource
                      :expected {:type :map
                                 :empty false}}))
     ;;
     (not (v/valid? resource))
     (throw (v/validate-resource resource)))

   ;;Operation block
   (let [restype (:resourceType resource)
         base [(mapper/create-table table-prefix)]
         sentence (into base (mapper/insert-to-sentence (mapper/template table-prefix [] resource) restype))]
     (mapper/return-value-process (db/jdbc-transaction! db-spec sentence))))

  ([db-spec ^String table-prefix mapping-fields resource]
   ;;Validation block
   (cond
     (not (:id resource))
     (throw (ex-info "Invalid resource. The key :id is required."
                     {:type :resource-validation
                      :param :id
                      :value (:id resource)
                      :expected {:type :string
                                 :blank false}
                      :resource resource}))
     ;;
     (not (:resourceType resource))
     (throw (ex-info "Invalid resource. The key :resourceType is required."
                     {:type :resource-validation
                      :param :resourceType
                      :value (:resourceType resource)
                      :expected {:type :string
                                 :blank false}
                      :resource resource}))
     ;;
     (not (map? db-spec))
     (throw (ex-info "The db-spec must be a map."
                     {:type :argument-validation
                      :name :db-spec
                      :value db-spec
                      :expected {:type :map
                                 :empty false}}))
     ;;
     (not (vector? mapping-fields))
     (throw (ex-info "The mapping-fields param must be a vector."
                     {:type :argument-validation
                      :name :mapping-fields
                      :value mapping-fields
                      :expected {:type :vector
                                 :empty false}}))
     ;;
     (empty? mapping-fields)
     (throw (ex-info "Mapping-fields param is empty."
                     {:type :argument-validation
                      :name :mapping-fields
                      :value mapping-fields
                      :expected {:type :vector
                                 :empty false}}))
     ;;
     (not (map? resource))
     (throw (ex-info "The resource must be a map."
                     {:type :argument-validation
                      :name :resource
                      :value resource
                      :expected {:type :map
                                 :empty false}}))
     ;;
     (not (v/valid? resource))
     (throw (v/validate-resource resource))
     ;;
     (let [table (str table-prefix "_" (.toLowerCase (:resourceType resource)))
           columns (db/get-columns-of! db-spec table)
           fields (reduce #(if (map? %2) (into %1 (keys %2)) (conj %1 %2)) [] (flatten (replace {:defaults [:meta :text]} mapping-fields)))]
       (when-not (empty? columns) (not-every? #(get columns %) fields)))
     (let [table (str table-prefix "_" (.toLowerCase (:resourceType resource)))
           columns (db/get-columns-of! db-spec table)]
       (throw (ex-info (str "The table " table " already exists and you can only map these fields " (vec (remove #{:id :resourcetype} columns)))
                       {:type :argument-validation
                        :name :mapping-fields
                        :value (->> mapping-fields (replace {:defaults [:meta :text]}) flatten vec)
                        :expected (vec (remove #{:id :resourcetype} columns))}))))

   ;;Operation block
   (let [fields (mapper/fields-types (remove #{:id :resourceType} mapping-fields) resource)
         restype (:resourceType resource)
         base [(mapper/create-table table-prefix)]
         sentence (if (some #(get resource %) (keys fields)) (conj base (mapper/create-table  table-prefix restype fields)) base)]
     (->> (mapper/insert-to-sentence (mapper/template table-prefix (keys fields) resource) restype)
          (into sentence)
          (db/jdbc-transaction! db-spec)
          mapper/return-value-process))))

(defn save-resources! "Works very similarly to `save-resource!`, with the difference that it handles multiple resources instead of just one. All resources are stored within a transaction. The resources can have two types of mapping:  
 \n- `:single`: all resources are of the same type, so they can be inserted into a single table. For this type, the mapping-fields parameter is a vector of fields in keyword format.  
 \n- `:specialized`: resources are of various types, so a separate table is created for each resource type. For this type, mapping-fields is a map where the keys are the resource types and the values are vectors containing the fields in keyword format. There are two reserved keywords: `:all` and `:others`. The first is used when specifying the fields to extract from all resources, and the second when specific resource types and their fields have been defined, and additional fields need to be specified for any other resource types. It is recommended to always include :others, but if omitted and a resource of an unspecified type is encountered, only the basic fields will be mapped in the main or controlling table. By basic fields we mean :id, :resourceType, and :content.  
 \nExample of a function call: \n```clojure \n(save-resources! db-spec \"fhir_reources\" :single [:defaults] <resources>) \n(save-resources! db-spec \"fhir_resources_database\" :specialized {:all [:text]} <resources>)
(save-resources! db-spec \"fhir_resources_database\" :specialized {:patient [:defaults :name], :others [:defaults]} <resources>)"
  ([db-spec ^String table-prefix resources]
   ;;Validation block
   (cond
     (not-every? :id resources)
     (throw (ex-info "Invalid resource. The key :id is required."
                     (let [resource (some #(when-not (:id %) %) resources)
                           id (:id resource)]
                       {:type :resource-validation
                        :param :id
                        :value id
                        :expected {:type :string
                                   :blank false}
                        :resource resource})))
     ;;
     (not-every? :resourceType resources)
     (throw (ex-info "Invalid resource. The key :resourceType is required."
                     (let [resource (some #(when-not (:resourceType %) %) resources)
                           rtype (:resourceType resource)]
                       {:type :resource-validation
                        :param :resourceType
                        :value rtype
                        :expected {:type :string
                                   :blank false}
                        :resource resource})))
     ;;
     (not (map? db-spec))
     (throw (ex-info "The db-spec must be a map."
                     {:type :argument-validation
                      :name :db-spec
                      :value db-spec
                      :expected {:type :map
                                 :empty false}}))
     ;;
     (map? resources)
     (throw (ex-info "The resources param must be a list or a vector."
                     {:type :argument-validation
                      :name :resources
                      :value resources
                      :expected {:type {:or [:vector :list]}
                                 :empty false}}))
     ;;
     (not-every? v/valid? resources)
     (throw (let [invalid-resource (some #(when-not (v/valid? %) %) resources)
                  invalid-id (:id invalid-resource)]
              (ex-info (str "The resource with id \"" invalid-id "\" is invalid.")
                       (assoc {:type :schema-validation} :errors (->> invalid-resource v/validate-resource ex-data :errors))))))

   ;;Operation block 
   (->> (reduce (fn [o r]
                  (let [restype (:resourceType r)]
                    (into o (mapper/insert-to-sentence (mapper/template table-prefix [] r) restype))))
                [] resources)
        (into [(mapper/create-table table-prefix)])
        (db/jdbc-transaction! db-spec)
        mapper/return-value-process))

  ([db-spec ^String table-prefix mapping-type mapping-fields resources]
   ;;Validation block
   (cond
     (not-every? :id resources)
     (throw (ex-info "Invalid resource. The key :id is required."
                     (let [resource (some #(when-not (:id %) %) resources)
                           id (:id resource)]
                       {:type :resource-validation
                        :param :id
                        :value id
                        :expected {:type :string
                                   :blank false}
                        :resource resource})))
     ;;
     (not-every? :resourceType resources)
     (throw (ex-info "Invalid resource. The key :resourceType is required."
                     (let [resource (some #(when-not (:resourceType %) %) resources)
                           rtype (:resourceType resource)]
                       {:type :resource-validation
                        :param :resourceType
                        :value rtype
                        :expected {:type :string
                                   :blank false}
                        :resource resource})))
     ;;
     (not (map? db-spec))
     (throw (ex-info "The db-spec must be a map."
                     {:type :argument-validation
                      :name :db-spec
                      :value db-spec
                      :expected {:type :map
                                 :empty false}}))
     ;;
     (map? resources)
     (throw (ex-info "The resources param must be a list or a vector."
                     {:type :argument-validation
                      :name :resources
                      :value resources
                      :expected {:type {:or [:vector :list]}
                                 :empty false}}))
     ;;
     (not-every? v/valid? resources)
     (throw (let [invalid-resource (some #(when-not (v/valid? %) %) resources)
                  invalid-id (:id invalid-resource)]
              (ex-info (str "The resource with id \"" invalid-id "\" is invalid.")
                       (assoc {:type :schema-validation} :errors (->> invalid-resource v/validate-resource ex-data :errors)))))
     ;;  
     (not (keyword? mapping-type))
     (throw (ex-info "The mapping-type must be a keyword."
                     {:type :argument-validation
                      :name :mapping-type
                      :value mapping-type
                      :expected {:type :keyword
                                 :constraint {:or [{:value :single}
                                                   {:value :specialized}]}}}))
     ;;
     (and (= :single mapping-type) (map? mapping-fields))
     (throw (ex-info "To do a single mapping, the mapping-fields param must be a vector or list."
                     {:type :argument-validation
                      :name :mapping-fields
                      :value mapping-fields
                      :expected {:type {:or [:vector :list]}
                                 :empty false}}))
     ;;
     (and (= :specialized mapping-type) (not (map? mapping-fields)))
     (throw (ex-info "To do a specialized mapping, the mapping-fields param must be a map."
                     {:type :argument-validation
                      :name :mapping-fields
                      :value mapping-fields
                      :expected {:type :map
                                 :empty false
                                 :constraint {:structure {:key {:type :keyword}
                                                          :value {:type :vector
                                                                  :empty false}
                                                          :multiple true}}}}))
     ;;
     (and (= :specialized mapping-type) (some empty? (vals mapping-fields)))
     (throw (let [k (some #(when-not (->> % second seq) (first %)) mapping-fields)]
              (ex-info (str "Invalid mapping-fields. The key " k " is associated with an empty vector or list.")
                       {:type :argument-validation
                        :name :mapping-fields
                        :value mapping-fields
                        :expected {:type :map
                                   :empty false
                                   :constraint {:structure {:key {:type :keyword}
                                                            :value {:type {:or [:vector :list]}
                                                                    :empty false}
                                                            :multiple true}}}}))))
   ;;
   (cond
     (= :single mapping-type)
     (do
       ;;This block is to validate the mapping-fields vector
       (let [table (->> (first resources) :resourceType .toLowerCase (str table-prefix "_"))
             columns (db/get-columns-of! db-spec table)
             fields (reduce #(if (map? %2) (into %1 (keys %2)) (conj %1 %2)) [] (flatten (replace {:defaults [:meta :text]} mapping-fields)))]
         (when (and (seq columns) (not-every? #(contains? columns %) fields))
           (throw (ex-info (str "The table " table " already exists and you can only map these fields " (vec (remove #{:id :resourcetype} columns)))
                           {:type :argument-validation
                            :name :mapping-fields
                            :value (vec fields)
                            :expected (vec (remove #{:id :resourcetype} columns))}))))
       ;;
       (when (not-every? #(= (:resourceType (first resources)) (:resourceType %)) resources)
         (let [restype (->> resources first :resourceType)
               res (some #(when (not (= restype (:resourceType %))) %) (rest resources))]
           (throw (ex-info (str "Invalid resources given to :single mapping-type. Resource with id \"" (:id res) "\" don't have \"" restype "\" resourceType.")
                           {:type :argument-validation
                            :name :resources
                            :value resources
                            :expected {:type {:or [:vector :list]}
                                       :empty false
                                       :constraint {:structure {:elements {:type :map}
                                                                :multiple true
                                                                :constraint {:same :resourceType}}}}}))))

       ;;This block is to store all resources within a transaction 
       (->> (reduce (fn [o r]
                      (let [restype (:resourceType r)
                            fields (mapper/fields-types (remove #{:id :resourceType} mapping-fields) resources)
                            base (if (some #(get r %) (keys fields)) [(mapper/create-table  table-prefix restype fields)] [])
                            sentences (into base (mapper/insert-to-sentence (mapper/template table-prefix (keys fields) r) restype))]
                        (into o sentences)))
                    [] resources)
            (into [(mapper/create-table table-prefix)])
            (db/jdbc-transaction! db-spec)
            mapper/return-value-process))
     (= :specialized mapping-type)

     (do
       ;;Validation block
       (if-let [f (:all mapping-fields)]
         (->> (db/get-tables! db-spec)
              (remove #(or (not (re-find (re-pattern (str table-prefix ".*")) (name %)))
                           (re-find #"_main$" (name %))))
              (reduce #(assoc %1 %2 (db/get-columns-of! db-spec %2)) {})

              (some (fn [[t columns]]
                      (let [fields (reduce #(if (map? %2) (into %1 (keys %2)) (conj %1 %2)) [] f)
                            valid-fields (vec (remove #{:id :resourcetype} columns))]
                        (when (and (seq columns) (not-every? #(contains? columns %) (flatten (replace {:defaults [:meta :text]} fields))))
                          (throw (ex-info (str "The table " (name t) " already exists and you can only map these fields " valid-fields)
                                          {:type :argument-validation
                                           :name :mapping-fields
                                           :value (->> fields (replace {:defaults [:meta :text]}) flatten vec)
                                           :expected valid-fields})))))))

         (->> (if-not (:others mapping-fields)
                (reduce-kv #(assoc %1 (->> %2 name .toLowerCase (str table-prefix "_") keyword) %3) {} mapping-fields)
                (let [tables-to-remove (->> (dissoc mapping-fields :others) keys (map #(->> % name (str table-prefix "_") keyword)) set)
                      all-tables (->> (db/get-tables! db-spec)
                                      (remove #(or (not (re-find (re-pattern (str table-prefix ".*")) (name %)))
                                                   (re-find #"_main$" (name %)))))
                      other-tables (remove tables-to-remove all-tables)
                      other-fields (:others mapping-fields)
                      new-mf-base (->> (dissoc mapping-fields :others)
                                       (reduce-kv #(assoc %1 (->> %2 name .toLowerCase (str table-prefix "_") keyword) %3) {}))]

                  (->> other-tables
                       (reduce #(assoc %1 %2 other-fields) new-mf-base))))
              (some (fn [[k v]]
                      (let [columns (db/get-columns-of! db-spec k)
                            fields (reduce #(if (map? %2) (into %1 (keys %2)) (conj %1 %2)) [] v)
                            valid-fields (vec (remove #{:id :resourcetype} columns))]
                        (when (and (seq columns) (not-every? #(contains? columns %) (flatten (replace {:defaults [:meta :text]} fields))))
                          (throw (ex-info (str "The table " (name k) " already exists and you can only map these fields " valid-fields)
                                          {:type :argument-validation
                                           :name :mapping-fields
                                           :value (->> fields (replace {:defaults [:meta :text]}) flatten vec)
                                           :expected valid-fields}))))))))

       ;;This block is to store all resources within a transaction 
       (->> (reduce (fn [o r]
                      (let [restype (:resourceType r)
                            restype-key (keyword (.toLowerCase restype))
                            fields (mapper/fields-types (->> (if-let [f (:all mapping-fields)]
                                                               f (if-let [fi (restype-key mapping-fields)]
                                                                   fi (if-let [fid (:others mapping-fields)]
                                                                        fid [])))
                                                             (remove #{:id :resourceType})) resources)
                            base (if (some #(get r %) (keys fields)) [(mapper/create-table  table-prefix restype fields)] [])
                            sentences (into base (mapper/insert-to-sentence (mapper/template table-prefix (keys fields) r) restype))]
                        (into o sentences)))
                    []
                    resources)
            (into [(mapper/create-table table-prefix)])
            (db/jdbc-transaction! db-spec)
            mapper/return-value-process))
     :else
     (throw (ex-info "The mapping-type given is invalid. Use :single or :specialized."
                     {:type :argument-validation
                      :name :mapping-type
                      :value mapping-type
                      :expected {:type :keyword
                                 :constraint {:or [{:value :single}
                                                   {:value :specialized}]}}})))))

(defn search-resources! "Retorna una seq con los recursos encontrados."
  [db-spec ^String table-prefix ^String restype conditions]
  (cond
    (not (map? db-spec)) (throw (IllegalArgumentException. "db-spec must be a map."))
    (not (vector? conditions)) (throw (IllegalArgumentException. "conditions must be a vector.")))
  (let [table (keyword (str table-prefix "_" (.toLowerCase restype)))
        main (keyword (str table-prefix "_main"))
        all-cond (into [:and [:= (keyword "m.resourcetype") restype]] conditions)]
    (if (contains? (db/get-tables! db-spec) table)
      (->>
       (db/jdbc-execute! db-spec
                         (-> (help/select :content)
                             (help/from [main :m])
                             (help/join table [:= :resource-id :id])
                             (help/where all-cond)
                             sql/format))
       (mapcat vals)
       (map mapper/parse-jsonb-obj))
      (->>
       (db/jdbc-execute! db-spec
                         (-> (help/select :content)
                             (help/from [main :m])
                             (help/where all-cond)
                             sql/format))
       (mapcat vals)
       (map mapper/parse-jsonb-obj)))))

(defn delete-resources! [db-spec ^String table-prefix ^String restype conditions]
  (cond
    (not (map? db-spec)) (throw (IllegalArgumentException. "The db-spec parameter must be a map."))
    (not (vector? conditions)) (throw (IllegalArgumentException. "conditions must be a vector.")))
  (when (seq (search-resources! db-spec table-prefix restype conditions))
    (let [table (keyword (str table-prefix "_main"))
          sentence (-> (help/delete-from table)
                       (help/where conditions)
                       sql/format)]
      (db/jdbc-execute! db-spec sentence))))

(defn update-resource! [db-spec ^String table-prefix ^String restype ^String id new-content]
  (cond
    (not (map? db-spec)) (throw (IllegalArgumentException. "The db-spec parameter must be a map."))
    (or (not (:id new-content)) (not (v/valid? new-content))) (throw (IllegalArgumentException. "The resource update are invalid."))
    (or (not= restype (:resourceType new-content)) (not= id (:id new-content))) (throw (IllegalArgumentException. "The id and resource type of resource update must be equal to the original id and resource type.")))
  (when (seq (search-resources! db-spec table-prefix restype [[:= :resource_id id] [:= :resourceType restype]]))
    (let [main (keyword (str table-prefix "_main"))
          table (keyword (str table-prefix "_" (.toLowerCase restype)))
          columns (remove #{:resourcetype :id} (db/get-columns-of! db-spec (name table)))
          base-sentence [(-> (help/update main)
                             (help/set {:content (mapper/to-pg-obj "jsonb" new-content)})
                             (help/where [:= :resource_id id])
                             (help/returning :content)
                             sql/format)]
          full-sentence (if (seq columns)
                          (conj base-sentence
                                (-> (help/update table)
                                    (help/set (reduce #(assoc %1 %2 (second (mapper/type-of (get new-content %2))))
                                                      {} columns))
                                    (help/where [:= :id id])
                                    sql/format))
                          base-sentence)]
      (mapper/return-value-process (db/jdbc-transaction! db-spec full-sentence)))))

