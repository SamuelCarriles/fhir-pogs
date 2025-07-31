(ns fhir-pogs.core
  (:require [fhir-pogs.mapper :as mapper]
            [fhir-pogs.db :as db]
            [fhir-pogs.validator :as v]
            [next.jdbc :as jdbc]
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
   (cond
     (not (and (contains? resource :id) (contains? resource :resourceType))) (throw (IllegalArgumentException. "The resource don't have the obligatory keys :id & :resourceType."))
     (not (map? db-spec)) (throw (IllegalArgumentException. "The db-spec parameter must be a map."))
     (not (map? resource)) (throw (IllegalArgumentException. "The resource parameter must be a map."))
     (not (v/valid? resource)) (throw (IllegalArgumentException. "Invalid resource.")))
   (let [restype (:resourceType resource)]
     (db/jdbc-execute! db-spec (mapper/create-table table-prefix))
     (map #(db/jdbc-execute! db-spec %) (mapper/insert-to-sentence (mapper/template table-prefix [] resource) restype))))
  ([db-spec ^String table-prefix mapping-fields resource]
   (cond
     (not (and (contains? resource :id) (contains? resource :resourceType))) (throw (IllegalArgumentException. "The resource don't have the obligatory keys :id & :resourceType."))
     (not (map? db-spec)) (throw (IllegalArgumentException. "The db-spec parameter must be a map."))
     (not (vector? mapping-fields)) (throw (IllegalArgumentException. "The mapping-fields parameter must be a vector."))
     (not (map? resource)) (throw (IllegalArgumentException. "The resource parameter must be a map."))
     (not (v/valid? resource)) (throw (IllegalArgumentException. "Invalid resource.")))
   (let [fields (mapper/fields-types mapping-fields resource)
         restype (:resourceType resource)]
     (db/jdbc-execute! db-spec (mapper/create-table table-prefix))
     (when (some #(get resource %) (keys fields)) (db/jdbc-execute! db-spec (mapper/create-table  table-prefix restype fields)))
     (map #(db/jdbc-execute! db-spec %) (mapper/insert-to-sentence (mapper/template table-prefix (keys fields) resource) restype)))))

(defn save-resources! "Works very similarly to `save-resource!`, with the difference that it handles multiple resources instead of just one. The resources can have two types of mapping:  
 \n- `:single`: all resources are of the same type, so they can be inserted into a single table. For this type, the mapping-fields parameter is a vector of fields in keyword format.  
 \n- `:specialized`: resources are of various types, so a separate table is created for each resource type. For this type, mapping-fields is a map where the keys are the resource types and the values are vectors containing the fields in keyword format. There are two reserved keywords: `:all` and `:others`. The first is used when specifying the fields to extract from all resources, and the second when specific resource types and their fields have been defined, and additional fields need to be specified for any other resource types. It is recommended to always include :others, but if omitted and a resource of an unspecified type is encountered, only the basic fields will be mapped in the main or controlling table. By basic fields we mean :id, :resourceType, and :content.  
 \nExample of a function call: \n```clojure \n(save-resources! db-spec \"fhir_reources\" :single [:defaults] <resources>) \n(save-resources! db-spec \"fhir_resources_database\" :specialized {:all [:text]} <resources>)"
  ([db-spec ^String table-prefix resources]
   (cond
     (not (map? db-spec)) (throw (IllegalArgumentException. "The db-spec parameter must be a map."))
     (map? resources) (throw (IllegalArgumentException. "The resources parameter must be a list or a vector."))
     (not-every? v/valid? resources) (throw (IllegalArgumentException. "Some resources are not valid.")))
   (map #(save-resource! db-spec table-prefix %) resources))
  ([db-spec ^String table-prefix mapping-type mapping-fields resources]
   (cond
     (not (map? db-spec)) (throw (IllegalArgumentException. "The db-spec parameter must be a map."))
     (not (keyword? mapping-type)) (throw (IllegalArgumentException. "The mapping-type parameter must be a keyword."))
     (map? resources) (throw (IllegalArgumentException. "The resources parameter must be a list or a vector."))
     (not-every? v/valid? resources) (throw (IllegalArgumentException. "Some resources are not valid."))
     (and (= :single mapping-type) (not (vector? mapping-fields))) (throw (IllegalArgumentException. "If you want a single mapping, the mapping-fields parameter must be a vector."))
     (and (= :specialized mapping-type) (not (map? mapping-fields))) (throw (IllegalArgumentException. "If you want a specialized mapping, the mapping-fields parameter must be a map.")))
   (cond
     (= :single mapping-type)
     (do (when (not-every? #(= (:resourceType (first resources)) (:resourceType %)) resources)
           (throw (IllegalArgumentException. (str "Not every resources are " (:resourceType (first resources)) ", you should use :specialized mapping type."))))
         (map #(save-resource! db-spec table-prefix [(mapper/fields-types mapping-fields resources)] %) resources))
     (= :specialized mapping-type)
     (do (db/jdbc-execute! db-spec (mapper/create-table table-prefix))
         (map (fn [r]
                (let [restype (:resourceType r)
                      restype-key (keyword (.toLowerCase restype))
                      fields (mapper/fields-types (if-let [f (:all mapping-fields)]
                                                    f (if-let [fi (restype-key mapping-fields)]
                                                        fi (if-let [fid (:others mapping-fields)]
                                                             fid []))) resources)]
                  (when (some #(get r %) (keys fields)) (db/jdbc-execute! db-spec (mapper/create-table  table-prefix restype fields)))
                  (map #(db/jdbc-execute! db-spec %) (mapper/insert-to-sentence (mapper/template table-prefix (keys fields) r) restype))))
              resources))
     :else
     (throw (IllegalArgumentException. (str "The mapping-type is incorrect. The type " mapping-type " doesn't exist."))))))

(defn search-resources! "Retorna una seq con los recursos encontrados."
  [db-spec ^String table-prefix ^String restype conditions]
  (cond
    (not (map? db-spec)) (throw (IllegalArgumentException. "db-spec must be a map."))
    (not (and (vector? conditions) (every? vector? conditions))) (throw (IllegalArgumentException. "conditions must be a vector of vectors.")))
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
  (when (seq (search-resources! db-spec table-prefix restype conditions))
    (let [table (keyword (str table-prefix "_main"))]
      (db/jdbc-execute! db-spec
                        (-> (help/delete-from table)
                            (help/where conditions)
                            sql/format)))))

(defn update-resource! [db-spec ^String table-prefix ^String restype ^String id new-content]
  (cond
    (or (not (:id new-content)) (not (v/valid? new-content))) (throw (IllegalArgumentException. "The resource update are invalid."))
    (or (not= restype (:resourceType new-content)) (not= id (:id new-content))) (throw (IllegalArgumentException. "The id and resource type of resource update must be equal to the original id and resource type.")))
  
  (when (seq (search-resources! db-spec table-prefix restype [[:= :resource_id id]]))
    (let [main (keyword (str table-prefix "_main"))
          table (keyword (str table-prefix "_" restype))
          datasource (jdbc/get-datasource db-spec)
          columns (remove #{:resourcetype :id} (db/get-columns-of! db-spec (.toLowerCase (name table))))
          base-sentence [(-> (help/update main)
                             (help/set {:content (mapper/to-pg-obj "jsonb" new-content)})
                             (help/where [:= :resource_id id]) sql/format)]
          full-sentence (if (seq columns)
                          (conj base-sentence
                                (-> (help/update table)
                                    (help/set (reduce #(assoc %1 %2 (second (mapper/type-of (get new-content %2))))
                                                      {} columns))
                                    (help/where [:= :id id])
                                    sql/format))
                          base-sentence)]
      (jdbc/with-transaction [tx datasource]
        (mapv #(jdbc/execute-one! tx %) full-sentence)))))

