(ns fhir-pogs.mapper
  (:require [cheshire.core :refer [generate-string parse-string]]
            [honey.sql :as sql]
            [honey.sql.helpers :as help]
            [fhir-pogs.db :refer [jdbc-execute!]])
  (:import [org.postgresql.util PGobject]))

(defn parse-resource "Parse a json resource to a clojure map."
  [json]
  (parse-string json true))

(defn to-pg-obj "Create a PGobject with the given type and value."
  [^String type value]
  (when (some nil? [type value])
    (throw (IllegalArgumentException. "Some argument it's nil.")))
  (doto (PGobject.)
    (.setType type)
    (.setValue value)))

(defn type-of "Returns a vector containing, as its first element, the keyword associated with the PostgreSQL data type, and as its second element, the value to be stored in the PostgreSQL field."
  [value]
  (when (nil? value) (throw (IllegalArgumentException. "Argument it's nil.")))
  (let [v (.toString value)]
    (cond
      (coll? value) [:jsonb (to-pg-obj "jsonb" (generate-string v))]

      (re-matches #"true|false" v)
      [:boolean (Boolean/parseBoolean v)]

      (re-matches #"([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1]))?)?" v)
      [:date (to-pg-obj "date" v)]

      (or (re-matches #"([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]{1,9})?" v)
          (re-matches #"([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]{1,9})?(Z|(\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00))" v)
          (re-matches #"([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1])(T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]{1,9})?)?)?(Z|(\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00)?)?)?" v))
      [:timestamptz (to-pg-obj "timestamptz" v)]

      (re-matches #"-?(0|[1-9][0-9]{0,17})(\.[0-9]{1,17})?([eE][+-]?[0-9]{1,9}})?" v)
      [:numeric (BigDecimal. v)]

      (or (re-matches #"[1-9][0-9]*" v)
          (re-matches #"[0]|[-+]?[1-9][0-9]*" v)
          (re-matches #"[0]|([1-9][0-9]*)" v))
      [:integer (Integer/parseInt v)]

      (re-matches #"urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" v)
      [:uuid (to-pg-obj "uuid" v)]

      (or (re-matches #"\S*" v)
          (re-matches #"[^\s]+( [^\s]+)*" v)
          (re-matches #"[A-Za-z0-9\-\.]{1,64}" v)
          (re-matches #"^[\s\S]+$" v)
          (re-matches #"urn:oid:[0-2](\.(0|[1-9][0-9]*))+" v)
          (re-matches #"" v))
      [:text v]

      (re-matches #"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?" v)
      [:bytea (to-pg-obj "bytea" v)])))

(defn create-table "Returns an SQL statement to create a table with the specified fields. If the function is called with a single argument, it will create the main table where general data for each resource will be stored.\n - `table-name`: base name of the table.\n - `restype`: type of resource to be stored.\n - `fields`: a map containing the fields to be extracted and the data type they store.\nExample of usage:\n ```clojure \n(create-table \"fhir_resources\")\n;; => [\"CREATE TABLE IF NOT EXISTS fhir_resources_main (resource_id TEXT PRIMARY KEY NOT NULL, resourceType TEXT NOT NULL, content JSONB NOT NULL)\"] \n(create-table \"fhir_resources\" \"Patient\" {:meta :jsonb :text :jsonb})\n;; => [\"CREATE TABLE IF NOT EXISTS fhir_resources_Patient (id TEXT PRIMARY KEY NOT NULL, resourceType TEXT NOT NULL, content JSONB NOT NULL, meta JSONB, text JSONB)\"]"
  ([^String table-name]
   (-> (help/create-table (keyword (str table-name "_main")) :if-not-exists)
       (help/with-columns [[:resource-id :text :primary-key :not-null]
                           [:resourceType :text :not-null]
                           [:content :jsonb :not-null]])
       sql/format))

  ([^String table-name ^String restype fields]
   (let [columns (into
                  [[:id :text :primary-key :not-null [:references (keyword (str table-name "_main")) :resource-id] :on-delete-cascade]]
                  fields)]
     (-> (help/create-table (keyword (str table-name "_" restype)) :if-not-exists)
         (help/with-columns columns)
         sql/format))))

(defn template "Creates a map that will serve as a template for inserting into the database the fields to be mapped from the FHIR resource.\n- `table-name`: the name of the table where the fields will be inserted.\n- `fields`: a vector containing the fields to be mapped into the database.\n- `resource`: the FHIR resource converted into a Clojure map."
  [^String table-name fields resource]
  (let [f (set (conj fields :id :resourceType))]
    (-> {:table table-name}
        (assoc :fields
               (-> (remove nil?
                           (mapv
                            (fn [[n v]]
                              (if (= n :id)
                                {:name n :value v}
                                (let [type (type-of v)
                                      value (second type)]
                                  {:name n
                                   :value value})))
                            (reduce (fn [o entry]
                                      (if (contains? f (key entry))
                                        (conj o entry) o))
                                    {} resource)))
                   (vec)
                   (conj {:name :content
                          :value (to-pg-obj "jsonb" (generate-string resource))}))))))

(defn insert-to-sentence "Generates the necessary SQL statements to insert all the data from template into its corresponding table.\n- `template`: a map resulting from applying the `template` function."
  [template ^String restype]
  (let [table (:table template)
        fields (:fields template)]
    (->> (reduce (fn [o {:keys [name value]}]
                   (let [t (keyword (if (or (= name :id)
                                            (= name :resourceType)
                                            (= name :content))
                                      (str table "_main")
                                      (str table "_" restype)))
                         n (if (and (= t (keyword (str table "_main")))
                                    (= name :id))
                             :resource-id name)
                         result (assoc-in o [t n] value)]
                     (if (and (> (count fields) 3) (= :id name))
                       (assoc-in result [(keyword (str table "_" restype)) :id] value)
                       result)))
                 {} fields)
         (map (fn [[n v]] (-> (help/insert-into n)
                              (help/values [v])
                              (sql/format))))
         (sort-by (fn [x] (not (re-find #"_main" (first x))))))))

(defn fields-types "Returns a map where each key is a field and its associated value is the data type of that field.\n- `f`: a vector with the fields whose data types are to be determined. Within this vector, there may be maps for fields that do not appear in the resource but still need to be created in the table, where each key is a field and the value is the data type. Both key and value are keywords.\n- `r`: the FHIR resource converted into a Clojure map."
  ([f r]
   (let [final (apply merge (filter map? f))
         fields (remove map? f)]
     (merge final (reduce (fn [x y]
                            (if-let [value (if (or (seq? r) (vector? r)) (some (fn [x] (when (contains? x y) (get x y))) r)
                                               (get r y))]
                              (assoc x y (first (type-of value)))
                              x))
                          {} (set (if (some #(= % :defaults) fields)
                                    (conj (remove #(= % :defaults) fields)
                                          :meta :text)
                                    fields)))))))

(defn map-resource "Maps a FHIR resource into a database.  
 \n- `db-spec`: the database specifications. For example: \n{:dbtype \"postgresql\", \n:dbname \"resources\", \n:host \"localhost\", \n:user \"postgres\", \n:port \"5432\", \n:password \"postgres\"}  
 \n- `table-name`: the name of the table where the resource should be mapped.  
 \n- `mapping-fields`: a vector containing the names of the fields from the resource to be mapped. The names are given in keyword format. Note that the resource must contain every field that is intended to be mapped. If a field is to be added to the table that is not present in the resource—perhaps for future use—it can be written within the vector as a map, where the key is the name of the field and the value is the data type that field will store. Some examples: `[:meta :text :active :deceased]`, `[:defaults {:some-field :type-of-field}]`  
 \n- resource: the FHIR resource converted into a Clojure map.  
 \nExample of usage:\n ```clojure\n (def test-1 (parse-string <json resource> true))
  (def test-2 (parse-string <json resource> true))
  (def test-3 (parse-string <json resource> true))

  (def db-spec {:dbtype \"postgresql\"
                :dbname \"resources\"
                :host \"localhost\"
                :user \"postgres\"
                :port \"5432\"
                :password \"postgres\"})

  (map #(map-resource db-spec \"fhir_resources\" [:defaults :active] %) [test-1 test-2 test-3])"

  [db-spec ^String table-name mapping-fields resource]
  (cond
    (not (and (contains? resource :id) (contains? resource :resourceType))) (throw (IllegalArgumentException. "The resource don't have the obligatory keys :id & :resourceType."))
    (not (map? db-spec)) (throw (IllegalArgumentException. "The db-spec parameter must be a map."))
    (not (vector? mapping-fields)) (throw (IllegalArgumentException. "The mapping-fields parameter must be a vector."))
    (not (map? resource)) (throw (IllegalArgumentException. "The resource parameter must be a map.")))
  (let [fields (fields-types mapping-fields resource)
        restype (:resourceType resource)]
    (jdbc-execute! db-spec (create-table table-name))
    (jdbc-execute! db-spec (create-table  table-name restype fields))
    (map #(jdbc-execute! db-spec %) (insert-to-sentence (template table-name (keys fields) resource) restype))))

(defn map-resources "Works very similarly to map-resource, with the difference that it handles multiple resources instead of just one. The resources can have two types of mapping:  
 \n- `:single`: all resources are of the same type, so they can be inserted into a single table. For this type, the mapping-fields parameter is a vector of fields in keyword format.  
 \n- `:specialized`: resources are of various types, so a separate table is created for each resource type. For this type, mapping-fields is a map where the keys are the resource types and the values are vectors containing the fields in keyword format. There are two reserved keywords: `:all` and `:others`. The first is used when specifying the fields to extract from all resources, and the second when specific resource types and their fields have been defined, and additional fields need to be specified for any other resource types. It is recommended to always include :others, but if omitted and a resource of an unspecified type is encountered, only the basic fields will be mapped in the main or controlling table. By basic fields we mean :id, :resourceType, and :content.  
 \nExample of a function call: \n```clojure \n(map-resources db-spec \"fhir_reources\" :single [:defaults] <resources>) \n(map-resources db-spec \"fhir_resources_database\" :specialized {:all [:text]} <resources>)"

  [db-spec ^String table-name mapping-type mapping-fields resources]
  (cond
    (not-every? #(and (contains? % :id) (contains? % :resourceType)) resources) (throw (IllegalArgumentException. "Some resource don't have the obligatory keys :id & :resourceType"))
    (not (map? db-spec)) (throw (IllegalArgumentException. "The db-spec parameter must be a map."))
    (not (keyword? mapping-type)) (throw (IllegalArgumentException. "The mapping-type parameter must be a keyword."))
    (map? resources) (throw (IllegalArgumentException. "The resources parameter must be a list or a vector."))
    (and (= :single mapping-type) (not (vector? mapping-fields))) (throw (IllegalArgumentException. "If you want a single mapping, the mapping-fields parameter must be a vector."))
    (and (= :specialized mapping-type) (not (map? mapping-fields))) (throw (IllegalArgumentException. "If you want a specialized mapping, the mapping-fields parameter must be a map.")))
  (cond
    (= :single mapping-type)
    (do (when (not-every? #(= (:resourceType (first resources)) (:resourceType %)) resources)
          (throw (IllegalArgumentException. (str "Not every resources are " (:resourceType (first resources)) ", you should use :specialized mapping type."))))
        (map #(map-resource db-spec table-name [(fields-types mapping-fields resources)] %) resources))
    (= :specialized mapping-type)
    (do (jdbc-execute! db-spec (create-table table-name))
        (map (fn [r]
               (let [restype (:resourceType r)
                     restype-key (keyword (.toLowerCase restype))
                     fields (fields-types (if-let [f (:all mapping-fields)]
                                            f (if-let [fi (restype-key mapping-fields)]
                                                fi (if-let [fid (:others mapping-fields)]
                                                     fid []))) resources)]
                 (jdbc-execute! db-spec (create-table  table-name restype fields))
                 (map #(jdbc-execute! db-spec %) (insert-to-sentence (template table-name (keys fields) r) restype))))
             resources))
    :else
    (throw (IllegalArgumentException. (str "The mapping-type is incorrect. The type " mapping-type " doesn't exist.")))))
