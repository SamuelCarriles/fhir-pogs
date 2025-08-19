(ns fhir-pogs.mapper
  (:require [cheshire.core :refer [generate-string parse-string]]
            [honey.sql :as sql]
            [honey.sql.helpers :as help])
  (:import [org.postgresql.util PGobject]))

(defn parse-resource "Parse a json resource to a clojure map."
  [json]
  (parse-string json true))

(defn parse-jsonb-obj [obj]
  (->> obj .getValue (parse-resource)))

(defn to-pg-obj "Create a PGobject with the given type and value."
  [^String type value]
  (when (some nil? [type value])
    (throw (IllegalArgumentException. "Some argument it's nil.")))
  (doto (PGobject.)
    (.setType type)
    (.setValue (if (coll? value) (generate-string value) value))))

(defn type-of "Returns a vector containing, as its first element, the keyword associated with the PostgreSQL data type, and as its second element, the value to be stored in the PostgreSQL field."
  [value]
  (when (nil? value) (throw (IllegalArgumentException. "Argument it's nil.")))
  (let [v (.toString value)]
    (cond
      (coll? value) [:jsonb (to-pg-obj "jsonb" value)]

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

(defn create-table "Returns an SQL statement to create a table with the specified fields. If the function is called with a single argument, it will create the main table where general data for each resource will be stored.\n - `table-prefix`: prefix to the name of the table.\n - `restype`: type of resource to be stored.\n - `fields`: a map containing the fields to be extracted and the data type they store.\nExample of usage:\n ```clojure \n(create-table \"fhir_resources\")\n;; => [\"CREATE TABLE IF NOT EXISTS fhir_resources_main (resource_id TEXT PRIMARY KEY NOT NULL, resourceType TEXT NOT NULL, content JSONB NOT NULL)\"] \n(create-table \"fhir_resources\" \"Patient\" {:meta :jsonb :text :jsonb})\n;; => [\"CREATE TABLE IF NOT EXISTS fhir_resources_Patient (id TEXT PRIMARY KEY NOT NULL, resourceType TEXT NOT NULL, content JSONB NOT NULL, meta JSONB, text JSONB)\"]"
  ([^String table-prefix]
   (-> (help/create-table (keyword (str table-prefix "_main")) :if-not-exists)
       (help/with-columns [[:resource-id :text [:not nil]]
                           [:resourceType :text [:not nil]]
                           [:content :jsonb [:not nil]]
                           [[:primary-key :resource-id :resourceType]]])
       sql/format))

  ([^String table-prefix ^String restype fields]
   (let [columns (conj
                  (into [[:id :text [:not nil]]
                         [:resourceType :text [:not nil] [:default restype]]] fields)
                  [[:primary-key :id :resourceType]]
                  [[:foreign-key :id :resourceType] [:references (keyword (str table-prefix "_main")) :resource-id :resourceType] :on-delete-cascade])]
     (-> (help/create-table (keyword (str table-prefix "_" restype)) :if-not-exists)
         (help/with-columns columns)
         sql/format))))
(create-table "fhir" "Patient" {:meta :jsonb})
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
                          :value (to-pg-obj "jsonb" resource)}))))))

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
                         result (assoc-in o [t (if (= name :id) :resource-id name)] value)]
                     (if (and (> (count fields) 3) (= :id name))
                       (assoc-in result [(keyword (str table "_" restype)) :id] value)
                       result)))
                 {} fields)
         (map (fn [[n v]]
                (let [sentence (-> (help/insert-into n)
                                   (help/values [v]))]
                  (if (= (str table "_main") (name n))
                    (-> sentence
                        (help/returning :content)
                        sql/format)
                    (-> sentence sql/format)))))
         (sort-by (fn [x] (not (re-find #"_main" (first x))))))))

(defn fields-types "Returns a map where each key is a field and its associated value is the data type of that field.\n- `f`: a vector with the fields whose data types are to be determined. Within this vector, there may be maps for fields that do not appear in the resource but still need to be created in the table, where each key is a field and the value is the data type. Both key and value are keywords.\n- `r`: the FHIR resource converted into a Clojure map."
  ([f r]
   (let [final (apply merge (filter map? f))
         fields (remove #(or (map? %) (contains? #{:id :resourceType} %)) f)]
     (merge final (reduce (fn [x y]
                            (if-let [value (if (or (seq? r) (vector? r)) (some (fn [x] (when (contains? x y) (get x y))) r)
                                               (get r y))]
                              (assoc x y (first (type-of value)))
                              x))
                          {} (set (if (some #(= % :defaults) fields)
                                    (conj (remove #(= % :defaults) fields)
                                          :meta :text)
                                    fields)))))))

(defn return-value-process [value]
  (let [value (->> (flatten value)
                   (remove (fn [m] (some #(= "next.jdbc" (namespace %)) (keys m)))))
        ready-value (reduce #(->>
                              (map (fn [[_ v]] v) %2)
                              (into %1))
                            [] value)]
    (mapv parse-jsonb-obj ready-value)))
