(ns fhir-pogs.mapper
  (:require [cheshire.core :refer [generate-string parse-string]]
            [honey.sql :as sql]
            [honey.sql.helpers :as help]
            [clojure.string :as str])
  (:import [org.postgresql.util PGobject]))

(def ^:private regex-patterns
  {:boolean #"true|false"
   :date #"([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1]))?)?"
   :time #"([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]{1,9})?"
   :datetime-full #"([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]{1,9})?(Z|(\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00))"
   :datetime-partial #"([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1])(T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]{1,9})?)?)?(Z|(\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00)?)?)?"
   :numeric #"-?(0|[1-9][0-9]{0,17})(\.[0-9]{1,17})?([eE][+-]?[0-9]{1,9}})?"
   :positive-integer #"[1-9][0-9]{0,9}"
   :integer #"[0]|[-+]?[1-9][0-9]{0,9}"
   :non-negative-integer #"[0]|([1-9][0-9]{0,9})"
   :uuid #"urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
   :oid #"urn:oid:[0-2](\.(0|[1-9][0-9]*))+"
   :base64 #"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?"})

;; Helper functions
(defn parse-resource
  "Parse a json resource to a clojure map."
  [json]
  {:pre [(string? json)]}
  (try
    (parse-string json true)
    (catch Exception e
      (throw (ex-info "Failed to parse JSON" {:json json :error (.getMessage e)})))))

(defn parse-jsonb-obj [obj]
  (when obj
    (->> obj .getValue parse-resource)))

(defn to-pg-obj
  "Create a PGobject with the given type and value."
  [^String type value]
  {:pre [(string? type) (not (str/blank? type)) (some? value)]}
  (doto (PGobject.)
    (.setType type)
    (.setValue (if (coll? value) (generate-string value) (str value)))))

;;


(defn type-of
  "Returns a vector containing the PostgreSQL data type keyword and processed value.
     Throws ex-info with descriptive message on nil input."
  [value]
  (when (nil? value)
    (throw (ex-info "Cannot determine type of nil value" {:value value})))

  (let [v (str value)]
    (cond
      (coll? value)
      [:jsonb (to-pg-obj "jsonb" value)]

      (re-matches (:boolean regex-patterns) v)
      [:boolean (Boolean/parseBoolean v)]

      (re-matches (:date regex-patterns) v)
      [:date (to-pg-obj "date" v)]

      (or (re-matches (:time regex-patterns) v)
          (re-matches (:datetime-full regex-patterns) v)
          (re-matches (:datetime-partial regex-patterns) v))
      [:timestamptz (to-pg-obj "timestamptz" v)]

      (or (re-matches (:positive-integer regex-patterns) v)
          (re-matches (:integer regex-patterns) v)
          (re-matches (:non-negative-integer regex-patterns) v))
      (try
        [:integer (Integer/parseInt v)]
        (catch NumberFormatException _
          (try
            [:bigint (Long/parseLong v)]
            (catch NumberFormatException _
              [:numeric (BigDecimal. v)]))))

      (re-matches (:numeric regex-patterns) v)
      [:numeric (BigDecimal. v)] 

      (re-matches (:uuid regex-patterns) v)
      [:uuid (to-pg-obj "uuid" v)] 

      :else [:text v])))

(defn create-table
  "Returns an SQL statement to create a table with the specified fields."
  ([^String table-prefix]
   (-> (help/create-table (keyword (str table-prefix "_main")) :if-not-exists)
       (help/with-columns [[:resource_id :text [:not nil]]
                           [:resourceType :text [:not nil]]
                           [:content :jsonb [:not nil]]
                           [[:primary-key :resource_id :resourceType]]])
       (sql/format {:quoted true})))

  ([^String table-prefix ^String restype fields]
   (let [base-columns [[:id :text [:not nil]]
                       [:resourceType :text [:not nil] [:default restype]]]
         field-columns (mapv (fn [[field type]] [field type]) fields)
         constraints [[[:primary-key :id :resourceType]]
                      [[:foreign-key :id :resourceType]
                       [:references (keyword (str table-prefix "_main")) :resource_id :resourceType]
                       :on-delete-cascade]]
         all-columns (concat base-columns field-columns constraints)]
     (-> (help/create-table (keyword (str table-prefix "_" (.toLowerCase restype))) :if-not-exists)
         (help/with-columns all-columns)
         (sql/format {:quoted true})))))



(defn template
  "Creates a map template for database insertion from FHIR resource."
  [^String table-prefix fields resource]
  (let [required-fields (conj (set fields) :id :resourceType)
        processed-fields (->> resource
                              (filter #(contains? required-fields (key %)))
                              (map (fn [[field-name value]]
                                     (if (= field-name :id)
                                       {:name field-name :value value}
                                       (let [[_ processed-value] (type-of value)]
                                         {:name field-name :value processed-value}))))
                              (remove nil?)
                              vec)]
    {:table table-prefix
     :fields (conj processed-fields
                   {:name :content
                    :value (to-pg-obj "jsonb" resource)})}))

(defn insert-to-sentence
  "Generates SQL statements for database insertion from template."
  [template ^String restype]
  (let [{:keys [table fields]} template
        main-table (keyword (str table "_main"))
        resource-table (keyword (str table "_" (.toLowerCase restype)))

        ;; Separate fields by destination table
        {main-fields true resource-fields false}
        (group-by #(contains? #{:id :resourceType :content} (:name %)) fields)

        ;; Process main table fields
        main-data (reduce (fn [acc {:keys [name value]}]
                            (assoc acc (if (= name :id) :resource_id name) value))
                          {} main-fields)

        ;; Process resource table fields (if any non-main fields exist)
        resource-data (when (> (count fields) 3) ; More than id, resourceType, content
                        (reduce (fn [acc {:keys [name value]}]
                                  (assoc acc name value))
                                {:id (:resource_id main-data)
                                 :resourceType (:resourceType main-data)} ; Include id reference
                                resource-fields))

        ;; Generate SQL statements
        statements (cond-> []
                     (seq main-data)
                     (conj (-> (help/insert-into main-table)
                               (help/values [main-data])
                               (help/returning :content)
                               (sql/format {:quoted true})))

                     (seq resource-data)
                     (conj (-> (help/insert-into resource-table)
                               (help/values [resource-data])
                               (sql/format {:quoted true}))))] 
    statements))

;; Improved fields-types function
(defn fields-types
  "Returns a map of field names to PostgreSQL data types."
  [fields resource]
  (let [explicit-types (apply merge (filter map? fields))
        field-names (remove #(or (map? %) (contains? #{:id :resourceType} %)) fields)

        ;; Add defaults if requested
        final-fields (if (some #(= % :defaults) field-names)
                       (-> field-names
                           (as-> f (remove #(= % :defaults) f))
                           (conj :meta :text))
                       field-names)

        ;; Infer types from resource data
        inferred-types (reduce (fn [acc field]
                                 (if-some [value (get resource field)]
                                   (assoc acc field (first (type-of value)))
                                   acc))
                               {} final-fields)]
    (merge explicit-types inferred-types)))

;; Improved return value processing
(defn return-value-process
  "Process database return values, filtering out next.jdbc metadata."
  [value]
  (->> value
       flatten
       (remove #(some (fn [k] (= "next.jdbc" (namespace k))) (keys %)))
       (mapcat vals)
       (mapv parse-jsonb-obj)))