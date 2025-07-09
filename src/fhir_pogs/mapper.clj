(ns fhir-pogs.mapper
  (:require [cheshire.core :refer [parse-string generate-string]]
            [honey.sql :as sql]
            [honey.sql.helpers :as help]
            [next.jdbc :as jdbc])
  (:import [org.postgresql.util PGobject]))

(def resources-fields {:resourceType :string
                       :id :string})

(defn to-pg-obj [^String type value]
  (doto (PGobject.)
    (.setType type)
    (.setValue value)))

(defn type-of "Retorna un vector que contiene como primer elemento 
               el keyword asociado al tipo de dato postgres, y como 
               segundo elemento el valor a guardar en el campo postgres."
  [value]
  (when (nil? value) (throw (IllegalArgumentException. "The value can't be nil")))
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

(defn create-table
  "Devuelve una sentencia SQL para crear una tabla con los campos especificados. Recibe como primer argumento
   el nombre de la tabla y como segundo los campos que se quieren extraer en formato de `keywords`. 
   Este vector puede también contener el keyword reservado `:defaults` y esto se asumirá como que se quieren los campos por defecto y los
   demás campos que estén también en el vector. Es decir, `[:defaults :name :text]` equivale a `[:meta :jsonb :text :jsonb :name :text]`.\n Ejemplo de uso:\n ```clojure
   (create-table \"fhir_resources\" [:defaults])\n => [\"CREATE TABLE IF NOT EXISTS fhir_resources (id TEXT PRIMARY KEY NOT NULL, resourceType TEXT NOT NULL, content JSONB NOT NULL, meta JSONB, text JSONB)\"]"
  [^String main-table-name fields]
  (let [default-columns [[:id :text :primary-key :not-null]
                         [:resourceType :text :not-null]
                         [:content :jsonb :not-null]]
        columns (into (if (some #(= % :defaults) fields)
                        (conj default-columns [:meta :jsonb] [:text :jsonb])
                        default-columns)
                      (mapv vec (partition 2 (remove #(= % :defaults) fields))))]
    (-> (help/create-table (keyword main-table-name) :if-not-exists)
        (help/with-columns columns)
        sql/format)))

(defn map-template [^String table-name resource fields]
  (let [f (set (conj (if (some #(= % :defaults) fields)
                       (conj (remove #(= % :defaults) fields) :meta :text)
                       fields) :id :resourceType))]
    (-> {:main-table table-name}
        (assoc :fields
               (-> (remove nil?
                           (mapv
                            (fn [[n v]]
                              (let [type (type-of v)
                                    dtype (first type)
                                    value (second type)]
                                {:name n :data-type dtype
                                 :value value}))
                            (reduce (fn [o [n v]]
                                      (if (contains? f n)
                                        (assoc o n v) o))
                                    {} resource)))
                   (vec)
                   (conj {:name :content
                          :data-type :json
                          :value (to-pg-obj "jsonb" (generate-string resource))}))))))

(defn create-sentence [temp]
  (let [main-table (:main-table temp)
        fields (:fields temp)]
    (->> (reduce (fn [o {:keys [name value]}]
                   (assoc-in o [(keyword main-table) name] value))
                 {} fields)
         (map (fn [[n v]] (-> (help/insert-into n)
                              (help/values [v])
                              (sql/format)))))))

(defn fields-types [fields r]
  (reduce (fn [x y]
            (if (get r y) (conj x y (first (type-of (get r y)))) x))
          [] (set (if (some #(= % :defaults) fields)
                    (conj (remove #(= % :defaults) fields)
                          :meta :text)
                    fields))))

(defn jdbc-execute! [db-spec sentence]
  (let [my-datasource (jdbc/get-datasource db-spec)]
    (with-open [connection (jdbc/get-connection my-datasource)]
      (jdbc/execute! connection sentence))))

(defn map-resource [db-spec ^String table-name mapping-fields resource]
  (let [fields-and-types (fields-types mapping-fields resource)]
    (jdbc-execute! db-spec (create-table table-name fields-and-types)) 
    (map #(jdbc-execute! db-spec %) (create-sentence (map-template table-name resource mapping-fields)))))

;;Hay que hacer que fields-and-types sea un map y arreglar lo que traiga eso consigo
(comment

  (def test-1 (parse-string (slurp "/home/samuel/Documents/FHIR/json/resources_example/resource_1.json") true))
  (def test-2 (parse-string (slurp "/home/samuel/Documents/FHIR/json/resources_example/resource_2.json") true))
  (def test-3 (parse-string (slurp "/home/samuel/Documents/FHIR/json/resources_example/resource_3.json") true))

  (def db-spec {:dbtype "postgresql"
                :dbname "resources"
                :host "localhost"
                :user "postgres"
                :port "5432"
                :password "postgres"})
  (map-resource db-spec "fhir_resources" [:defaults :active] test-3)

  (map :columns/column_name (flatten (jdbc-execute! db-spec ["select column_name from information_schema.columns where table_name = ?" "fhir_resources"])))
  :.
  )