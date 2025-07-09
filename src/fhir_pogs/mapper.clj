(ns fhir-pogs.mapper
  (:require [cheshire.core :refer [parse-string generate-string]]
            [honey.sql :as sql]
            [honey.sql.helpers :as help]
            [next.jdbc :as jdbc])
  (:import [org.postgresql.util PGobject]))


(defn to-pg-obj "Crea un PGobject con el tipo y valor dados." 
  [^String type value]
  (when (some nil? [type value])
    (throw (IllegalArgumentException. "Some argument it's nil.")))
  (doto (PGobject.)
    (.setType type)
    (.setValue value)))

(defn type-of "Retorna un vector que contiene como primer elemento 
               el keyword asociado al tipo de dato postgres, y como 
               segundo elemento el valor a guardar en el campo postgres."
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

(defn create-table "Devuelve una sentencia SQL para crear una tabla con los campos especificados. Recibe como primer argumento
   el nombre de la tabla y como segundo un mapa con los campos que se quieren extraer y el tipo de dato que almacenan. 
  \n Ejemplo de uso:\n ```clojure
   (create-table \"fhir_resources\" {:meta :jsonb :text :jsonb})\n => [\"CREATE TABLE IF NOT EXISTS fhir_resources (id TEXT PRIMARY KEY NOT NULL, resourceType TEXT NOT NULL, content JSONB NOT NULL, meta JSONB, text JSONB)\"]"
  [^String main-table-name fields]
  (let [default-columns [[:id :text :primary-key :not-null]
                         [:resourceType :text :not-null]
                         [:content :jsonb :not-null]]
        columns (into default-columns fields)]
    (-> (help/create-table (keyword main-table-name) :if-not-exists)
        (help/with-columns columns)
        sql/format)))

(defn template "Crea un mapa que servirá de plantilla
                para insertar en la base de datos los
                campos que se quieren mapear del recurso FHIR.
                \n - `table-name`: el nombre de la tabla donde
                serán insertados los campos.\n - `resource`: 
                recurso FHIR llevado a un mapa clojure.\n - `fields`:
                un vector con los campos que se desean mapear en la base de datos."
  [^String table-name resource fields]
  (let [f (set (conj fields :id :resourceType))]
    (-> {:main-table table-name}
        (assoc :fields
               (-> (remove nil?
                           (mapv
                            (fn [[n v]]
                              (let [type (type-of v)
                                    value (second type)]
                                {:name n
                                 :value value}))
                            (reduce (fn [o entry]
                                      (if (contains? f (key entry))
                                        (conj o entry) o))
                                    {} resource)))
                   (vec)
                   (conj {:name :content
                          :value (to-pg-obj "jsonb" (generate-string resource))}))))))

(defn insert-to-sentence "Crea las sentencias SQL necesarias
                          para insertar todos los datos de `template`
                          en su respectiva tabla.\n - `template`: mapa 
                          que resulta de aplicar la fn `template`."
  [template]
  (let [main-table (:main-table template)
        fields (:fields template)]
    (->> (reduce (fn [o {:keys [name value]}]
                   (assoc-in o [(keyword main-table) name] value))
                 {} fields)
         (map (fn [[n v]] (-> (help/insert-into n)
                              (help/values [v])
                              (sql/format)))))))

(defn fields-types "Retorna un mapa donde cada clave es
                    un campo y su valor asociado es el tipo
                    de dato de ese campo.\n - `fields`: un vector 
                    con los campos de los que se desea conocer 
                    el tipo de dato que almacenan.\n - `r`: el recurso
                   FHIR llevado a un mapa clojure."
  [fields r]
  (reduce (fn [x y]
            (if (get r y) (assoc x y (first (type-of (get r y)))) x))
          {} (set (if (some #(= % :defaults) fields)
                    (conj (remove #(= % :defaults) fields)
                          :meta :text)
                    fields))))

(defn jdbc-execute! "Obtiene el datasource correspondiente al db-spec,
                     se conecta a la base de datos y
                     ejecuta un jdbc/execute! dentro de un bloque with-open."
  [db-spec sentence]
  (let [my-datasource (jdbc/get-datasource db-spec)]
    (with-open [connection (jdbc/get-connection my-datasource)]
      (jdbc/execute! connection sentence))))

(defn map-resource "Mapea un recurso FHIR en una data base.\n - `db-spec`: las especificaciones de la base de
                    datos. Por ejemplo :\n {:dbtype \"postgresql\", :dbname \"resources\", :host \"localhost\", :user \"postgres\", :port \"5432\", :password \"postgres\"}\n - `table-name`: el nombre de la tabla donde se desea mapear el recurso.
                    \n - `mapping-fields`: un vector con el 
                    nombre de los campos del recurso que se quieren
                    mapear. Los nombres se dan en formato de `keyword`.
                    Por ejemplo : `[:meta :text :active :deceased]`.
                    \n - `resource`: el recurso FHIR llevado a un mapa clojure.
                    \n Ejemplo de uso:\n ```clojure\n (def test-1 (parse-string <json resource> true))
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
  (let [fields (fields-types mapping-fields resource)]
    (jdbc-execute! db-spec (create-table table-name fields))
    (map #(jdbc-execute! db-spec %) (insert-to-sentence (template table-name resource (keys fields))))))
