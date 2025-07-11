(ns fhir-pogs.mapper
  (:require [cheshire.core :refer [generate-string]]
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

(defn create-table "Devuelve una sentencia SQL para crear una tabla con los campos especificados. Si se llama la fn con un solo argumento creará la la tabla principal donde
                    se almacenarán los datos generales de cada recurso.\n - `table-name`: nombre base de la tabla. \n - `restype`: tipo de recurso que almacenará. \n - `fields`: 
                    un mapa con los campos que se quieren extraer y el tipo de dato que almacenan. 
                    \n Ejemplo de uso:\n ```clojure \n(create-table \"fhir_resources\")\n;; => [\"CREATE TABLE IF NOT EXISTS fhir_resources_main (resource_id TEXT PRIMARY KEY NOT NULL, resourceType TEXT NOT NULL, content JSONB NOT NULL)\"] \n(create-table \"fhir_resources\" \"Patient\" {:meta :jsonb :text :jsonb})\n;; => [\"CREATE TABLE IF NOT EXISTS fhir_resources_Patient (id TEXT PRIMARY KEY NOT NULL, resourceType TEXT NOT NULL, content JSONB NOT NULL, meta JSONB, text JSONB)\"]"
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

(defn template "Crea un mapa que servirá de plantilla
                para insertar en la base de datos los
                campos que se quieren mapear del recurso FHIR.
                \n - `table-name`: el nombre de la tabla donde
                serán insertados los campos.\n - `fields`:
                un vector con los campos que se desean mapear 
                en la base de datos.\n - `resource`: 
                recurso FHIR llevado a un mapa clojure."
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

(defn insert-to-sentence "Crea las sentencias SQL necesarias
                          para insertar todos los datos de `template`
                          en su respectiva tabla.\n - `template`: mapa 
                          que resulta de aplicar la fn `template`."
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
                     (if (= :id name)
                       (assoc-in result [(keyword (str table "_" restype)) :id] value)
                       result)))
                 {} fields)
         (map (fn [[n v]] (-> (help/insert-into n)
                              (help/values [v])
                              (sql/format))))
         (sort-by (fn [x] (not (re-find #"_main" (first x))))))))

(defn fields-types "Retorna un mapa donde cada clave es
                    un campo y su valor asociado es el tipo
                    de dato de ese campo.\n - `f`: un vector 
                    con los campos de los que se desea conocer 
                    el tipo de dato que almacenan. Dentro de
                    este vector pueden existir mapas para los
                    campos que no aparecen en el recurso pero que 
                    aún así se desean crear en la tabla, donde
                    cada clave es un campo, y el  valor es el
                    tipo de dato. Tanto clave como valor son `keywords`.
                    \n - `r`: el recurso FHIR llevado a un mapa clojure." 
  ([f r]
  (let [final (apply merge (filter map? f))
        fields (remove map? f)]
    (merge final (reduce (fn [x y]
                           (if-let [value (if (vector? r) (some (fn [x] (when (contains? x y) (get x y))) r)
                                              (get r y))]
                             (assoc x y (first (type-of value)))
                             x))
                         {} (set (if (some #(= % :defaults) fields)
                                   (conj (remove #(= % :defaults) fields)
                                         :meta :text)
                                   fields)))))))

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
                    mapear. Los nombres se dan en formato de `keyword`. Se debe tener en cuenta que el recurso
                    tiene que tener cada campo que se desee mapear. Si se quiere añadir
                    un campo a la tabla que no está en el recurso para un uso posterior quizá,
                    dentro del vector se puede escribir un mapa donde la clave es el nombre del campo y el valor
                    es el tipo de dato que guardará ese campo.Algunos ejemplos : `[:meta :text :active :deceased]`, `[:defaults {:some-field :type-of-field}]`.
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
  (let [fields (fields-types mapping-fields resource)
        restype (:resourceType resource)]
    (jdbc-execute! db-spec (create-table table-name))
    (jdbc-execute! db-spec (create-table  table-name restype fields))
    (map #(jdbc-execute! db-spec %) (insert-to-sentence (template table-name (keys fields) resource) restype))))

(defn map-resources "Trabaja my parecido a `map-resource`, la diferencia es que
                     maneja varios recursos y no solamente uno. Los recursos pueden
                     tener dos tipos de mapping:\n - `:single`: todos los recursos
                     son de un mismo tipo, por lo que se pueden insertar en una tabla única. 
                     Para este tipo el parámetro `mapping-fields` es un vector de campos en 
                     formato `keyword.`\n - `:specialized`: los recursos son de varios tipos, por lo que
                     se crea una tabla diferente para cada tipo de recurso. Para este tipo, `mapping-fields`
                      es un mapa donde las claves son el tipo de recurso y los valores son vectores
                     que contienen los campos en formato `keyword`. Existen dos `keywords` reservadas: `:all` y `:others`.
                     La primera se usa cuando se quieren dar los campos a extraer de todos los recursos, y la
                     segunda cuando se han dado ya tipos de recursos con sus campos y se quiere especificar
                     qué campos extraer de cualquier otro tipo de recurso. Se recomienda siempre poner `:others`, pero
                     en caso de no estar y encnotrarse un recurso de un tipo no especificado, se mapearán los campos básicos en 
                     la tabla principal o controladora. Con campos básicos me refiero a `:id`, `:resourceType` y `:content`.\n 
                     Ejemplo de llamada de la fn: \n```clojure \n(map-resources db-spec \"fhir_reources\" :single [:defaults] <resources>) \n(map-resources db-spec \"fhir_resources_database\" :specialized {:all [:text]} <resources>)"
  [db-spec ^String table-name mapping-type mapping-fields resources]
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



(comment
  #_(let [fields (fields-types mapping-fields resources)]
      (jdbc-execute! db-spec (create-table table-name))
      (map (fn [x]
             (let [restype (:resourceType x)]
               (jdbc-execute! db-spec (create-table  table-name restype fields))
               (map #(jdbc-execute! db-spec %) (insert-to-sentence (template table-name (keys fields) x) restype))))
           resources))
  :.)