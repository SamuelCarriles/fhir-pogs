[![Clojure](https://img.shields.io/badge/clojure-1.12.1-blue?logo=clojure&logoColor=white)](https://clojure.org/) 
[![FHIR Compatible](https://img.shields.io/badge/FHIR-Compatible-orange?)](https://fhir.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16.5-blue?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License : MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
# FHIR POGS - PostgreSQL + FHIR + Clojure
Clojure library to map FHIR resources to PostgreSQL.
## 📑About Library
FHIR POGS es una librería clojure que tiene como objetivo dotar al usuario de la capacidad de almacenar recursos FHIR en una base de datos Postgres e interactuar con ellos a través de operaciones básicas como **C**reate, **R**ead, **U**pdate y **D**elete (**CRUD**) y consultas sencillas sobre los campos extraídos del recurso o avanzadas sobre campos JSONB.

## ⚙️Funcionamiento
El funcionamiento de la librería se resume así:

1. Se toma el recurso o los recursos a almacenar y los campos que se desean extraer.
2. Se crea una tabla principal donde se guarda el id, el tipo de recurso y el recurso completo en formato JSONB en las columnas `id`, `resourcetype` y `content` respectivamente.
3. En una tabla aparte se guardan los otros campos que se decidieron extraer, además del id del recurso, que se usa como clave foránea para refernciar los datos de la tabla que se crea con la tabla principal que almacena todos los recursos. Si el campo del  recurso es un dato primitivo de FHIR, se almacena en una columna de un tipo de dato Postgres que pueda contenerlo, en caso contrario, se almacena en una columna JSONB.
4. Con los datos almacenados ya es posible realizar consultas básicas con filtros simples, o consultas semi-avanzadas en campos JSONB utilizando las funciones ubicadas en el namespace `querier`.

## 🛠️Operaciones Básicas: Crear/almacenar recursos

### Ejemplo de uso para un solo recurso FHIR: `map-resource`
```clj
(:require [fhir-pogs.core :refer [parse-resource]]
          [fhir-pogs.mapper :refer [map-resource]])

(def db-spec {:dbtype "postgresql"
              :dbname "resources"
              :host "localhost"
              :user "postgres"
              :password "postgres"
              :port "5432"})

(def resource (parse-resource "{\n  \"resourceType\": \"Patient\",\n  \"id\": \"exampl\",\n  \"text\": {\n    \"status\": \"generated\",\n    \"div\": \"<div xmlns=\\\"http://www.w3.org/1999/xhtml\\\">\\n\\t\\t\\t<table>\\n\\t\\t\\t\\t<tbody>\\n\\t\\t\\t\\t\\t<tr>\\n\\t\\t\\t\\t\\t\\t<td>Name</td>\\n\\t\\t\\t\\t\\t\\t<td>Peter James \\n              <b>Chalmers</b> (&quot;Jim&quot;)\\n</td>\\n\\t\\t\\t\\t\\t</tr>\\n\\t\\t\\t\\t\\t</div>\"\n  },\n  \"active\": true,\n  \"deceasedBoolean\": false\n}\n"))
;;=> {:id "example",
;;    :resourceType "Patient",
;;    :text {:status "generated",
;;           :div "<div xmlns=\"http://www.w3.org/1999/xhtml\">\n\t\t\t<table>\n\t\t\t\t<tbody>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Name</td>\n\t\t\t\t\t\t<td>Peter James \n              <b>Chalmers</b> (&quot;Jim&quot;)\n</td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t</div>"},
;;  :active true,
;;  :deceasedBoolean false}

(map-resource db-spec "fhir_resources" [:defaults] resource)
```
Aquí vemos como se almacena un recurso de tipo `Patient`  a la base de datos especificada en el `db-spec`. Se utiliza la función `map-resource` para mapear este recurso dentro de la base de datos. Esta función acepta los siguientes parámetros en orden:
- `db-spec`: las especificaciones de la base de datos en la que se almacenará el recurso. Estas especificaciones son usadas por `next.jdbc` para hacer las conexiones y ejecutar las operaciones necesarias.
- `table-name`: el nombre de la tabla donde se desea mapear el recurso.
- `mapping-fields`: un vector con el nombre de los campos del recurso que se quieren almacenar. Los nombres se dan en formato de `keyword`. Se debe tener en cuenta que el recurso tiene que tener cada campo que se desee mapear presente dentro de él. Si se quiere añadir un campo a la tabla que no está en el recurso para un uso posterior quizás, dentro del vector se puede escribir un mapa donde la clave es el nombre del campo y el valor es el tipo de dato que guardará ese campo. Algunos ejemplos : `[:meta :text :active :deceased]`, `[:defaults {:some-field :type-of-field}]`.
- `resource`: el recurso FHIR llevado a un mapa clojure.

> [!NOTE]
>  *Un detalle interesante es que dentro del vector que contiene los campos almacenar, se puede utilizar el keyword reservado `:defaults`, que indicará que se extraigan los campos `meta` y `text` del recurso.*

Veamos las tablas que resultan de este ejemplo:

---
Tabla *fhir_resources_main*

|id|resourcetype|content|
|:-:|:----------:|:-----:|
|example|Patient|"{"id" : "example"...}"|
---
Tabla *fhir_resources_patient*

|resource_id|text|
|:---------:|:--:|
|example|"{"status" : "generated"...}"|
---
Como el recurso no tenía `:meta` no se extrajo.

 Esta función es solamente para almacenar un recurso, por lo que si se van a almacenar varios recursos la indicada es `map-resources`.

### Ejemplo de uso para varios recursos FHIR: `map-resources`
Supongamos que ya tenemos una colección de recursos llamada `resources`. Para almacenarlos en una base de datos PostgreSQL tenemos que utilizar la función `map-resources` que hemos comentado en el segmento anterior. Veamos un ejemplo de su uso:

```clj
(:require [fhir-pogs.mapper :refer [map-resources]])

(def db-spec {:dbtype "postgresql"
              :dbname "resources"
              :host "localhost"
              :user "postgres"
              :password "postgres"
              :port "5432"})

(map-resources db-spec "fhir_resources" :single [:active :text] resources)
;;When we have only one type of resource, we use :single mapping type

(map-resources db-spec "fhir_resources" :specialized {:patient [:active :text] :others [:defaults]} resources)
;;When we don't have only one type of resource, we use :specialized mapping type
```
Para esta función los argumentos cambian ligeramente:
- `db-spec`: especificaciones de la base de datos en la que se almacenarán los recursos.
- `table-name`: el nombre que se tomará como base para crear las tablas necesarias.
- `mapping-type`: un keyword que especificará el tipo de mapeo que se quiere hacer. Puede ser `:single` cuando todos los recursos son del mismo tipo, o puede ser `:specialized` cuando no todos los recursos son del mismo tipo.
- `mapping-fields`: en el caso de que el `mapping-type` sea `:single`, este parámetro es un vector, como mismo en `map-resource`, pero si se va a hacer un mapeo especializado, este campo será un mapa de manera tal que cada clave sea el tipo de recurso y cada valor un vector con los campos que se desean extraer de ese recurso, sabiendo que esos campo van en formato keyword dentro del vector. Existen acá dos keywords reservados: 
  - `:all`: cuando queremos que de todos los recursos se extraigan los mismos campos.Se vería así `{:all [:meta :text]}`.
  - `:others`: cuando especificamos los campos de ciertos recursos y queremos plantear que campos extraer de cualquier otro recurso diferente de los que ya hemos especificado. Se vería así `{:patient [:active :text] :others [:defaults]}`.
  - `resources`: es una colección de recursos que se desean almacenar.

> [!IMPORTANT]
> *Las tablas resultantes de este ejemplo dependen de los recursos que se tengan, pero de manera general, para el mapeo `:single` se tendrá una tabla principal con columnas `id`, `resourcetype` y `content`, y una tabla secundaria con columnas `resource_id`, `active` y `text`; para el mapeo `specialized` se tiene la misma tabla principal, una tabla secundaria que almacenará recursos tipo `Patient` con columnas `active` y `text`, y se tendrán tantas tablas con campos `meta` y `text` como sean necesarias.*

> [!TIP] 
> *Por cada tipo diferente de recurso que se tenga, se genera una tabla diferente. Esto se hace con el objetivo de que los recursos de un mismo tipo estén almacenados en una misma tabla.* 
## 📈Estado: En desarrollo

Esta librería está en fase activa de desarrollo. Algunas funciones pueden cambiar y nuevas características están por venir.

## ✴️Próximamente
- [X] Mapear recursos FHIR (JSON/Clojure) a tablas PostgreSQL.
- [ ] Consultar recursos almacenados con filtros simples.
  - [ ] Búsqueda por `id`.
  - [ ] Búsqueda por campos planos.
  - [ ] Paginación `limit/offset`. 
- [ ] Soportar consultas semi-avanzadas a campos JSONB.
  - [ ] Consultas en campos anidados.
  - [ ] Soporte para operadores JSONB.
  - [ ] Índices GIN.


