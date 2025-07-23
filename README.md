[![Clojure](https://img.shields.io/badge/clojure-1.12.1-blue?logo=clojure&logoColor=white)](https://clojure.org/) 
[![FHIR Compatible](https://img.shields.io/badge/FHIR-Compatible-orange?)](https://fhir.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16.5-blue?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License : MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![AskDeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/SamuelCarriles/fhir-pogs)
# FHIR POGS - PostgreSQL + FHIR + Clojure
Clojure library to map FHIR resources to PostgreSQL and interact with them.  
## 🤔Why this name?
Why "POGS" instead of just "pg" or "postgres"? What we're eliminating with this library: 
- **S**uperfluous
- **T**axing
- **R**edundant
- **E**xcess

Therefore: 
```clojure
(-> "POSTGRES" 
      (clojure.string/replace-first #"S" "")
      (clojure.string/replace-first #"T" "")
      (clojure.string/replace-first #"R" "")
      (clojure.string/replace-first #"E" ""))
;;=> "POGS"
```
Just as we remove these letters, FHIR POGS strips away unnecessary complexity when working with FHIR resources in Postgres, leaving only what's essential.

## 📑About Library
FHIR POGS is a Clojure library that aims to provide users with the capability to store FHIR resources in a PostgreSQL database and interact with them through **CRUD** operations (**C**reate, **R**ead, **U**pdate, and **D**elete).
## ⚙️How It Works
The library's operation can be summarized as follows:  

1. The resource or resources to be stored and the fields to be extracted are taken.  
2. A main table is created to store the `id`, the resource type, and the complete resource in JSONB format in the columns `id`, `resourcetype`, and `content`, respectively.  
3. In a separate table, the other fields that were chosen to be extracted are stored, along with the resource `id`, which is used as a foreign key to reference the data in the table created with the main table that stores all the resources. If the resource field is a primitive FHIR data type, it is stored in a PostgreSQL column of a compatible data type; otherwise, it is stored in a JSONB column.  
4. With the data stored, it is now possible to perform **CRUD** operations located in `fhir-pogs.core`.

## 🛠️Basic Operations: Create/Store Resources

### Example Usage for a Single FHIR Resource: `save-resource!`
```clj
(:require [fhir-pogs.mapper :refer [parse-resource]]
          [fhir-pogs.core :refer [save-resource!]])

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

(save-resource! db-spec "fhir_resources" [:defaults] resource)
```
Here, we see how a `Patient`-type resource is stored in the database specified in `db-spec`. The `save-resource!` function maps and saves a FHIR resource to the database. This function accepts the following parameters in order:  
- `db-spec`: the database specifications where the resource will be stored. These specifications are used by `next.jdbc` to establish connections and execute the necessary operations.  
- `table-prefix`: one prefix to the name of the tables where the resource will be mapped.  
- `mapping-fields`: a vector containing the names of the resource fields to be stored. The names are given as `keywords`. Note that the resource must contain each field to be mapped. If you want to add a field to the table that is not in the resource for later use, you can include a map in the vector where the key is the field name and the value is the data type of that field. Some examples: `[:meta :text :active :deceased]`, `[:defaults {:some-field :type-of-field}]`.  
- `resource`: the FHIR resource converted into a Clojure map.  

> [!NOTE]  
> *An interesting detail is that within the vector containing the fields to store, you can use the reserved keyword `:defaults`, which indicates that the `meta` and `text` fields of the resource should be extracted.*  

The resulting tables from the previous example are as follows:

---
*fhir_resources_main*

|resource_id|resourcetype|content|
|:-:|:----------:|:-----:|
|example|Patient|"{"id" : "example"...}"|
---
*fhir_resources_patient*

|id|resourcetype|text|
|:---------:|:--:|:--:|
|example|Patient|"{"status" : "generated"...}"|
---
The resource don't have `:meta`, therefore the `meta` column was not created in *fhir_resources_patient*.



### Example Usage for Multiple FHIR Resources: `save-resources!` 
Let's suppose we already have a coll of resources called `resources`. To store them into database, we need to use `save-resources!`. Let's look at an example:

```clj
(:require [fhir-pogs.core :refer [save-resources!]])

(def db-spec {:dbtype "postgresql"
              :dbname "resources"
              :host "localhost"
              :user "postgres"
              :password "postgres"
              :port "5432"})

(save-resources! db-spec "fhir_resources" :single [:active :text] resources)
;;When we have only one type of resource, we use :single mapping type

(save-resources! db-spec "fhir_resources" :specialized {:patient [:active :text] :others [:defaults]} resources)
;;When we don't have only one type of resource, we use :specialized mapping type
```
For this function, the arguments change slightly:  
- `db-spec`: the database specifications where the resources will be stored.  
- `table-prefix`: the table's name prefix used to create the necessary tables.  
- `mapping-type`: a keyword specifying the type of mapping to be performed. It can be `:single` when all resources are of the same type, or `:specialized` when the resources are of different types.  
- `mapping-fields`: if the `mapping-type` is `:single`, this parameter is a vector, similar to `save-resource!`. However, if the mapping is specialized, this field will be a map where each key is the resource type and each value is a vector containing the fields to be extracted from that resource, given as keywords within the vector. There are two reserved keywords here:  
  - `:all`: used when you want the same fields to be extracted from all resources. It looks like this: `{:all [:meta :text]}`.  
  - `:others`: used when specifying fields for certain resources and you want to define which fields to extract from any other resource not already specified. It looks like this: `{:patient [:active :text] :others [:defaults]}`.  
- `resources`: a collection of resources to be stored.  

> [!IMPORTANT]  
> *The resulting tables from this example depend on the resources available. Generally, for `:single` mapping, there will be a main table with columns `id`, `resourcetype`, and `content`, and a secondary table with columns `resource_id`, `active`, and `text`. For `:specialized` mapping, the same main table is created, a secondary table storing `Patient`-type resources with columns `active` and `text`, and as many additional tables with `meta` and `text` fields as needed.*  

> [!TIP]  
> *A separate table is generated for each different resource type. This ensures that resources of the same type are stored together in the same table.*  

## ⚗️Tests
To run all tests, first start the container defined in `docker-compose.yml` and after execute the test:
```bash
docker compose up -d
clojure -M:test
```
>[!NOTE]
>*When tests begin, the current tables are created and when the tests finished, database is reset.*
### Prerequisites
- Ensure JSON test resources are in `resources/json/`.
- Requires **Clojure CLI**.
- **Docker** and **Docker Compose**.
## 📈Status: In Development
This library is in active development. Some functions may change, and new features are coming soon.  

## ✴️Coming Soon  
- [X] Map FHIR resources (JSON/Clojure) to PostgreSQL tables.  
- [X] Query stored resources with filters.  
  - [x] Search by `id`.  
  - [x] Search by simple and advanced conditions.    
- [ ] Support semi-advanced queries on JSONB fields.  
  
