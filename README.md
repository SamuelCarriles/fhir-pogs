[![Clojure](https://img.shields.io/badge/clojure-1.12.1-blue?logo=clojure&logoColor=white)](https://clojure.org/) 
[![FHIR Compatible](https://img.shields.io/badge/FHIR-Compatible-orange?)](https://fhir.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16.5-blue?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License : MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/obsidian-lotus/fhir-pogs)
# FHIR POGS - PostgreSQL + FHIR + Clojure
Clojure library to map FHIR resources to PostgreSQL and interact with them. This project adheres to the [FHIR](https://www.hl7.org/fhir/) standard for healthcare data exchange. 

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
2. A main table is created to store the `id`, the resource type, and the complete resource in JSONB format in the columns `resource_id`, `resourceType`, and `content`, respectively.  
3. In a separate table, the other fields that were chosen to be extracted are stored, along with the resource `id` and `resourceType`, which are used as a foreign key to reference the data in the table created with the main table that stores all the resources. If the resource field is a primitive FHIR data type, it is stored in a PostgreSQL column of a compatible data type; otherwise, it is stored in a JSONB column.  
4. With the data stored, it is now possible to perform **CRUD** operations located in `fhir-pogs.core`.

## 🛠️Basic Operations: Create/Store Resources

### Example Usage for a Single FHIR Resource
```clojure
(:require [fhir-pogs.mapper :refer [parse-resource]]
          [fhir-pogs.core :refer [save-resource!]])

(def db-uri "jdbc:postgresql://localhost:5432/resources?user=postgres&password=postgres")

(def resource (parse-resource "{\n  \"resourceType\": \"Patient\",\n  \"id\": \"exampl\",\n  \"text\": {\n    \"status\": \"generated\",\n    \"div\": \"<div xmlns=\\\"http://www.w3.org/1999/xhtml\\\">\\n\\t\\t\\t<table>\\n\\t\\t\\t\\t<tbody>\\n\\t\\t\\t\\t\\t<tr>\\n\\t\\t\\t\\t\\t\\t<td>Name</td>\\n\\t\\t\\t\\t\\t\\t<td>Peter James \\n              <b>Chalmers</b> (&quot;Jim&quot;)\\n</td>\\n\\t\\t\\t\\t\\t</tr>\\n\\t\\t\\t\\t\\t</div>\"\n  },\n  \"active\": true,\n  \"deceasedBoolean\": false\n}\n"))
;;=> {:id "example",
;;    :resourceType "Patient",
;;    :text {:status "generated",
;;           :div "<div xmlns=\"http://www.w3.org/1999/xhtml\">\n\t\t\t<table>\n\t\t\t\t<tbody>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Name</td>\n\t\t\t\t\t\t<td>Peter James \n              <b>Chalmers</b> (&quot;Jim&quot;)\n</td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t</div>"},
;;  :active true,
;;  :deceasedBoolean false}

(save-resource! db-uri "fhir_resources" [:defaults] resource)
;;or if you only want essentials:
(save-resource! db-uri "fhir_resources" resource)
;;=>[{:id "example",
;;    :resourceType "Patient", ...}]
```
Here, we see how a `Patient`-type resource is stored in the database specified in `db-uri`. The `save-resource!` function maps and saves a FHIR resource to the database. This function accepts the following parameters in order:  
1. `db-uri`: the JDBC connection URI for the database where the resource will be stored. Example: `"jdbc:postgresql://localhost:5432/resources?user=postgres&password=postgres"`.  
2. `table-prefix`: one prefix to the name of the tables where the resource will be mapped.  
3. `mapping-fields`: a vector containing the names of the resource fields to be stored. The names are given as `keywords`. Note that the resource must contain each field to be mapped. If you want to add a field to the table that is not in the resource for later use, you can include a map in the vector where the key is the field name and the value is the data type of that field. Some examples: `[:meta :text :active :deceased]`, `[:defaults {:some-field :type-of-field}]`.  
4. `resource`: the FHIR resource converted into a Clojure map.  

If you only want to save the essentials fields (id,resourceType), you don't need to give mapping-fields. Also, it just generate the main table.

> [!NOTE]  
> *An interesting detail is that within the vector containing the fields to store, you can use the reserved keyword `:defaults`, which indicates that the `meta` and `text` fields of the resource should be extracted.*  

The resulting tables from the previous example are as follows:

---
*fhir_resources_main*

|resource_id|resourceType|content|
|:-:|:----------:|:-----:|
|example|Patient|{"id" : "example"...}|
---
*fhir_resources_patient*

|id|resourceType|text|
|:---------:|:--:|:--:|
|example|Patient|{"status" : "generated"...}|
---
The resource don't have `:meta`, therefore the `meta` column was not created in *fhir_resources_patient*.



### Example Usage for Multiple FHIR Resources
Let's suppose we already have a coll of resources called `resources`. To store them into database, we need to use `save-resources!`. Let's look at an example:

```clojure
(:require [fhir-pogs.core :refer [save-resources!]])

(def db-uri "jdbc:postgresql://localhost:5432/resources?user=postgres&password=postgres")

(save-resources! db-uri "fhir_resources" :single [:active :text] resources)
;;When we have only one type of resource, we use :single mapping type

(save-resources! db-uri "fhir_resources" :specialized {:patient [:active :text] :others [:defaults]} resources)
;;When we don't have only one type of resource, we use :specialized mapping type

;;if you only want to save essentials fields (id, resourceType and content):
(save-resources! db-uri "fhir_resources" resources)
```
For this function, the arguments change slightly:  
1. `db-uri`: the JDBC connection URI for the database where the resources will be stored.  
2. `table-prefix`: the table's name prefix used to create the necessary tables.  
3. `mapping-type`: a keyword specifying the type of mapping to be performed. It can be `:single` when all resources are of the same type, or `:specialized` when the resources are of different types.  
4. `mapping-fields`: if the `mapping-type` is `:single`, this parameter is a vector, similar to `save-resource!`. However, if the mapping is specialized, this field will be a map where each key is the resource type and each value is a vector containing the fields to be extracted from that resource, given as keywords within the vector. There are two reserved keywords here:  
  - `:all`: used when you want the same fields to be extracted from all resources. It looks like this: `{:all [:meta :text]}`.  
  - `:others`: used when specifying fields for certain resources and you want to define which fields to extract from any other resource not already specified. It looks like this: `{:patient [:active :text] :others [:defaults]}`.  
5. `resources`: a collection of resources to be stored.  

> [!IMPORTANT]  
> *The resulting tables from this example depend on the resources available. Generally, for `:single` mapping, there will be a main table with columns `id`, `resourceType`, and `content`, and a secondary table with columns `resource_id`, `active`, and `text`. For `:specialized` mapping, the same main table is created, a secondary table storing `Patient`-type resources with columns `active` and `text`, and as many additional tables with `meta` and `text` fields as needed.*  

> [!TIP]  
> *A separate table is generated for each different resource type. This ensures that resources of the same type are stored together in the same table.* 

## 🛠️Basic Operations: Read/Search Resources
To look up a resource —or a bunch of them— that match certain criteria, you'll want to use the `search-resources` function. Here's a quick example:
```clojure
(:require [fhir-pogs.core :refer [search-resources]])

(def db-uri "jdbc:postgresql://localhost:5432/resources?user=postgres&password=postgres")

(search-resources db-uri "fhir_resources" "Patient" [:= :resource_id "pat123"])
;;=>({:id "pat123", :name [{:given ["Sam"], :family "Altman"}], :resourceType "Patient"})
```
This function takes four arguments:
1. `db-uri`: the JDBC connection URI for the database where the search will happen.
2. `table-prefix`: the prefix used for the tables you're working with.
3. `restype`: the type of resource you're looking for.
4. `conditions`: a vector of vectors, each one representing a condition that the resource has to meet to be returned.

>[!IMPORTANT]
*Conditions need to follow the format supported by the honeysql library. If you want the full scoop, check out the README for honeysql. But in short, a condition is just a vector where the first item is a keyword operator, and the rest are the values used to evaluate the condition. For example, a valid condition could be `[:= :gender "female"]`. That would only work if your table has a gender column, but you get the idea.*

You can make your search conditions as complex as you want —just stick to the honeysql format.

## 🛠️Basic Operations: Put/Update Resources
To update a resource, use `update-resource!`:
```clojure
(:require [fhir-pogs.core :refer [update-resource!]])

(def db-uri "jdbc:postgresql://localhost:5432/resources?user=postgres&password=postgres")

(update-resource! db-uri "fhir_resources" "Patient" "pat123" {:id "pat123"
                                                               :resourceType "Patient"
                                                               :name [{:given ["Sam"]
                                                                       :family "Altman"}]})
```
Here's what you need to pass in:
1. `db-uri`: the JDBC connection URI for your database.
2. `table-prefix`: the table prefix you're working with.
3. `restype`: the type of resource you're updating.
4. `id`: the ID of the resource you want to update.
5. `new-content`: the full resource with the updated fields.

## 🛠️Basic Operations: Delete Resources
To delete resources from the database, use `delete-resources!`:
```clojure
(:require [fhir-pogs.core :refer [delete-resources!]])

(def db-uri "jdbc:postgresql://localhost:5432/resources?user=postgres&password=postgres")

(delete-resources! db-uri "fhir_resources" "Patient" [[:= :resource_id "pat123"]])
```
This function works just like `search-resources`, except instead of returning a sequence of resources, it gives you a `fully-realized` result set from `next.jdbc` to confirm the operation went through.

## 📈Status: In Development
This library is in active development. The **core functions** (CRUD) are ready to use. The **search** namespace is currently under development and will be included in future release. 

## ✴️Coming Soon      
- [ ] Build the necessary tools in `fhir-pogs.search` to transform an AST tree produced by a FHIR search query into condition clauses usable by `fhir-pogs.core/search-resources!`.
  - [X] Support **token search** with commons modifiers: not, missing, text, of-type. 
  - [ ] Support **string search**: names, text fields (family, given, address.city)
    - [ ] Support :exact, :contains, :missing modifiers
- [ ] Define `fhir-pogs.search/format` to format search results in compliance with FHIR search standards.