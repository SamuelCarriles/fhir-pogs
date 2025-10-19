(ns fhir-pogs.search.token
  (:require [fhir-pogs.search.param-data :as param]
            [fhir-pogs.db :as db]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [fhir-search.uri-query :refer [parse]]
            [fhir-search.complex :refer [clean]]
            [clojure.string :as str]))

(comment
  (def db-spec {:dbtype "postgresql"
                :dbname "resources"
                :host "localhost"
                :user "postgres"
                :port "5432"
                :password "postgres"})

  (defn process-param [path {:keys [name join params modifier value]}])

  (defn token-search [db-spec table-prefix {:keys [type params]}]
    (let [table (str table-prefix "_" (str/lower-case type))
          columns (db/get-columns db-spec table)]
      (reduce (fn [acc curr]
                (let [{:keys [paths]} (param/get-data type (:name curr))
                      set-paths (->> paths
                                       (map #(-> (str/split % #"\." 2) first))
                                       set)]
                  (if (some columns set-paths)
                    ) ))
              [] params)))



  (let [table-prefix "fhir"
        restype "Patient"
        search-param "active"]
    (some #(= (name %) search-param) (db/get-columns db-spec (str table-prefix "_" (.toLowerCase restype)))))


  (filter #(and (str/includes? % "fhir")
                (some (fn [x] (= (name x) "activ")) (db/get-columns db-spec %))) (db/get-tables db-spec))

  ;; Si el filtro no da seq, hay que buscar en la main table, en content
  {:type "Patient"
   :join :fhir.search.join/and
   :params [{:name "active"
             :value "true"}]}

  :.)