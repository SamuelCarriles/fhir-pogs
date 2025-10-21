(ns fhir-pogs.search.token
  (:require [fhir-pogs.search.param-data :as param]
            [fhir-pogs.db :as db] 
            [clojure.string :as str]))

(defn process-param [columns restype paths param]
  (let [{search-param :name :keys [modifier value params]} param
        mod (or (when modifier (-> modifier name)) "base")]
    (into [] (if (some #(= (name %) search-param) columns)
               (let [locations (map #(let [[column path] (str/split % #"\." 2)]
                                       {:column (keyword column)
                                        :path (or path column)}) paths)]
                 (mapcat (fn [{:keys [column path]}]
                           (if-not params
                             [[:'fhir_token_search column path value mod]]
                             (map #(process-param columns restype paths %) params))) locations))
               ;;
               (mapcat (fn [path]
                         (if-not params
                           [[:'fhir_token_search :content path value mod]]
                           (map #(process-param columns restype paths %) params))) paths)))))

(defn token-search-conds [db-spec table-prefix restype params]
  (let [columns (db/get-columns db-spec (str table-prefix "_" (str/lower-case restype)))
        conditions (mapcat (fn [{:keys [name] :as param}]
                             (let [paths (:paths (param/get-data restype name))]
                               (process-param columns restype paths param))) params)] 
    (if (> (count conditions) 1) (into [:or] cat conditions) (first conditions))))

(comment
  (def db-spec {:dbtype "postgresql"
                :dbname "resources"
                :host "localhost"
                :user "postgres"
                :port "5432"
                :password "postgres"})

  ;; Si la columna está, se splitea el primer valor del path y luego se aplica la función correspondiente 
  ;; en esa columna 
  
  :.
  )