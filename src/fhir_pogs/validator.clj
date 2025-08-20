(ns fhir-pogs.validator
  (:require [json-schema.core :refer [validate]]
            [cheshire.core :refer [parse-string]]))

(def fhir-schema (parse-string (slurp "resources/fhir-schema.json") true))


(defn validate-resource [resource]
  (try (validate fhir-schema resource)
       (catch clojure.lang.ExceptionInfo e
         (let [errors (->> (ex-data e) :errors set vec)]
           (throw (ex-info (str "Invalid resource schema\n" (.getMessage e))
                           {:type :schema-validation
                            :errors errors}))))))

(defn valid-resource? [resource]
  (try (validate-resource resource) true
       (catch Exception _ nil)))

(defn validate-mapping-fields [mf mt]
  (let [get-type #(-> % type .getSimpleName)
        result (if (empty? mf)
                 (cond
                   (vector? mf) [{:expected "non-empty vector"
                                  :got "empty vector"}]
                   (map? mf) [{:expected "non-empty map"
                               :got "empty map"}])
                 [])
        error-data (->> (case mt
                          :single (reduce
                                   (fn [o i]
                                     (if-not (or (keyword? i) (map? i))
                                       (conj o {:index [(.indexOf mf i)]
                                                :expected "A keyword or a keyword-to-keyword map"
                                                :got (str (get-type i) " element")})
                                       (if-not (map? i) o
                                               (->> (reduce-kv
                                                     (fn [b k v]
                                                       (->> (cond-> []
                                                              (not (keyword? k))
                                                              (conj {:index [(.indexOf mf i)
                                                                             (.indexOf (vec i) [k v])
                                                                             0]
                                                                     :expected "A keyword-to-keyword map"
                                                                     :got (str (get-type k) " as key")})
                                                              (not (keyword? v))
                                                              (conj {:index [(.indexOf mf i)
                                                                             (.indexOf (vec i) [k v])
                                                                             1]
                                                                     :expected "A keyword-to-keyword map"
                                                                     :got (str (get-type v) " as value")}))
                                                            (into b))) [] i)
                                                    (into o)))))
                                   result mf)

                          :specialized (reduce-kv
                                        (fn [o k v]
                                          (let [mf-v (vec mf)]
                                            (if (empty? v)
                                              (conj o {:index [(.indexOf mf-v [k v]) 1]
                                                       :expected "non-empty vector as value"
                                                       :got "empty vector"})
                                              (->> (cond-> []
                                                     (not (keyword? k)) (conj {:index [(.indexOf mf-v [k v]) 0]
                                                                               :expected "A keyword as key"
                                                                               :got (str (get-type k) " element")})

                                                     (not (vector? v)) (conj {:index [(.indexOf mf-v [k v]) 1]
                                                                              :expected "A vector as value"
                                                                              :got (str (get-type v) " element")})

                                                     (vector? v) (into (->> (validate-mapping-fields v :single)
                                                                            (mapv #(update % :index (fn [n] (into [(.indexOf mf-v [k v])] n)))))))
                                                   (into o)))))
                                        result mf))

                        (assoc {:type :schema-validation} :errors))]
    (when-not (empty? (:errors error-data))
      (throw (ex-info "Invalid mapping-fields schema." error-data)))))



