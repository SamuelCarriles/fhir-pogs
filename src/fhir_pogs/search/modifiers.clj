(ns fhir-pogs.search.modifiers
  (:require [clj-http.client :as http] 
            [clojure.string :as str]))

(def terminology-server "https://tx.fhir.org/r4")

(defn expand-valueset [valueset-url]
  (try
    (let [response (http/get (str terminology-server "/ValueSet/$expand")
                             {:query-params {:url valueset-url}
                              :accept :json
                              :as :json
                              :timeout 10000})]
      (:body response))
    (catch Exception e (println (str "Error: " (.getMessage e)))
           nil)))

(defn get-valueset-codes [url]
  (->> (get-in (expand-valueset url) [:expansion :contains])
       (map :code)))

(defn get-operator [m v]
  (let [op (when-not (nil? m) (-> m name keyword))
        vl (str/split v #"\|")
        value (if (re-find #"\|" v) 
                (str "(@.system == \"" (first vl) "\" && @.code == \"" (second vl) "\"") 
                v)
        sv (str "\"" value "\"") 
        operator (case op
                   :exact (str " ? (@ ==" sv ")")
                   :contains (str " ? (@ like_regex \".*" v ".*\")")
                   :missing ""
                   :not (str " ? (@ !=" sv ")") 

                   :in (reduce #(str %1 "@ == \"" %2 "\"" (if (= %2 (last value)) ")" " || ")) " ? (" value)
                   :not-in (reduce #(str %1 "@ != \"" %2 "\"" (if (= %2 (last value)) ")" " && ")) " ? (" value)

                   :identifier (str " ? (@.system ==" (first value) "&& @.value ==" (second value) ")")
                   :code-text (str " ? (@ like %" v "%)")
                   :iterate (str " ? (@ ==" sv ")")
                   :of-type (str " ? (@.type == " sv ")")
                   :above (str " ? (@ starts with " sv ")")
                   :below (str " ? (" sv " starts with @)") ;;Revisar soporte para below
                   nil (str "? (@ ==\"" v "\")")
                   (throw (IllegalArgumentException. (str "Incorrect modifier: " m))))]
                   [value operator]))

(defn g-o [m v]
  (let [[part1 part2] (str/split v #"\|")
        mod (when m (-> m name keyword))]
    (if mod
      (case mod
        :exact (str " ? (@ == \"" part1 "\")") 
        :in (let [valueset (get-valueset-codes v)]
              (reduce #(str %1 "@ == \"" %2 "\"" (if (= %2 (last valueset)) ")" " || ")) " ? (" valueset))
        :not-in (let [valueset (get-valueset-codes v)]
                  (reduce #(str %1 "@ != \"" %2 "\"" (if (= %2 (last valueset)) ")" " && ")) " ? (" valueset)))
      (if part2
        (str " ? (@.system == " part1 " && @.code == " part2 ")")
        (str "? (@ == \"" v "\")")))))

(comment
(g-o :fhir.search.modifier/in "http://hl7.org/fhir/ValueSet/condition-category") 
  
  :.)

