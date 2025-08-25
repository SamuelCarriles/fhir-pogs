(ns fhir-pogs.search.modifiers
  (:require [clj-http.client :as http] 
            [clojure.string :as str]
            [honey.sql.helpers :as h]))

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

(defn g-o [m data-type v]
  (let [[part1 part2] (str/split v #"\|")
        mod (when m (-> m name keyword))]
    (if mod
      (case mod
        :exact (str "(@ == \"" part1 "\")")
        :contains (str "(@ like_regex \".*" part1 ".*\")")
        :in (let [valueset (get-valueset-codes v)]
              (reduce #(str %1 "@ == \"" %2 "\"" (if (= %2 (last valueset)) ")" " || ")) "(" valueset))
        :not-in (let [valueset (get-valueset-codes v)]
                  (reduce #(str %1 "@ != \"" %2 "\"" (if (= %2 (last valueset)) ")" " && ")) "(" valueset)))
      (if part2
        (str "(@.system == " part1 " && @.code == " part2 ")")
        (case data-type
          :string (str "(@ like_reguex \"" v ".*\")")
          (str "(@ == \"" v "\")"))))))

(defn jsonb-path-exists 
  ([path]
   (let [p (if (coll? path) path (vector path))
         conds (reduce
                #(conj %1
                       [:jsonb_path_exists :content [:cast (str "$." %2 ".** ") :jsonpath]])
                [] p)]
     (if (> (count p) 1) (vec (conj conds :or)) (first conds))))
  ([path comp]
  (when-not (or (nil? path) (not (seq path)) (nil? comp) (not (string? comp)))
    (let [p (if (coll? path) path (vector path))
          conds (reduce
                 #(conj %1
                        [:jsonb_path_exists :content [:cast (str "$." %2 ".** " (when-not (str/blank? comp) "?") comp) :jsonpath]])
                 [] p)]
      (if (> (count p) 1) (vec (conj conds :or)) (first conds))))))

(defn get-string-data-cond [path m v]
  (let [mod (when m (-> m name keyword))]
    (case mod
      :exact (jsonb-path-exists path (str "(@ == \"" v "\")"))
      :contains (jsonb-path-exists path (str "(@ like_regex \".*" v ".*\")"))
      :missing (case v
                 "true" [:not (jsonb-path-exists path)]
                 "false" (jsonb-path-exists path)
                 nil)
      (jsonb-path-exists path (str "(@ like_reguex \"" v ".*\")")))))

(comment

  
  :.)

