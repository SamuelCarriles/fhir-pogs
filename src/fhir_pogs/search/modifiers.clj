(ns fhir-pogs.search.modifiers
  (:require [clojure.string :as str]
            [honey.sql.helpers :as h]))

(defn jsonb-path-exists
  ([path]
   (let [conds (reduce
                #(conj %1
                       [:jsonb_path_exists :content [:cast (str "$." %2 ".** ") :jsonpath]])
                [] path)]
     (if (> (count path) 1) (vec (conj conds :or)) (first conds))))
  ([path comp]
   (when-not (or (nil? path) (not (seq path)) (nil? comp) (not (string? comp)))
     (let [path (if (coll? path) path (vector path))
           conds (reduce
                  #(conj %1
                         [:jsonb_path_exists :content [:cast (str "$." %2 ".** " (when-not (str/blank? comp) "?") comp) :jsonpath]])
                  [] path)]
       (if (> (count path) 1) (vec (conj conds :or)) (first conds))))))

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

  (defn jsonb-path-exists
    ([path]
     (let [conds (reduce
                  #(conj %1
                         [:jsonb_path_exists :content [:cast (str "$." %2 ".** ") :jsonpath]])
                  [] path)]
       (if (> (count path) 1) (vec (conj conds :or)) (first conds))))
    ([path comp]
     (when-not (or (nil? path) (not (seq path)) (nil? comp) (not (string? comp)))
       (let [path (if (coll? path) path (vector path))
             conds (reduce
                    #(conj %1
                           [:jsonb_path_exists :content [:cast (str "$." %2 ".** " (when-not (str/blank? comp) "?") comp) :jsonpath]])
                    [] path)]
         (if (> (count path) 1) (vec (conj conds :or)) (first conds))))))

  :.)

