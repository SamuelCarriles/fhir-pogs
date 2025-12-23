(ns fhir-pogs.config
  (:require [config.dotenv :as dotenv]
            [config.core :as cfg]
            [fhir-pogs.db :as db]))

(def env-struct
  [[:jdbc-url {:env "DATABASE_URL" :of-type :cfg/url}]
   [:test-jdbc-url {:env "TEST_DATABASE_URL" :of-type :cfg/url}]])

(defonce config
  (-> {}
      (cfg/patch (dotenv/parse ".env")
                 env-struct)))

(defonce datasource
  (when-let [url (:jdbc-url config)]
    (db/create-datasource url)))

(defonce test-datasource
  (when-let [url (:test-jdbc-url config)]
    (db/create-datasource url)))