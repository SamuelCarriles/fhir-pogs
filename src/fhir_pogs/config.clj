(ns fhir-pogs.config
  (:require [config.dotenv :as dotenv]
            [config.core :as cfg]))

(def env-struct
  [[:jdbc-url {:env "DATABASE_URL" :of-type :cfg/url}]
   [:test-jdbc-url {:env "TEST_DATABASE_URL" :of-type :cfg/url}]
   [:table-prefix {:env "TABLE_PREFIX" :of-type :cfg/string}]])

(def env
  (-> {}
      (cfg/patch (dotenv/parse ".env")
                 env-struct)))

env