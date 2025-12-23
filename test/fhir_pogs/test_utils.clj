(ns fhir-pogs.test-utils
  (:require [fhir-pogs.db :as db]
            [honey.sql.helpers :as help]
            [honey.sql :as sql]))

(defn drop-all-tables!
  "Drop all tables. ONLY for testing. DO NOT use in production."
  [connectable]
  (let [tables (db/get-tables connectable)]
    (when (seq tables)
      (db/execute! connectable
                   (-> (apply help/drop-table tables)
                       sql/format)))))