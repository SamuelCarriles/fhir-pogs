(ns fhir-pogs.search.migrations
  (:require [migratus.core :as mig]))

(def config {:store :database
             :migration-dir "db/migrations/"})

(defn migrate!
  "Apply all pending migrations"
  [db-spec]
  (mig/migrate (assoc config :db db-spec)))

(defn rollback!
  "Roll back the last applied migration"
  [db-spec]
  (mig/rollback (assoc config :db db-spec)))

(defn create!
  "Create a new migration"
  [name]
  (mig/create config name))

(defn pending
  "List all pending migrations"
  [db-spec]
  (mig/pending-list (assoc config :db db-spec)))

