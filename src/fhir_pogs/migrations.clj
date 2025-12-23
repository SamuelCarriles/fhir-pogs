(ns fhir-pogs.migrations
  (:require [migratus.core :as mig]))

(def config {:store :database
             :migration-dir "db/migrations/"})

(defn migrate!
  "Apply all pending migrations"
  [connectable]
  (mig/migrate (assoc config :db connectable)))

(defn rollback!
  "Roll back the last applied migration"
  [connectable]
  (mig/rollback (assoc config :db connectable)))

(defn create!
  "Create a new migration"
  [name]
  (mig/create config name))

(defn pending
  "List all pending migrations"
  [connectable]
  (mig/pending-list (assoc config :db connectable)))

