(ns autorest.core
  (:require [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.java.jdbc :as sql]
            [polaris.core :as polaris]
            ring.adapter.jetty))

;; Data
(def defaults ["localhost" 5432 "addressbook"])

(def db-spec {:subprotocol "postgresql"
              :subname "//localhost:5432/addressbook"})

(defn get-tables [db-spec]
  (let [query "SELECT table_name
                 FROM information_schema.tables
                WHERE table_schema='public'"]
    (sql/query db-spec query)))

(defn get-columns
  [db-spec table]
  (let [query ["SELECT column_name
                  FROM information_schema.columns
                 WHERE table_name=?"
               table]]
    (sql/query db-spec query)))

;; Controllers
(defprotocol IUrl
  (url [this] "Returns the url for the given resource"))

(defrecord Resource [name]
  IUrl
  (url [_] (str "http://localhost:3000/" name)))

(defn envelope-error [error]
  {:error error})

(defn envelope-result [result]
  {:result result})

(defn response
  ([] (response ""))
  ([data] (response 200 data))
  ([status data]
   (let [envelope (if (< status 400) envelope-result envelope-error)]
     {:body (json/generate-string (envelope data))
      :status status
      :headers {"Content-Type" "application/json"}})))

(defn not-implemented []
  (response 501 "Not Implemented"))

(defn base-handler
  [request]
  (condp = (:request-method request)
    :get (try (let [results (->> (get-tables db-spec)
                              (map :table_name)
                              (map ->Resource)
                              (map (fn [r] {:name (:name r) :location (.url r)}))
                              vec)]
                (response {:resources results}))
           (catch org.postgresql.util.PSQLException e
             (response 503 (.getMessage e))))
    (not-implemented)))

(defn valid-table? [table]
  (let [tables (map :table_name (get-tables db-spec))]
    (some #{table} tables)))

(defn table-handler
  [request]
  (condp = (:request-method request)
    :get
    (let [table (-> request :params :table)]
      (if-not (valid-table? table)
        (response 404 (str table " is not a valid resource."))
        (let [columns (->> table
                        (get-columns db-spec)
                        (map :column_name))]
          (if-let [resource-id (-> request :params :id)]
            ;; Get one row
            (let [query [(format "SELECT %s FROM %s WHERE id=?"
                                 (string/join ", " columns)
                                 table)
                         (Integer/parseInt resource-id)]
                  obj (first (sql/query db-spec query))]
              (if-not (nil? obj)
                (response obj)
                (response 404 "Not Found")))
            ;; Get all rows
            (let [query (format "SELECT %s FROM %s" (string/join ", " columns) table)
                  result (sql/query db-spec query)]
              (response (vec result)))))))
    (not-implemented)))

(defn echo-handler [request]
  {:body (str request)
   :status 200
   :headers {"Content-Type" "text/plain"}})

;; Routes
(def route-definitions
  [["" :index base-handler]
   ["/echo" :echo echo-handler]
   ["/:table" :table table-handler
    ["/:id" :table-item table-handler]]])

(def routes (polaris/build-routes route-definitions))

;; Ring main handler
(def handler (polaris/router routes))

;; Main handler
(defn -main []
  (ring.adapter.jetty/run-jetty handler {:port 3000 :join? false}))
