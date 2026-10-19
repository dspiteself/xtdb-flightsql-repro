;; Bug 1: FlightSQL cannot stream a page whose shape differs from the declared result type.
;; Rows flushed to a block have vq.unit; the live row does not. The node returns the live row; FlightSQL fails.
;; Run from the repo root: java --add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -cp "$(clojure -Spath)" clojure.main repro-minimal.clj
(require '[xtdb.api :as xt] '[xtdb.node :as xtn] '[xtdb.flight-sql :as fsql])
(with-open [node (xtn/start-node {:server {:port 0} :flight-sql {:port 0} :indexer {:rows-per-block 20}})
            fs (fsql/open-conn {:port (.getFlightSqlPort node)})]
  (doseq [i (range 50)] ; flushed to blocks: vq has a unit
    (xt/execute-tx node [[:sql (str "INSERT INTO obs RECORDS {_id: 'f" i "', vq: {value: 1.0, unit: 'mg'}}")]]))
  (xt/execute-tx node [[:sql "INSERT INTO obs RECORDS {_id: 'live', vq: {value: 2.0}}"]]) ; live: no unit
  (println "node    :" (xt/q node ["SELECT * FROM obs WHERE _id = 'live'"]))
  (println "flight  :" (try (xt/q fs ["SELECT * FROM obs WHERE _id = 'live'"]) (catch Exception e (ex-message e)))))
(shutdown-agents)
(System/exit 0)
