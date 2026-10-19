;; Bug 2: two separate nodes in one JVM. Node 1 writes a struct with one key order;
;; node 2 (different table, different SQL text) writes the same fields in another order.
;; Reading node 2 over FlightSQL gets another field's buffers, and the client JVM crashes (SIGSEGV).
;; Run from the repo root: java --add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -cp "$(clojure -Spath)" clojure.main perm-jvm.clj
(require '[xtdb.api :as xt] '[xtdb.node :as xtn] '[xtdb.flight-sql :as fsql])
(def vq {"value" 60.0 "unit" "UNIT" "system" "SYSTEM" "code" "CODE"})
(defn ordered [ks] (apply array-map (mapcat (fn [k] [k (vq k)]) ks)))
(doseq [[table order select] [["first_table" ["system" "unit" "value" "code"] "SELECT * FROM first_table"]
                              ["other_table" ["value" "unit" "system" "code"] "SELECT _id, q FROM other_table WHERE TRUE"]]]
  (with-open [node (xtn/start-node {:flight-sql {:port 0}})
              fs (fsql/open-conn {:port (.getFlightSqlPort node)})]
    (xt/execute-tx node [[:sql (str "INSERT INTO " table " (_id, q) VALUES (?, ?)") ["o1" (ordered order)]]])
    (println table "node  :" (pr-str (:q (first (xt/q node [select])))))
    (flush)
    (println table "flight:" (pr-str (:q (first (xt/q fs [select])))))
    (flush)))
(shutdown-agents)
(System/exit 0)
