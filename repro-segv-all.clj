;; Bug 2, silent variant: one node, 24 tables, each written with a different key order of the same 4-field struct.
;; Only the first order seen in the JVM reads back correctly over FlightSQL; the rest return other fields' bytes.
;; Run from the repo root: java --add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -cp "$(clojure -Spath)" clojure.main repro-segv-all.clj
(require '[xtdb.api :as xt] '[xtdb.node :as xtn] '[xtdb.flight-sql :as fsql])
(defn perms [xs] (if (empty? xs) [[]] (for [x xs p (perms (remove #{x} xs))] (cons x p))))
(def orders (into (sorted-map) (map-indexed (fn [i p] [(str "p" (format "%02d" i) "-" (apply str (map first p))) (vec p)]) (perms ["value" "unit" "system" "code"]))))
(def vals {"value" 60.0 "unit" "/min" "system" "http://unitsofmeasure.org" "code" "/min"})
(with-open [node (xtn/start-node {:flight-sql {:port 0}})
            fs (fsql/open-conn {:port (.getFlightSqlPort node)})]
  (doseq [[label ks] orders]
    (let [table (str "obs_" (subs label 1 3))
          vq (apply array-map (mapcat (fn [k] [k (vals k)]) ks))]
      (xt/execute-tx node [[:sql (str "INSERT INTO " table " (_id, vq) VALUES (?, ?)") ["o1" vq]]])
      (println (format "%-12s node  :" label) (pr-str (:vq (first (xt/q node [(str "SELECT * FROM " table)])))))
      (flush)
      (println (format "%-12s flight:" label)
               (try (pr-str (:vq (first (xt/q fs [(str "SELECT * FROM " table)]))))
                    (catch Throwable e (str "ERROR " (ex-message e)))))
      (flush))))
(shutdown-agents)
(System/exit 0)
