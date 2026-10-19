;; Drives a real XTDB ResultCursor the way Xtdb.Connection.toArrowReader does (buggy), and with
;; each page first copied into a Relation built from the declared result schema (proposed fix).
;; Run from the repo root: java --add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -cp "$(clojure -Spath)" clojure.main proof-twodb.clj   (MODES=fixed,fixed-direct to skip the crashing buggy load)
(require '[xtdb.util] '[xtdb.serde] '[xtdb.api :as xt] '[xtdb.node :as xtn] '[xtdb.vector.writer :as vw])
(import '(org.apache.arrow.vector VectorLoader VectorSchemaRoot)
        '(org.apache.arrow.vector.types.pojo Schema)
        '(xtdb.arrow Relation RelationReader ArrowTypes VectorType)
        '(xtdb.api ResultCursor))

(defn schema-of [^ResultCursor cursor]
  (Schema. (for [[n ^VectorType t] (.getResultTypes cursor)]
             (ArrowTypes/withUnionTypeIds (.toField t n)))))

(def ^:dynamic *db* nil)
(defn drain [node sql args mode]
  (with-open [conn (if *db* (.connect node *db*) (.connect node))
              stmt (.createStatement conn sql)]
    (.prepare stmt)
    (when (seq args) (with-open [r (vw/open-args (.getAllocator conn) args)] (.bind stmt r)))
    (with-open [cursor (.openQuery stmt)]
      (let [al (.getAllocator conn)
            schema (schema-of cursor)
            out (atom [])]
        (with-open [root (VectorSchemaRoot/create schema al)]
          (while (.tryAdvance cursor
                   (fn [^RelationReader in-rel]
                     (if (= mode :fixed-direct)
                       (with-open [conformed (Relation. al schema)]
                         (.copyRange (.rowCopier in-rel conformed) 0 (.getRowCount in-rel))
                         (with-open [rb (.openArrowRecordBatch conformed)]
                           (.load (VectorLoader. root) rb)))
                     (with-open [page (.openDirectSlice in-rel al)]
                       (case mode
                         :buggy (with-open [rb (.openArrowRecordBatch page)]
                                  (.load (VectorLoader. root) rb))
                         :fixed (with-open [conformed (Relation. al schema)]
                                  (.copyRange (.rowCopier page conformed) 0 (.getRowCount page))
                                  (with-open [rb (.openArrowRecordBatch conformed)]
                                    (.load (VectorLoader. root) rb))))))
                     (with-open [rel (Relation/fromRoot al root)]
                       (swap! out into (map xtdb.util/->clj) (.toMaps rel (xtdb.serde/read-key-fn :kebab-case-keyword)))))))
          @out)))))


(require '[xtdb.flight-sql :as fsql])
(def vq {"value" 60.0 "unit" "/min" "system" "http://unitsofmeasure.org" "code" "/min"})
(defn ordered [ks] (apply array-map (mapcat (fn [k] [k (vq k)]) ks)))
(with-open [node (xtn/start-node {:server {:port 0}})]
  (with-open [jc (.getConnection (xt/client {:host "127.0.0.1" :port (.getServerPort node) :dbname "xtdb"}))]
    (doseq [db ["a" "b"]] (.execute (.createStatement jc) (str "ATTACH DATABASE " db " WITH $$\nlog: !InMemory\nstorage: !InMemory\n$$"))))
  (doseq [[db ks] [["a" ["system" "unit" "value" "code"]] ["b" ["value" "unit" "system" "code"]]]]
    (let [pg (xt/client {:host "127.0.0.1" :port (.getServerPort node) :dbname db})]
      (with-open [jc (.getConnection pg)] nil)
      (with-open [c (.connect node db)] nil)
      (xt/execute-tx (.connect node db) [[:sql "INSERT INTO obs (_id, vq) VALUES (?, ?)" ["o1" (ordered ks)]]])
      (doseq [mode (map keyword (clojure.string/split (or (System/getenv "MODES") "buggy,fixed,fixed-direct") #","))]
        (binding [*db* db]
          (let [rows (try (drain node "SELECT * FROM obs WHERE _id = ?" ["o1"] mode) (catch Throwable e [(ex-message e)]))
                expect (xt/q pg ["SELECT * FROM obs WHERE _id = ?" "o1"])]
            (println "db" db (format "%-13s" mode) "equal to pgwire:" (= (set (map :vq rows)) (set (map :vq expect))) (pr-str (map :vq rows)))))))))
(shutdown-agents)
(System/exit 0)
