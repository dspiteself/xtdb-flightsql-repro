;; Drives a real XTDB ResultCursor the way Xtdb.Connection.toArrowReader does (buggy), and with
;; each page first copied into a Relation built from the declared result schema (proposed fix).
;; Run from the repo root: java --add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -cp "$(clojure -Spath)" clojure.main proof.clj
(require '[xtdb.util] '[xtdb.serde] '[xtdb.api :as xt] '[xtdb.node :as xtn] '[xtdb.vector.writer :as vw])
(import '(org.apache.arrow.vector VectorLoader VectorSchemaRoot)
        '(org.apache.arrow.vector.types.pojo Schema)
        '(xtdb.arrow Relation RelationReader ArrowTypes VectorType)
        '(xtdb.api ResultCursor))

(defn schema-of [^ResultCursor cursor]
  (Schema. (for [[n ^VectorType t] (.getResultTypes cursor)]
             (ArrowTypes/withUnionTypeIds (.toField t n)))))

(defn drain [node sql args mode]
  (with-open [conn (.connect node)
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

(defn load! [node table flushed live]
  (doseq [i (range 50)]
    (xt/execute-tx node [[:sql (str "INSERT INTO " table " RECORDS " (flushed (str "f" i)))]]))
  (doseq [i (range 3)]
    (xt/execute-tx node [[:sql (str "INSERT INTO " table " RECORDS " (live (str "l" i)))]])))

;; one node per case: rows-per-block counts rows across all tables
(def cases
  [["struct field only in flushed rows" "obs"
    #(str "{_id: '" % "', vq: {value: 1.0, unit: 'mg'}, codes: [{reference: 'r', display: 'd'}]}")
    #(str "{_id: '" % "', vq: {value: 2.0}, codes: [{reference: 'r'}]}")]
   ["scalar long flushed, string live" "poly" #(str "{_id: '" % "', v: 1}") #(str "{_id: '" % "', v: 'str'}")]
   ["top-level column only in flushed rows" "flushedonly" #(str "{_id: '" % "', a: 1, extra: 'x'}") #(str "{_id: '" % "', a: 2}")]
   ["struct in list in struct" "deep" #(str "{_id: '" % "', r: {items: [{c: {code: 'x', sys: 's'}, n: 1}]}}")
    #(str "{_id: '" % "', r: {items: [{c: {code: 'y'}}]}}")]
   ["struct flushed, string live" "polystruct" #(str "{_id: '" % "', p: {x: 1}}") #(str "{_id: '" % "', p: 'flat'}")]
   ["list of long flushed, mixed list live" "listvar" #(str "{_id: '" % "', xs: [1, 2]}") #(str "{_id: '" % "', xs: ['a', {k: 1}]}")]])

(doseq [[label table flushed live] cases]
  (with-open [node (xtn/start-node {:indexer {:rows-per-block 20}})]
    (load! node table flushed live)
    (println "==" label)
    (doseq [[sql args] [[(str "SELECT * FROM " table " WHERE _id = ?") ["l1"]]
                        [(str "SELECT * FROM " table " WHERE _id = ?") ["f1"]]
                        [(str "SELECT * FROM " table) []]
                        [(str "SELECT *, _system_from FROM " table " FOR ALL SYSTEM_TIME") []]]
            mode [:buggy :fixed :fixed-direct]]
      (try
        (let [rows (drain node sql args mode)
              expect (xt/q node (into [sql] args))]
          (println mode sql args "=>" (count rows) "rows; equal to xt/q:" (= (set rows) (set expect))
                   (when (= 1 (count rows)) (pr-str (first rows))) (when-not (= (set rows) (set expect)) (pr-str (take 2 expect)))))
        (catch Throwable e (println mode sql args "FAILED:" (ex-message e)))))))
(shutdown-agents)
(System/exit 0)
