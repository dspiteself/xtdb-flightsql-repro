;; Bug 3: in-process emulation of XtdbProducer.streamArrowReader (no gRPC), many concurrent readers.
;; Args: seconds readers shared-conn? mode. Mode no-vsr checks each unloaded buffer the way ArrowMessage.asInputStream does.
;; Run from the repo root: java --add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -cp "$(clojure -Spath)" clojure.main streamrace3.clj 15 8 false no-vsr
(require '[xtdb.api :as xt] '[xtdb.node :as xtn] '[xtdb.vector.writer :as vw])
(import '(java.util.concurrent Executors Callable TimeUnit) '(java.util.concurrent.atomic AtomicLong)
        '(org.apache.arrow.vector VectorSchemaRoot VectorLoader VectorUnloader) '(org.apache.arrow.memory RootAllocator))
(load-file "stress-data.clj")
(defn serialize-check [^VectorSchemaRoot vsr]
  ;; what ArrowMessage.asInputStream does with each buffer of the batch
  (with-open [rb (.getRecordBatch (VectorUnloader. vsr))]
    (doseq [b (.getBuffers rb)] (.nioBuffer ^org.apache.arrow.memory.ArrowBuf b))))
(let [[secs nr shared mode] *command-line-args* secs (parse-long secs) nr (parse-long nr) shared? (= "true" shared)
      deadline (+ (System/currentTimeMillis) (* 1000 secs))
      reads (AtomicLong.) bad (AtomicLong.) msgs (java.util.concurrent.ConcurrentHashMap.)
      producer-al nil]
  (with-open [node (xtn/start-node {})] (let [producer-al (.newChildAllocator (.getAllocator node) "flight-sql" 0 Long/MAX_VALUE)]
    (doseq [i (range 200)] (xt/execute-tx node [[:put-docs :observation (obs (str "bg" i) i :component? (zero? (mod i 3)))]]))
    (xt/execute-tx node [[:put-docs :observation (obs "target" 0)]])
    (let [conns (vec (repeatedly nr #(.connect node)))
          pool (Executors/newFixedThreadPool nr)]
      (dotimes [t nr]
        (.submit pool ^Callable
                 (fn [] (while (< (System/currentTimeMillis) deadline)
                          (try (with-open [stmt (.createStatement (if shared? (conns t) (.connect node)) "SELECT *, _system_from FROM observation WHERE _id = ?")]
                                 (.prepare stmt)
                                 (with-open [r (vw/open-args (.getAllocator node) ["target"])] (.bind stmt r))
                                 (with-open [rdr (.getReader (.executeQuery stmt))
                                             vsr (VectorSchemaRoot/create (.getSchema (.getVectorSchemaRoot rdr))
                                                                          (if (= mode "own-al") (.newChildAllocator (.getAllocator node) "own" 0 Long/MAX_VALUE) producer-al))]
                                   (let [loader (VectorLoader. vsr)]
                                     (while (.loadNextBatch rdr)
                                       (with-open [rb (.getRecordBatch (VectorUnloader. (.getVectorSchemaRoot rdr)))]
                                         (if (= mode "no-vsr")
                                           (doseq [b (.getBuffers rb)]
                                             (try (.nioBuffer ^org.apache.arrow.memory.ArrowBuf b)
                                                  (catch IndexOutOfBoundsException e
                                                    (.putIfAbsent msgs (str "FAILING BUF capacity=" (.capacity b) " writerIndex=" (.writerIndex b)
                                                                            " refmgr=" (.getSimpleName (class (.getReferenceManager b)))
                                                                            " node-empty?=" (identical? b (.getEmpty (.getAllocator node)))) true)
                                                    (throw e))))
                                           (do (.load loader rb) (serialize-check vsr))))))))
                               (.incrementAndGet reads)
                               (catch Throwable e (.incrementAndGet bad) (.putIfAbsent msgs (subs (str (class e) " " (ex-message e)) 0 (min 140 (count (str (class e) " " (ex-message e))))) true)))))))
      (.shutdown pool) (.awaitTermination pool 1 TimeUnit/HOURS)
      (println "readers" nr "mode" mode "reads" (.get reads) "bad" (.get bad))
      (doseq [m (take 4 (keys msgs))] (println "  " m))))))
(shutdown-agents) (System/exit 0)
