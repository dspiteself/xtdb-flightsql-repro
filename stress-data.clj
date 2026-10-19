;; Observation-shaped documents with nested, non-nullable columns, used by streamrace3.clj.
(require '[xtdb.api :as xt])
(def zones ["-05:00" "Z" "-06:00"])
(defn zdt [i] (java.time.ZonedDateTime/parse (str "2026-01-01T00:00" (zones (mod i 3)))))
(defn coding [c] {:coding [{:system "http://loinc.org" :code c :display (str "d" c)}] :text (str "t" c)})
(defn obs [id i & {:keys [component?]}]
  (cond-> {:xt/id id :resourceType "Observation" :status (if (even? i) "final" "amended") :id id
           :category [(coding "vital")] :category_tokens ["vital"] :code (coding "8867-4") :code_tokens ["8867-4"]
           :subject {:reference "Patient/p1"} :effectiveDateTime (zdt i) :issued (zdt (inc i))
           :valueQuantity {:system "http://unitsofmeasure.org" :unit "/min" :value (if (even? i) (int (+ 60 i)) (+ 60.5 i)) :code "/min"}}
    component? (assoc :component [{:code (coding "c1") :valueQuantity {:system "s" :unit "mm" :value (if (even? i) 1.5 (int 2)) :code "mm"}}
                                  {:code (coding "c2") :valueCodeableConcept (coding "x")}]
                      :component_tokens [(coding "c1")])))
