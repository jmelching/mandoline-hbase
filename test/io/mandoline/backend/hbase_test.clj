(ns io.mandoline.backend.hbase-test
  (:require
    [clojure.test :refer :all]
    [io.mandoline.backend.hbase :as hbase]
    [io.mandoline.chunk :refer [generate-id]]
    [io.mandoline.impl :refer [mk-schema]]
    [io.mandoline.test.utils :refer :all]
    [io.mandoline.test
     [concurrency :refer [lots-of-overlaps
                          lots-of-processes
                          lots-of-tiny-slices]]
     [entire-flow :refer [entire-flow]]
     [failed-ingest :refer [failed-write]]
     [grow :refer [grow-dataset]]
     [linear-versions :refer [linear-versions]]
     [nan :refer [fill-double
                  fill-float
                  fill-short]]
     [overwrite :refer [overwrite-dataset
                        overwrite-extend-dataset]]
     [scalar :refer [write-scalar]]
     [shrink :refer [shrink-dataset]]]
    [io.mandoline.test.protocol
     [chunk-store :as chunk-store]
     [schema :as schema]])
  (:import
    [java.io File]
    [java.nio ByteBuffer]
    [java.util UUID]
    [org.apache.commons.codec.digest DigestUtils]
    [com.google.common.io Files]))

(def ^:private test-root-base
  "integration-testing.mandoline.io")

(def ^:private test-dataset-name "test-dataset")

(defn- setup
  "Create a random store spec for testing the HBase Mandoline
  backend.

  This function is intended to be used with the matching teardown
  function."
  []
  (let [root (format "%s.%s" (random-name) test-root-base)
        dataset test-dataset-name]
    {:store "io.mandoline.backend.hbase/mk-schema"
     :root root
     :dataset dataset}))

(defn teardown
  [store-spec]
  (let [s (hbase/mk-schema store-spec)]
    (with-test-out
      (println
        "Please wait for post-test cleanup of HBase tables."
        "This may take a while.")
      (try
        ;(.destroy-dataset s test-dataset-name)
        ; Swallow any exception after making a best effort
        (catch Exception e
          (println
            (format "Failed to destroy the dataset: %s %s" test-dataset-name e)))))))


(deftest test-hbase-schema-properties
          (let [store-specs (atom {}) ; map of Schema instance -> store-spec
                setup-schema (fn []
                               (let [store-spec (setup)
                                     s (hbase/mk-schema store-spec)]
                                 (swap! store-specs assoc s store-spec)
                                 s))
                teardown-schema (fn [s]
                                  (let [store-spec (@store-specs s)]
                                    (teardown store-spec)))
                num-datasets 1]
            (schema/test-schema-properties-single-threaded
              setup-schema teardown-schema num-datasets)
            ;(schema/test-schema-properties-multi-threaded
            ;  setup-schema teardown-schema num-datasets)
            ))

(deftest test-hbase-chunk-store-properties
          (let [store-specs (atom {}) ; map of ChunkStore instance -> store-spec
                setup-chunk-store (fn []
                                    (let [store-spec (setup)
                                          dataset (:dataset store-spec)
                                          c (-> (doto (hbase/mk-schema store-spec)
                                                  (.create-dataset dataset))
                                                (.connect dataset)
                                                (.chunk-store {}))]
                                      (swap! store-specs assoc c store-spec)
                                      c))
                teardown-chunk-store (fn [c]
                                       (let [store-spec (@store-specs c)]
                                         (teardown store-spec)))
                num-chunks 10]
            (chunk-store/test-chunk-store-properties-single-threaded
              setup-chunk-store teardown-chunk-store num-chunks)
            ;(chunk-store/test-chunk-store-properties-multi-threaded
            ;  setup-chunk-store teardown-chunk-store num-chunks)
            ))

; TODO add list tables test

;(deftest ^:integration hbase-entire-flow
;  (with-and-without-caches
;    (entire-flow setup teardown)))
;
;(deftest ^:integration sqlite-grow-dataset
;  (with-and-without-caches
;    (grow-dataset setup teardown)))
;
;(deftest ^:integration hbase-shrink-dataset
;  (with-and-without-caches
;    (shrink-dataset setup teardown)))
;
;(deftest ^:integration hbase-overwrite-dataset
;  (with-and-without-caches
;    (overwrite-dataset setup teardown)))
;
;(deftest ^:integration hbase-overwrite-extend-dataset
;  (with-and-without-caches
;    (overwrite-extend-dataset setup teardown)))
;
(deftest ^:integration hbase-linear-versions
  (with-and-without-caches
    (linear-versions setup teardown)))
;
(deftest ^:integration hbase-write-scalar
  (with-and-without-caches
    (write-scalar setup teardown)))
;
;(deftest ^:integration sqlite-lots-of-processes-ordered
;  (lots-of-processes setup teardown false))
;
;(deftest ^:integration sqlite-lots-of-processes-misordered
;  (lots-of-processes setup teardown true))
;
;(deftest ^:integration sqlite-lots-of-tiny-slices
;  (with-and-without-caches
;    (lots-of-tiny-slices setup teardown)))
;
;(deftest ^:integration sqlite-write-fail-write
;  (with-and-without-caches
;    (failed-write setup teardown)))
;
;(deftest ^:integration sqlite-lots-of-overlaps
;  (with-and-without-caches
;    (lots-of-overlaps setup teardown)))
;
;(deftest ^:integration sqlite-nan-fill-values
;  (with-and-without-caches
;    (fill-double setup teardown)
;    (fill-float setup teardown)
;    (fill-short setup teardown)))
;
;(deftest ^:experimental sqlite-chunk-store-write-benchmark
;  ; WARNING: Running this test sometimes triggeres SIGSEGV
;  ; # Problematic frame
;  ; # C  [sqlite-3.7.2-libsqlitejdbc.so+0xe242]  short+0xbb
;  (with-test-out
;    (testing "Benchmarking concurrent writes to SQLiteChunkStore"
;      (println (testing-contexts-str))
;      (with-temp-db store-spec setup teardown
;                    (let [chunk-store (-> (mk-schema store-spec)
;                                          (.connect (:dataset store-spec))
;                                          (.chunk-store {}))
;                          random (java.util.Random.)
;                          chunks (doall
;                                   (for [i (range 100)
;                                         :let [bytes (ByteBuffer/wrap
;                                                       (let [ba (byte-array 64000)]
;                                                         (.nextBytes random ba)
;                                                         ba))]]
;                                     {:bytes bytes
;                                      :hash (DigestUtils/shaHex (.array bytes))
;                                      :ref-count (rand-int 10)}))
;                          write! (fn [{:keys [hash ref-count bytes]}]
;                                   (.write-chunk chunk-store hash ref-count bytes))
;                          statistics (benchmark
;                                       (doall (pmap write! chunks))
;                                       nil)
;                          upper-quantile (first (:upper-q statistics))
;                          threshold 1.0] ; threshold in seconds
;                      (is (< upper-quantile threshold)
;                          (format
;                            "%f quantile execution time ought to be < %f seconds"
;                            (- 1.0 (:tail-quantile statistics))
;                            threshold))
;                      (report-result statistics))))))
