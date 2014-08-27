(ns io.mandoline.backend.hbase
  (:import
    [java.nio ByteBuffer]
    [org.joda.time DateTime]
    [org.apache.hadoop.hbase.util Bytes]
    [org.apache.hadoop.hbase.filter RowFilter]
    [org.apache.hadoop.hbase.filter CompareFilter$CompareOp]
    [org.apache.hadoop.hbase.filter BinaryComparator]
    [org.apache.hadoop.hbase TableNotFoundException])
  (:require
    [clojure.core.memoize :as memo]
    [clojure.string :as string]
    [clojure.tools.logging :as log]
    [clojure-hbase.admin :as admin]
    [clojure-hbase.core :as hbase]
    [io.mandoline.utils :as utils]
    [io.mandoline.impl.protocol :as proto]))

;;;;;;;;;;;;;;;; HBase schema ;;;;;;;;;;;;;;;;
;;
;; Chunk:   {:k <chunk-id (string)>
;;           :r <ref count (number)>
;;           :v <data (bytes)>}
;; Index:   {:k <var-name(string)|coordinate(strings joined by /)|version-id(number)>
;;           :v <chunk id (string)>}
;; Version: { key =  <version-id (number)>
;;            :t <version-id (number)> ;This really can be removed because it is the same as the key
;;            :v <json (string)>}
;;
;;;;;;;;;;;;;;;;;; Table names ;;;;;;;;;;;;;;;;;;
;;
;; Chunk:   <namespace>.<dataset-name>.chunks
;; Index:   <namespace>.<dataset-name>.indices
;; Version: <namespace>.<dataset-name>.versions
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Result mappers
(defn- keywordize [x] (keyword (Bytes/toString x)))

(defn- result-mapper
  [result]
  (hbase/latest-as-map result :map-family keywordize
                       :map-qualifier keywordize
                       :map-value #(Bytes/toString %)))

(defn- chunk-mapper
  [result]
  (merge {:version-id (last (clojure.string/split (Bytes/toString (.getRow result)) #"\|"))}
         (hbase/latest-as-map result
                              :map-family keywordize
                              :map-value #(Bytes/toString %)
                              :map-qualifier keywordize)))

;; Schema related functions

(defn- get-table-name
  "Construct a table name from one or more table name components.
  Keywords are automatically converted to their string names.

  (get-table-name \"com.foo.dev.bar\" \"datasetname\"))
  => \"com.foo.dev.bar.datasetname\"

  (get-table-name \"com.foo.dev.bar\" :datasetname))
  => \"com.foo.dev.bar.datasetname\"
  "
  [head & more]
  (string/join \. (map name (cons head more))))

(defn- get-metadata [client-opts table version]

  (if-let [result (hbase/with-table [hbase-table (hbase/table (get-table-name table "versions"))]
                    (hbase/get hbase-table version :columns
                               [:D [:v :t]]))]
    (do
      (let [result-map (hbase/latest-as-map result
                                            :map-family keywordize
                                            :map-qualifier keywordize
                                            :map-value #(Bytes/toString %))]
        (get-in result-map [:D :v])))))

(defn- delete-table
  [client-opts table]
  (try
    (admin/disable-table table)
    (admin/delete-table table)
    :success
    (catch TableNotFoundException _
      :success)))

(defn- create-table
  [client-opts table hash-keydef & opts]
  (admin/create-table (admin/table-descriptor table :family (admin/column-descriptor :D))))

;; Chunk functions

(defn- read-a-chunk [client-opts table hash]
  (when (or (empty? hash) (not (string? hash)))
    (throw
      (IllegalArgumentException. "hash must be a non-empty string")))
  (log/debugf "Reading chunk %s" hash)
  (hbase/with-table [hbase-table (hbase/table (get-table-name table "chunks"))]
    (hbase/get hbase-table hash :columns [:D [:v :r]])))

(defn- read-chunk-ref [client-opts table hash]
  (when (or (empty? hash) (not (string? hash)))
    (throw
      (IllegalArgumentException. "hash must be a non-empty string")))
  (log/debugf "Reading chunk-ref %s" hash)
  (if-let [result (hbase/with-table [hbase-table (hbase/table (get-table-name table "chunks"))]
                    (hbase/get hbase-table hash :column [:D :r]))]
    (do
      (let [result-map (hbase/latest-as-map result
                                            :map-family keywordize
                                            :map-qualifier keywordize
                                            :map-value #(Bytes/toLong %))]
        (get-in result-map [:D :r])))
    (throw
      (IllegalArgumentException.
        (format "No reference count was found for hash %s" hash)))))

(deftype HBaseChunkStore [table client-opts]
  proto/ChunkStore
  (read-chunk [_ hash]
    (if-let [item (read-a-chunk client-opts table hash)]
      (do
        (let [result-map (hbase/latest-as-map item
                                              :map-family keywordize
                                              :map-qualifier keywordize)]
          (java.nio.ByteBuffer/wrap (get-in result-map [:D :v]))))
      (throw
        (IllegalArgumentException.
          (format "No chunk was found for hash %s" hash)))))

  (chunk-refs [_ hash]
    (read-chunk-ref client-opts table hash))

  (write-chunk [_ hash ref-count bytes]
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (when-not (integer? ref-count)
      (throw
        (IllegalArgumentException. "ref-count must be an integer")))
    (when-not (instance? ByteBuffer bytes)
      (throw
        (IllegalArgumentException. "bytes must be a ByteBuffer instance")))
    (when-not (pos? (.remaining bytes))
      (throw
        (IllegalArgumentException. "Chunk has no remaining bytes")))
    (hbase/with-table [hbase-table (hbase/table (get-table-name table "chunks"))]
      (hbase/put hbase-table hash :values [:D [:r ref-count
                                               :v bytes]]))
    nil)

  (update-chunk-refs [_ hash delta]
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (when-not (integer? delta)
      (throw
        (IllegalArgumentException. "delta must be an integer")))
    (let [ref-count (read-chunk-ref client-opts table hash)]
      (when-not ref-count
        (throw
          (IllegalArgumentException.
            (format "No chunk was found for hash %s" hash))))
      (log/debugf
        "Adding %d to reference count for chunk with hash: %s" delta hash)
      (hbase/with-table [hbase-table (hbase/table (get-table-name table "chunks"))]
        (hbase/put hbase-table hash :value [:D :r (+ delta ref-count)])))
    nil))
;))

;; Index functions
(defn- coordinate->id [coord]
  (if (empty? coord)
    "_"
    (->> coord (interpose "/") (apply str))))

(defn- coordinate->key [metadata var-name coord]
  (str (name var-name) "|" (coordinate->id coord) "|" (:version-id metadata)))

(defn- find-index
  [var-name coordinate version-cache client-opts table version-id next-version]
  (let [rowkey (str (name var-name) "|" (coordinate->id coordinate) "|")]
    (if-let [results (-> (hbase/with-table [hbase-table (hbase/table (get-table-name table "indices"))]
                           (hbase/with-scanner [scan-results (hbase/scan hbase-table
                                                                         :columns [:D [:v]]
                                                                         :filter (RowFilter. (CompareFilter$CompareOp/LESS_OR_EQUAL) (BinaryComparator. (Bytes/toBytes (str rowkey next-version))))
                                                                         :start-row rowkey
                                                                         :stop-row (str rowkey "a"))]
                             (doall (map chunk-mapper (-> scan-results .iterator iterator-seq))))))]
      (let [transformed-results (reduce merge (map (fn [x] {(read-string (:version-id x)) (get-in x [:D :v])}) results))
            result (get transformed-results version-id)]
        (if result
          result
          (get transformed-results (-> (keys transformed-results) sort reverse first)))))))

(deftype HBaseIndex [table client-opts var-name metadata version-cache]
  ;; table is a dataset-level table name
  proto/Index

  (target [_]
    (log/debugf "HBaseIndex target method called: metadata %s, var-name %s " metadata var-name)
    {:metadata metadata :var-name var-name})


  (chunk-at [_ coordinate]
    (find-index
      var-name coordinate
      version-cache client-opts table
      (:version-id metadata)
      (:version-id metadata)))

  ; TODO use find-index
  (chunk-at [_ coordinate version-id]
    (log/debugf "HBaseIndex chunk-at version-id method called: coordinate: %s ,version-id: %s"
                coordinate version-id)
    (let [key (coordinate->key {:version-id version-id} var-name coordinate)
          result (hbase/with-table [hbase-table (hbase/table (get-table-name table "indices"))]
                   (hbase/get hbase-table key :columns
                              [:D [:v]]))]
      (do
        (let [result-map (hbase/latest-as-map result
                                              :map-family keywordize
                                              :map-qualifier keywordize)]
          (get-in result-map [:D :v])))))

  (write-index [_ coordinate old-hash new-hash]
    (log/debugf "HBaseIndex write-index method called: coordinate: %s, old-hash: %s, new-hash: %s"
                coordinate old-hash new-hash)
    (let [key (coordinate->key metadata var-name coordinate)
          put (hbase/put* key :values [:D [:k key
                                           :v new-hash]])]
      (when (not= "" (:version-id metadata))
        (log/debugf "writing index at coordinate: %s" (pr-str coordinate))
        (try
          (hbase/with-table [hbase-table (hbase/table (get-table-name table "indices"))]
            (hbase/check-and-put hbase-table key
                                 :D :v (if (nil? old-hash)
                                         nil
                                         old-hash)
                                 put))
          true
          (catch Exception _
            false)))))

  (flush-index [_]
    (log/infof "HBaseIndex flush method called: no-op")))


(deftype HBaseConnection [table client-opts]
  proto/Connection

  (index [this var-name metadata options]
    (->HBaseIndex table client-opts var-name metadata
                  (utils/mk-version-cache
                    (map #(-> % :version Long/parseLong)
                         (proto/versions this {:metadata? false})))))

  (write-version [_ metadata]
    (log/debugf "HBaseConnection write-version method called with metadata: %s" (:version-id metadata))
    (hbase/with-table [hbase-table (hbase/table (get-table-name table "versions"))]
      (hbase/put hbase-table (str (:version-id metadata))
                 :values [:D [:t (str (:version-id metadata)) ;TODO remove this once i figure out key only
                              :v (utils/generate-metadata metadata)]])))

  (chunk-store [_ options]
    (->HBaseChunkStore table client-opts))

  (get-stats [_]
    {:metadata-size (admin/get-table-descriptor (get-table-name table "versions"))
     :index-size    (admin/get-table-descriptor (get-table-name table "indices"))
     :data-size     (admin/get-table-descriptor (get-table-name table "chunks"))})

  (metadata [_ version]
    (log/debugf "HBaseConnection metadata method called: version %s" version)
    (-> (get-metadata client-opts table version)
        (utils/parse-metadata true)))

  (versions [_ {:keys [limit metadata?]}]
    (log/debugf "HBaseConnection versions method called: metadata: %s, limit: %d " metadata? limit)
    (hbase/with-table [hbase-table (hbase/table (get-table-name table "versions"))]
      (hbase/with-scanner [scan-results (hbase/scan hbase-table :columns [:D (if metadata? [:t :v] [:t])])]
        (let [limit-fn (if limit #(take limit %) #(identity %))
              result (doall (map result-mapper
                                 (-> scan-results .iterator iterator-seq)))
              versions (for [r result]
                         (do
                           (merge
                             {:timestamp (DateTime. (read-string (get-in r [:D :t])))
                              :version   (str (get-in r [:D :t]))}
                             (when metadata? {:metadata (utils/parse-metadata (get-in r [:D :v]) true)})
                             )))]
          (->> versions reverse limit-fn))))))

(deftype HBaseSchema [root-table client-opts]
  proto/Schema

  (create-dataset [_ name]
    (when-not (and (string? name) (not (string/blank? name)))
      (throw
        (IllegalArgumentException.
          "dataset name must be a non-empty string")))
    (let [root-path (get-table-name root-table name)]
      (create-table client-opts (get-table-name root-path "chunks") [:k :s])
      (create-table client-opts (get-table-name root-path "indices") [:k :s])
      (create-table client-opts (get-table-name root-path "versions") [:k :s]))
    nil)

  (destroy-dataset [_ name]
    (doseq [t ["versions" "indices" "chunks"]]
      (->> (get-table-name root-table name t)
           (delete-table client-opts))))

  (list-datasets [_]
    (let [prefix (str root-table ".")
          filter-fn (fn [t]
                      (.startsWith (.getNameAsString t) prefix))
          extract-fn (fn [t]
                       (->> (string/replace (.getNameAsString t) prefix "")
                            (#(string/split % #"[.]+"))
                            ;; drop table suffix
                            (first)))]
      (->> (admin/list-tables)
           (filter filter-fn)
           (map extract-fn)
           (distinct))))

  (connect [_ dataset-name]
    (let [conn (->HBaseConnection
                 (get-table-name root-table dataset-name) client-opts)]
      (try
        (.get-stats conn)
        (catch Exception e
          (throw
            (RuntimeException.
              (format
                "Failed to connect to dataset \"%s\" with root-table \"%s\""
                dataset-name root-table)
              e))))
      conn)))

(defn- root-table-prefix
  "Given a HBaseSchema root, construct a root prefix for naming the
  associated HBase tables.

  This function reverses the components of the provided HBaseSchema
  root to construct the table naming prefix. Example:

      (root-table-prefix \"foo.bar.com\") => \"com.bar.foo\"
  "
  ([root db-version]
   (->> (string/split root #"[.]+")
        (reverse)
        (#(if db-version (cons db-version %) %))
        (string/join ".")))
  ([root]
   (root-table-prefix root nil)))


(defn mk-schema [store-spec]
  "Given a store spec map, return a HBaseSchema instance

  The store-spec argument is a map that can include the following entries

   :root            - the root of the store
   :db-version      - (optional) the version of this library"
  (let [root-table (root-table-prefix
                     (:root store-spec) (:db-version store-spec))]
    (->HBaseSchema root-table store-spec)))