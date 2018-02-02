(ns koala.dataframe
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [koala.series :as series]
   [koala.util :as util]
   [plumbing.core :refer [map-from-keys map-vals]])
  (:import
   (it.unimi.dsi.fastutil.doubles
    DoubleArrayList)
   (it.unimi.dsi.fastutil.longs
    LongArrayList)
   (it.unimi.dsi.fastutil.objects
    ObjectArrayList)
   (java.util Iterator)))

(deftype Dataframe [column->series ordered-columns]

  clojure.lang.Seqable
  (seq [_]
    (let [iters (map #(.iterator (column->series %)) ordered-columns)]
      (iterator-seq
       (reify Iterator
         (hasNext [_]
           (.hasNext ^Iterator (first iters)))
         (next [_]
           ;; code hot-spot, pay attention to perf
           (loop [cols ordered-columns
                  iters iters
                  result (transient [])]
             (if-let [c (first cols)]
               (let [it (first iters)]
                 (recur (next cols)
                        (next iters)
                        (conj! result [c (.next ^Iterator it)])))
               (persistent! result))))))))

  clojure.lang.Associative
  (containsKey [_ k]
    (.containsKey ^clojure.lang.Associative column->series k))
  (entryAt [_ k]
    (.entryAt ^clojure.lang.Associative column->series k))
  (assoc [this k v]
    (when-not (= (count this) (count v))
      (throw (ex-info "New column doesn't match length"
                      {:new-column (count v)
                       :existing-columns (count this)})))
    (Dataframe.
     (assoc column->series k (series/make v))
     (conj ordered-columns k)))

  clojure.lang.ILookup
  (valAt [_ k]
    (get column->series k))
  (valAt [_ k nf]
    (get column->series k nf))

  clojure.lang.Indexed
  (nth [_ idx]
    (->> ordered-columns
         (mapv (fn [c] (util/->Pair c (nth (column->series c) idx))))))

  clojure.lang.Counted
  (count [_]
    (if-let [s (-> column->series vals first)]
      (count s)
      0)))

(defmethod print-method Dataframe [df ^java.io.Writer w]
  (let [^DataFrame df df])
  (.write w (format "#Dataframe{ cols: %s, count: %d}"
                    (vec (.ordered-columns df))
                    (count df))))

(defn make [data
            & {:keys [index, dtype]
               :or {dtype :double}}]
  (cond

    (map? data)
    (Dataframe. (map-vals series/make data) nil)

    (or (seq? data) (vector? data))
    (let [columns (sort (set (mapcat keys data)))]
      (Dataframe.
       (map-vals
        (fn [k]
          (series/make
           (map #(get % k) data)
           :index))
        columns)
       columns
       ))))

(defn- read-raw-columns [header tail]
  (let [header->column (map-from-keys (fn [_] (transient [])) header)
        num-headers (count header)]
    (loop [tail tail header->column header->column]
      (if-let [row (first tail)]
        (let [pairs (mapv util/->Pair header row)]
          (when-not (= num-headers (count pairs))
            (throw (ex-info "Row has wrong number of elements"
                            {:header header :row row})))
          (recur
           (next tail)
           (persistent! (reduce
             (fn [res [h r]]
               (assoc! res h (conj! (res h) r)))
             (transient header->column)
             pairs))))
        (map-vals persistent! header->column)))))


(defn from-csv
  [source &
   {:keys [column-fn]
    :or   {column-fn identity}}]
  (with-open [f (io/reader source)]
    (let [[headers & tail] (csv/read-csv f)
          headers (map column-fn headers)
          header->vals (read-raw-columns headers tail)]
      (Dataframe.
       (map-vals series/make header->vals)
       headers))))
