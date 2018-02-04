(ns koala.dataframe
  (:refer-clojure :exclude [filter map range])
  (:require
   [clojure.java.io :as io]
   [koala.series :as series]
   [koala.util :as util]
   [plumbing.core :refer [map-from-keys map-vals]])
  (:import
   (clojure.lang
    Associative
    Counted
    ILookup
    Indexed
    Seqable)
   (org.simpleflatmapper.csv
    CsvParser)
   (it.unimi.dsi.fastutil.doubles
    DoubleArrayList)
   (it.unimi.dsi.fastutil.longs
    LongArrayList)
   (it.unimi.dsi.fastutil.objects
    ObjectArrayList)
   (java.util Iterator)))


(deftype Dataframe [column->series ordered-columns]

  Seqable
  (seq [this]
    (iterator-seq (.iterator ^Iterable this)))

  Associative
  (containsKey [_ k]
    (.containsKey ^clojure.lang.Associative column->series k))
  (entryAt [_ k]
    (.entryAt ^clojure.lang.Associative column->series k))
  (assoc [this k v]
    
    (let [v (if (fn? v)
              (clojure.core/map (fn [row] (v (into {} row))) (seq this))
              v)
          s (series/make v)]
      (when-not (= (count this) (count s))
        (throw (ex-info "New series data doesn't match data-frame length"
                        {:new-column (count s)
                         :existing-columns (count this)})))
      (Dataframe.
       (assoc column->series k s)
       (distinct (conj ordered-columns k)))))

  ILookup
  (valAt [_ k]
    (get column->series k))
  (valAt [_ k nf]
    (get column->series k nf))

  Indexed
  (nth [_ idx]
    (->> ordered-columns
         (mapv (fn [c] (util/->Pair c (nth (column->series c) idx))))))

  Iterable
  (iterator [_]
    (let [iters (mapv #(.iterator ^Iterable (column->series %)) ordered-columns)
          cols (into [] ordered-columns)
          n (count iters)]
      (reify Iterator
        (hasNext [_]
          (.hasNext ^Iterator (first iters)))
        (next [_]
          (let [result (java.util.ArrayList. n)]
            (dotimes [idx n]
              (let [col (nth cols idx)
                    ^Iterator iter (nth iters idx)]
                (.add result (util/->Pair col (.next iter)))))
            result)))))

  Counted
  (count [_]
    (if-let [s (-> column->series vals first)]
      (count s)
      0)))

(defmethod print-method Dataframe [df ^java.io.Writer w]
  (let [^Dataframe df df]
    (.write w (format "#Dataframe{ cols: %s, count: %d}"
                      (vec (.ordered_columns df))
                      (count df)))))

(defn make [data
            & {:keys [index, dtype]
               :or {dtype :double}}]
  (cond

    (map? data)
    (Dataframe. (map-vals series/make data) (sort (keys data)))

    (or (seq? data) (vector? data))
    (let [columns (vec (sort (set (mapcat keys data))))]
      (Dataframe.
       (map-from-keys
        (fn [k]
          (series/make
           (clojure.core/map #(get % k) data)))
        columns)
       columns
       ))))

(defn filter
  [pred ^Dataframe df]
  (let [cols (.ordered-columns df)
        n (count cols)
        header->vals (map-from-keys
                      (fn [_] (java.util.ArrayList. n))
                      cols)
        it (.iterator df)]
    (loop []
      (when (.hasNext it)
        (let [row (.next it)]
          (when (pred (into {} row))
              (doseq [[k v] row]
                (.add ^java.util.List (header->vals k) v)))
          (recur))))
    (Dataframe.
     (map-vals series/make header->vals)
     cols)))

(defn map
  [f ^Dataframe df]
  (let [cols (.ordered-columns df)
        n (count cols)
        header->vals (map-from-keys
                      (fn [_] (java.util.ArrayList. n))
                      cols)
        it (.iterator df)]
    (loop []
      (when (.hasNext it)
        (let [row (f (.next it))]
          (doseq [[k v] row]
            (.add ^java.util.List (header->vals k) v))
          (recur))))
    (Dataframe.
     (map-vals series/make header->vals)
     cols)))

(defn- read-raw-columns [header tail]
  (let [header->column (map-from-keys (fn [_] (transient [])) header)
        num-headers (count header)]
    (loop [tail tail header->column header->column]
      (if-let [row (first tail)]
        (let [pairs (clojure.core/map util/->Pair header row)]
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

(def ^:private ^:const
  +strings+
  (class (into-array String [])))

(defn from-csv
  [source &
   {:keys [column-fn]
    :or   {column-fn identity}}]
  (let [it (CsvParser/iterator (io/reader source))
        headers (object-array (clojure.core/map column-fn (.next it)))
        n (count headers)
        header->vals (map-from-keys
                      (fn [_] (java.util.ArrayList. n))
                      headers)]
    (loop []
      (when (.hasNext it)
        (let [row (.next it)]
          (dotimes [idx n]
            (let [h (aget ^objects headers idx)
                  ^java.util.List vals (header->vals h)]
              (.add vals (aget ^objects row idx))))
          (recur))))
    (Dataframe.
     (map-vals series/make header->vals)
     (into [] headers))))


(defn as-numeric [^Dataframe df cols]
  (Dataframe.
   (reduce
    (fn [m c]
      (update m c series/as-numeric))
    (.column->series df)
    cols)
   (.ordered-columns df)))

(defn range [^Dataframe df [start stop]]
  (Dataframe.
   (map-vals
    (fn [s] (series/range s [start stop]))
    (.column->series df))
   (.ordered-columns ^Dataframe df)))

(defn ->html [^Dataframe df]
  [:table
   [:tr (mapv (fn [c] [:th (str c)]) (.ordered-columns df))]
   (mapv
    (fn [row]
      [:tr (mapv (fn [[_ v]] [:td v]) row)])
    df)])
