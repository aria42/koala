(ns koala.series
  (:refer-clojure :exclude [range map])
  (:require
   [clojure.spec.alpha :as s])
  (:import
   (clojure.lang
    Counted
    ILookup
    Indexed
    IPersistentCollection
    IPersistentMap
    IPersistentVector
    Seqable)
   (java.lang
    Iterable)
   (java.util
    Arrays)
   (org.apache.commons.lang3.math NumberUtils)))

;;; Public Schema

(s/def ::dtype #{:double :long :object})

(defn num-dtype? [dtype]
  (#{:double :long} dtype))


;;; Private Protocols and Types

(defprotocol InternalSeries
  (-has-index? [this])
  (-dtype [this])
  (-index-keys [this]))

(deftype FullSeries [key->index vals dtype]

  Counted
  (count [_]
    (count vals))

  ILookup
  (valAt [_ key]
    (when-not key->index
      (throw (ex-info "Series does not have an index" {})))
    (when-let [idx (get key->index key)]
      (nth vals idx)))
  (valAt [_ key not-found]
    (when-not key->index
      (throw (ex-info "Series does not have an index" {})))
    (if-let [idx (get key->index key)]
      (nth vals idx)
      not-found))

  Iterable
  (iterator [this]
    (.iterator ^Iterable vals))

  Series
  (dtype [this] dtype)

  Indexed
  (nth [_ idx]
    (nth vals idx))
  (nth [_ idx not-found]
    (if (< idx (count vals))
      (nth vals idx)
      not-found))

  Seqable
  (seq [_]
    (seq vals))


  InternalSeries
  (-dtype [this] dtype)
  (-has-index? [this]
    (boolean key->index))
  (-index-keys [this]
    (when-not (-has-index? this)
      (throw (ex-info "Series has no index" {})))
    (clojure.core/map first (sort-by val key->index))))


(s/def ::value
  (s/or :number number? :string string? :keyword keyword?))

(s/def ::index-key
  (s/or :string string? :keyword keyword?))

(s/def ::flat-seq
  (s/coll-of ::value))

(s/def ::obj-series
  (s/map-of keyword? ::value))

(defn- coerce-index
  [index]
  (cond
    (or (seq? index) (vector? index))
    (into {} (map-indexed (fn [idx k] [k idx]) index))

    (map? index)
    index

    :else
    (throw (ex-info "Unrecognized index" {:index index}))))

(defn- coerce-vals
  [dtype vals]
  (case dtype
    :double (double-array vals)
    :long (long-array vals)
    :object (into [] vals)))


(defn guess-dtype [vals]
  {:pre [] :post [(s/conform ::dtype %)]}
  (let [dtypes (frequencies
                (clojure.core/map
                 (fn inner-cunt [v]
                   (cond
                     (float? v) :double
                     (number? v) :long
                     (string? v)
                     (if (NumberUtils/isNumber ^String v)
                       (let [d (Double/parseDouble ^String v)]
                         (if (= (Math/floor d) d) :long :double))
                       :object)
                     :else (throw (ex-info "Bad value" {:value v}))))
                 vals))]
    (cond
      (= (dtypes :long) (count vals)) :long
      (= (+ (dtypes :long 0) (dtypes :double 0)) (count vals)) :double
      :else :object)))

;;; Public Functions

(defn as-numeric
  "convert `:object` series to numeric type
   provided by `dtype` or guesses by examining entries. Assumes
   that the string representation of the elements is parse-able 
   as a number"
  [s & [dtype]]
  {:pre [(instance? FullSeries s) (= :object (-dtype s))]
   :post [(num-dtype? (-dtype %))]}
  (let [dtype (if dtype dtype (guess-dtype s))]
    (FullSeries.
     (.key->index ^FullSeries s)
     (case dtype
       :double (double-array (map #(Double/parseDouble (str %)) s))
       :long (long-array (map #(Long/parseLong (str %)) s))
       (throw (ex-info "Couldn't parse series as numeric" {:series s})))
     dtype)))

(defn make [data & {:keys [dtype, index]}]
  (cond
    ;; flat sequence has no index
    (every? #(or (number? %) (string? %)) data)
    (let [dtype (or dtype :object)]
      (FullSeries.
       (when index (coerce-index index))
       (coerce-vals dtype data)
       dtype))

    ;; single object with index keys
    ;; will impose an ordering by sorting keys
    (s/valid? ::obj-series data)
    (let [all-keys (set (keys data))
          sorted-keys (sort all-keys)
          dtype (or dtype :object)]
      (FullSeries.
       (coerce-index sorted-keys)
       (coerce-vals dtype (clojure.core/map data sorted-keys))
       dtype))

    :else
    (throw (ex-info "Can't make data from data" {:data data}))))

(defn map [f & input-series]
  (let [m (or (meta f) {})
        dtype (->> [:long :double :object]
                   (filter #(m %))
                   first)]
    (make
     (apply clojure.core/map f input-series)
     :dtype dtype)))

(defn range
  "Create a sub-series from a range of a series."
  [series [start stop]]
  (let [^FullSeries series series
        dtype (-dtype series)
        stop (if-not stop (count series) stop)
        k->i (.key->index series)
        i->k (into {} (for [[k i] k->i] [i k]))]
    (make
     (case dtype
         :long (Arrays/copyOfRange ^longs (.vals series) (int start) (int stop))
         :double (Arrays/copyOfRange ^doubles (.vals series) (int start) (int stop))
         :object (vec (subvec (.vals series) start stop)))
     :dtype dtype
     :index (mapv i->k (clojure.core/range start stop)))))


(defn index-keys
  "return index keys over series if available
   or nil if none exists"
  [series]
  (-index-keys series))

(defn has-index? [series]
  (-has-index? series))
