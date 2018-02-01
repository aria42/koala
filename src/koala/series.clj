(ns koala.series
  (:refer-clojure :exclude [range])
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
    Iterable)))

;;; Internal Protocols and Types

(defprotocol InternalSeries
  (-has-index? [this])
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
  (-has-index? [this]
    (boolean key->index))
  (-index-keys [this]
    (when-not (-has-index? this)
      (throw (ex-info "Series has no index" {})))
    (map first (sort-by val key->index))))


;;; Schemas

(s/def ::dtype #{:double :short :long :int :string :date})

(defn num-dtype? [dtype]
  (#{:double :short :long :int} dtype))

(s/def ::value
  (s/or :number number? :string string?))

(s/def ::index-key
  (s/or :string string? :keyword keyword?))

(s/def ::flat-seq
  (s/coll-of ::value))

(s/def ::obj-series
  (s/map-of keyword? ::value))

(defn coerce-index
  [index]
  (cond
    (or (seq? index) (vector? index))
    (into {} (map-indexed (fn [idx k] [k idx]) index))

    (map? index)
    index

    :else
    (throw (ex-info "Unrecognized index" {:index index}))))

(defn coerce-vals
  [dtype vals]
  (cond
    (num-dtype? dtype)
    (apply vector-of dtype vals)

    (= :string dtype)
    (mapv str vals)


    :else
    vals))

;;; Public Functions

(defn make [data &
            {:keys [dtype, index]
            :or {dtype :double}}]
  (cond
    ;; flat sequence has no index
    (every? #(s/valid? ::value %) data)
    (FullSeries.
     (when index (coerce-index index))
     (coerce-vals dtype data)
     dtype)

    ;; single object with index keys
    ;; will impose an ordering by sorting keys
    (s/valid? ::obj-series data)
    (let [all-keys (set (keys data))
          sorted-keys (sort all-keys)]
      (FullSeries.
       (coerce-index sorted-keys)
       (coerce-vals dtype (map data sorted-keys))
       dtype))

    :else
    (throw (ex-info "Can't make data from data" {:data data}))))

(defn range
  "Create a sub-series from a range of a series."
  [series [start stop]]
  (let [^FullSeries series series
        stop (if-not stop (count series) stop)
        k->i (.key->index series)
        i->k (into {} (for [[k i] k->i] [i k]))]
    (make
     (vec (subvec (.vals series) start stop))
     :dtype (.dtype series)
     :index (mapv i->k (clojure.core/range start stop)))))


(defn index-keys
  "return index keys over series if available
   or nil if none exists"
  [series]
  (-index-keys series))

(defn has-index? [series]
  (-has-index? series))
