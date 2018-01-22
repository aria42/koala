(ns koala.series
  (:refer-clojure :exclude [range])
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
    (.iterator ^IPersistentVector vals))

  Indexed
  (nth [_ idx]
    (nth vals idx))
  (nth [_ idx not-found]
    (if (< idx (count vals))
      (nth vals idx)
      not-found))

  Seqable
  (seq [_]
    (seq vals)))

(defn num-dtype? [dtype]
  (#{:double :short :long :int} dtype))

(defn make [vals &
            {:keys [dtype, index]
             :or {dtype :double}}]
  (FullSeries.
   index
   (if (num-dtype? dtype)
     (apply vector-of dtype vals)
     vals)
   dtype))

(defn range [series start & [stop]]
  (let [^FullSeries series series
        stop (if-not stop (count series) stop)]
    (FullSeries.
     (into {}
           (for [[key idx] (.key->index series)
                 :when (and (>= idx start) (< idx stop))]
             [key idx]))
     (subvec (.vals series) start stop)
     (.dtype series))))
