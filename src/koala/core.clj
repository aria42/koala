(ns koala.core
  (:import
   (clojure.lang
    IPersistentCollection
    IPersistentMap
    IPersistentVector)))

(deftype FullSeries [key->index vals]

  clojure.lang.ILookup
  (valAt [this key]
    (when-let [idx (get key->index key)]
      (nth vals idx)))
  (valAt [this key not-found]
    (if-let [idx (get key->index)]
      (nth vals idx)
      not-found))

  java.lang.Iterable
  (iterator [this]
    (.iterator ^IPersistentVector vals))

  clojure.lang.Seqable
  (seq [_]
    vals))
