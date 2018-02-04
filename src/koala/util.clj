(ns koala.util
  (:import
   (clojure.lang
    Indexed
    IPersistentCollection
    Seqable)
   (java.util
    Objects
    Map$Entry)))

(deftype Pair [x1 x2]

  Indexed
  (nth [_ idx]
    (cond
      (= idx 0) x1
      (= idx 1) x2
      :else (throw (IndexOutOfBoundsException.))))
  (nth [_ idx not-found]
    (cond
      (= idx 0) x1
      (= idx 1) x2
      :else not-found))

  Map$Entry
  (getKey [_] x1)
  (getValue [_] x2)


  IPersistentCollection
  (count [_] 2)
  (equiv [_ other]
    (and (instance? Pair other)
         (= x1 (.x1 ^Pair other))
         (= x2 (.x2 ^Pair other))))

  Object
  (equals [_ o]
    (and (instance? Pair o)
         (= x1 (.x1 ^Pair o))
         (= x2 (.x2 ^Pair o))))
  (hashCode [_]
    (Objects/hash (object-array x1 x2)))

  Seqable
  (seq [_]
    (list x1 x2)))


(defmethod print-method Pair [p ^java.io.Writer w]
  (.write w (pr-str [(first p) (second p)])))
