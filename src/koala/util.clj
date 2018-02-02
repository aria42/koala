(ns koala.util
  (:import
   (clojure.lang
    Indexed
    IPersistentCollection
    Seqable)))

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


  IPersistentCollection
  (count [_] 2)
  (equiv [_ other]
    (and (instance? Pair other)
         (= x1 (.x1 ^Pair other))
         (= x2 (.x2 ^Pair other))))


  Seqable
  (seq [_]
    (list x1 x2)))


(defmethod print-method Pair [p ^java.io.Writer w]
  (.write w (pr-str [(first p) (second p)])))
