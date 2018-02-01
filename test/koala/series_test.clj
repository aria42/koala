(ns koala.series-test
  (:require [koala.series :as series]
            [clojure.test :refer [deftest is are testing]]))

(deftest flat-series
  (testing "no index"
    (let [d (series/make [1 2 3] :dtype :int)]
      (is (= [1 2 3] (seq d)))
      (is (= 3 (count d)))
      (is (= 2 (nth d 1)))
      (is (not (series/has-index? d)))))
  (testing "with index"
    (let [d (series/make [1 2 3] :index [:a :b :c])]
      (is (= [1.0 2.0 3.0] (seq d)))
      (is (= [:a :b :c] (series/index-keys d)))
      (is (= 1.0 (:a d)))
      (is (= 2.0 (:b d)))
      (is (= 3.0 (:c d)))
      (is (= :not-found (:not-exists d :not-found)))
      (is (series/has-index? d)))))


(deftest object-series
  (let [d (series/make {:a 1.0 :b 2.0 :c 3.0})]
    (is (= [1.0 2.0 3.0] (seq d)))
    (is (= [:a :b :c] (series/index-keys d)))
    (is (= 1.0 (:a d)))
    (is (= 2.0 (:b d)))
    (is (= 3.0 (:c d)))
    (is (= :not-found (:not-exists d :not-found)))))


(deftest range-test
  (let [s1 (series/make [1 2 3 4] :index [:a :b :c :d])
        sub-s1 (series/range s1 [1 3])]
    (is (= (count sub-s1) 2))
    (is (= [2.0 3.0] (seq sub-s1)))
    (is (= 2.0 (:b sub-s1)))
    (is (= 3.0 (:c sub-s1)))
    (is (= :not-found (:d sub-s1 :not-found)))))
