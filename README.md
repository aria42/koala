# koala

A clojure dataframe library, meant to replicate some of what [pandas](https://pandas.pydata.org/) does for python. The library is **extremely alpha** at the moment.

While early, the out-of-the-box performance is pretty reasonable. For example, for [this CSV](https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv?accessType=DOWNLOAD) file from Data.gov with over 950k rows (and about 500 MB uncompressed) in about 6 seconds (assuming you've got enough JVM memory allocated to accomodate this data). 

## Usage

In addition to this README, check out the `dataframe_test` test for more examples in action.

### Read a dataframe from a CSV file

```clojure
(require '[koala.dataframe :as df]')
(def data (df/from-csv "/path/to/csv" :column-fn keyword))
;; first 5 values of column 5
(->> data :col1 (take 3))
user> ("1" "2" "3")
```

The column values have been read as strings by default, but you can force numerical columns (which will use `long[]` or `double[]` under the hood as approriate). You can use `df/as-numeric` which can be told or guessed the numerical type of the column

```clojure
(def data (-> "/path/to/csv")
              (df/from-csv :column-fn keyword)
              (df/as-numeric [:col1]))
(->> data :col1 (take 3))
user> (1 2 3)
```
### A dataframe can be treated as a sequence of rows

You can use the usual sequence functions on a dataframe which will return a sequence of the `(column, value)` pairs for each row; the row is not put into a map to avoid the perf hit unless needed (this might change if all use-cases really want the map)

```clojure
(first data)
user> [[:col1 1] [:col2 "CA"]]
(take 2 data)
user> ([[:col1 1] [:col2 "CA"]] [[:col1 2] [:col2 "WA"]])
(count data)
user> 42
```

You can use `df/range` to get a range of rows and return a data-frame

```clojure
(def first-two (range data [0 2]))
(count first-two)
user> 2
(seq first-two)
user> ([[:col1 1] [:col2 "CA"]] [[:col1 2] [:col2 "WA"]])
```

### Filtering rows of the data frame 

A common use for a data-frame is to apply some function to each row or on a specific columns to filter the data-frame (e.g, toss out rows where a value is missing). You can use `df/filter` and a predicate on the map of `column->value` for each row to determine whether to filter the row out or not. 

```clojure
;; filter to only rows where `:col1` is even and the `:col2` is Washington (`"WA"`)
(def smaller (df/filter 
               (fn [r] (or (even? (:col1 r)) (= (:col2 r) "WA")))
               df))
(count smaller)
user> 12
(first smaller)
user> [[:col1 2] [:col2 "WA"]]
```

### Add a column to dataframe 

There are a few ways to add a column to the dataframe. The simplest is to simply `assoc` a series onto the dataframe, but note the length of the column needs to match the number of rows in the existing dataframe: 

```clojure
;; take first three rows and add a new column with three values
(def augmented-data (-> data (df/range [0 3]) (assoc :new-column [2 4 6])))
(:new-column augmented-data)
user> (2 4 6)
(first augmented-data)
user> [[:col1 1] [:col2 "CA"] [:new-column 2]]
```

If you want more fine grained control over the column, you can use the functions in `koala.series` to determine attributes of the new column:
```clojure
;; take first three rows and add a new column with three values
(def new-column (series/make [2 4 6] :dtype :long))
(def augmented-data (-> data (df/range [0 3]) (assoc :new-column new-column)))
```

You can also easily add a column by providing a function which takes the current row as map of `(column, value)` and returns the new column value:
```clojure
(def augmented-data (->> data (assoc :new-column (fn [r] (* 2 (:col1 r))))))
(first augmented-data)
user> [[:col1 1] [:col2 "CA"] [:new-column 2]]
```

Of course, this lets you do things like add a constant column pretty easily
```clojure
(assoc data :bias (constantly 1))
```

### Update a columns value

Since dataframes support `assoc`, the standard set of ways of 'updating' associative structures in clojure apply to koala. For instance, the `series/unit-normalzie` function will unit-normalize

```clojure
(def d (df/make {:col1 [1, 2, 3]}))
(seq d)
user> ([[:col1 1]] [[:col1 2]] [[:col1 3]])
(-> d (update :col1 series/unit-normalize) seq)
([[:col1 -1]] [[:col1 0]] [[:col1 1]])
```

If you want to transform each value of the column to update, the function can `map` over series values and the returned lazy sequence is converted back into a series. 

```clojure
(-> d (update :col1 (fn [s] (map inc s))) seq)
([[:col1 2]] [[:col1 3]] [[:col1 4]])
```

### Convert table to Hiccup HTML to use with Notebook

You can use convert a dataframe to a HTML table for pretty display for Notebook environments, including [Clojupyter](https://github.com/clojupyter/clojupyter), using `df/->html` which will return a [hiccup](https://github.com/weavejester/hiccup) data-structure.

## License

Copyright © 2018 aria42 Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
