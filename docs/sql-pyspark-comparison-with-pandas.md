---
layout: global
title: PySpark Comparison with Pandas 
displayTitle: PySpark Comparison with Pandas 
---

Both PySpark and Pandas cover important use cases and provide a rich set of features to interact 
with various structural and semistructral data in Python world. Often, PySpark users are used to 
Pandas. Therefore, this document targets to document the comparison.

* Overview
* DataFrame APIs
  * Quick References
  * Create DataFrame
  * Load DataFrame
  * Save DataFrame
  * Inspect DataFrame
  * Interaction between PySpark and Pandas
* Notable Differences
  * Lazy and Eager Evaluation
  * Direct assignment
  * NULL, None, NaN and NaT
  * Type inference, coercion and cast


## Overview

PySpark and Pandas support common functionality to load, save, create, transform and describe 
DataFrame. PySpark provides conversion from/to Pandas DataFrame, and PySpark introduced Pandas 
UDFs which allow to use Pandas APIs as are for interoperability between them.

Nevertheless, there are fundamental differences between them to note in general.

1. PySpark DataFrame is a distributed dataset across multiple nodes whereas Pandas DataFrame is a
  local dataset within single node.

    It brings a practical point. If you handle larget dataset, arguably PySpark brings arguably a
    better performance in general. If the dataset to process does not fix into the memory in a
    single node, using PySpark is probably the way. In case of small dataset, Pandas might be
    faster in general since there would not be overhead, for instance, network.

2. PySpark DataFrame is lazy evaluation whereas Pandas DataFrame is eager evaluation.

    PySpark DataFrame executes lazily whereas Pandas DataFrame executes each operation
    immediately against the data set.

3. PySpark DataFrame is immutable in nature whereas Pandas DataFrame is mutable.

    In PySpark, it creates DataFrame once which cannot be changed. Instead, it should transform
    it to another DataFrame whereas Pandas DataFrame is mutable which directly updates the state
    of it. Typical example is `String` vs `StringBuilder` in Java.
  
4. PySpark operations on DataFrame tend to comply SQL.

    It causes some subtleties comparing to Pandas, for instance, about `NaN`, `None` and `NULL`.

There are similarities and differences between them which might bring confusion. In this document
these are described and illuminated by several examples.



## DataFrame APIs

This chapter describes DataFrame APIs in both PySpark and Pandas.


### Quick References

| PySpark                                                            | Pandas                                   |
| ------------------------------------------------------------------ | ---------------------------------------- |
| `df.limit(3)`                                                      | `df.head(3)`                             |
| `df.filter("a == 1 AND b == 2")`                                   | `df.filter("(df.a == 1) & (df.b == 2)")` |
| `df.filter((df.a == 1) & (df.b == 2))`                             | `df[(df.a == 1) & (df.b == 2)]`          |
| `df.select("a", "b")`                                              | `df[["a", "b"]]`                         |
| `df.drop_duplicates()`                                             | `df.drop_duplicates()`                   |
| `df.sample(fraction=0.01)`                                         | `df.sample(frac=0.01)`                   |
| `df.groupby("a").count()`                                          | `df.groupby("a").size()`                 |
| `df.groupby("a").agg({"b": "sum"})`                                | `df.groupby("a").agg({"b": np.sum})`     |
| `df1.join(df2, on="a")`                                            | `pandas.merge(df1, df2, on="a")`         |
| `df1.union(df2)`                                                   | `pandas.concat(df1, df2)`                |
| `df = df.select(when(df["a"] < 5, df["a"] * 2).otherwise(df["a"]))`| `df.loc[pdf['a'] < 5, 'a'] *= 2`         |


### Create DataFrame

In order to create DataFrame in PySpark and Pandas, you can run the codes below: 

```python
# PySpark
data = zip(['Chicago', 'San Francisco', 'New York City'], range(1, 4))
spark.createDataFrame(list(data), ["city", "rank"])
```

```python
# Pandas
data = {'city': ['Chicago', 'San Francisco', 'New York City'], 'rank': range(1, 4)}
pandas.DataFrame(data)
```

One notable difference when creating DataFrame is that Pandas accepts the data as below:

```
data = {
    'city': ['Chicago', 'San Francisco', 'New York City'],
    'rank': range(1, 4)
}
```

and it interprets as:

```
            city  rank
0        Chicago     1
1  San Francisco     2
2  New York City     3
```

So, a dictionary that contains key and multiple values becomes DataFrame but PySpark does
support this. Instead, PySpark can make DataFrame from Pandas DataFrame as below:

```
createDataFrame(pandas.DataFrame(data))
```

For more information see "Interaction between PySpark and Pandas" below.


### Load DataFrame

To load DataFrame, there are APIs, usually, `spark.read.xxx` (or `spark.read.format("xxx")`)
for PySpark and `pandas.read_xxx` for Pandas.

```python
# PySpark
df = spark.read.csv("data.csv")
```

```python
# Pandas
df = pandas.read_csv("data.csv")
```

There are many sources available in both PySpark and Pandas. For example, CSV, JSON and
Parquet are commonly supported but each supports different set of sources. There are pretty
different set of options availabe as well, please see the API documentation for both sides.
Note that, to match behaviours for PySpark to Pandas, `header=True` and `inferSchema=True`
options are required.


### Save DataFrame

To load DataFrame, likewise, the APIs are `spark.read.xxx` (or `spark.read.format("xxx")`) for
PySpark and `pandas_to_xxx` for Pandas.

```
df.to_csv("data.csv")
```

```
df.write.csv("data.set")
```

Likewise, different set of sources are supported. Note that, to match behaviours for PySpark
to Pandas, `header=True` option is required.


### Inspect DataFrame

DataFrame in both PySpark and Pandas can be expected in many ways. One of the way is to
`describe()` as below:

```
# PySpark
df.describe().show()
```

```
+-------+-------------+----+
|summary|         city|rank|
+-------+-------------+----+
|  count|            4|   4|
|   mean|         null| NaN|
| stddev|         null| NaN|
|    min|      Chicago| 1.0|
|    max|San Francisco| NaN|
+-------+-------------+----+
```

In Pandas, the same name function `describe()` can be called:

```
# Pandas
df.describe()
```

```
       rank
count   3.0
mean    2.0
std     1.0
min     1.0
25%     1.5
50%     2.0
75%     2.5
max     3.0
```

There are some differences, for instance, in case of PySpark, `NaN` is excluded but Pandas ignore `NaN` in some statistics. 

To inspect columns, there are `dtype` API in PySpark and Pandas.

```
# PySpark
df.dtypes
```

```
[('city', 'string'), ('rank', 'double')]
```

```
# Pandas
df.dtypes
```

```
0     object
1    float64
dtype: object
```

PySpark types are represented as Spark SQL types whereas they are NumPy types in case of Pandas.

Note that, PySpark supports pretty printing of schema as below:

```
# PySpark
df.printSchema()
```

```
root
 |-- city: string (nullable = true)
 |-- rank: double (nullable = true)
```


### Interaction between PySpark and Pandas

PySpark supports conversion to/from Pandas and Pandas UDFs out of the box.
See [PySpark Usage Guide for Pandas with Apache Arrow](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html).



## Notable Differences

There are other notable differences for many reasons, for instance,
PySpark DataFrame's core is Spark SQL so some of APIs comply SQL, and this might be different
comparing to Pandas context which somtimes causes some subtleties. See the differences below.


### Lazy and Eager Evaluation

PySpark DataFrame is lazy whereas Pandas DataFrame is eager. Therefore,
PySpark DataFrame can optimize the computation before an actual execution whereas Pandas
DataFrame can have each direct result immediately after each execution.

For sintance, suppose you select columns multiple times as below:
  
```python
df = ...  # PySpark DataFrame
df = df.select("fieldA", "fieldB")
df = df.select("fieldB")
df.show()
```

```python
pdf = ...  # Pandas DataFrame
pdf = pdf[["fieldA", "fieldB"]]
pdf = pdf[["fieldA"]]
```
  
PySpark DataFrame only selects `fieldB` alone for its execution when an action, `collect`, is
performed. Pandas DataFrame immediately applies the operation at each line. So, `fieldA` and
`fieldB` are selected, and then `fieldB` is selected with the updated result for each.


### Direct assignment

PySpark does not allow the direct assignment whereas Pandas does.

```python
# Pandas
df.city = 1
```

```
   city  rank
0     1     1
1     1     2
2     1     3
```

In PySpark, it should be done as:

```
df.withColumn("city", lit(1)).show()
```

This is fundamentally because PySpark DataFrame is immutable whereas Pandas DataFrame is mutable.


### `NULL`, `None`, `NaN` and `NaT` 

`NULL`, `None`, `NaN` and `NaT` concepts sometimes bring confusions to users and developers. Here are definitions of each:

- In Spark SQL, `NULL` represents a missing value.
- In Python, `None` represents a missing value.
- In Pandas, for float types, `NaN` represents not-a-number and also a missing value. 
- In Pandas, for `datetime64[ns]` type, `NaT` represents a missing value.

For `NaN` comparison, sometimes confusions happen. Usually the comparison is not only specific in Pandas but also regular Python, NumPy, etc. 

See the comparisons below:

```python
>>> np.nan and True
True
>>> True and np.nan
nan
```

```python
>>> np.nan and False
False
>>> False and np.nan
False
```

```python
>>> np.nan or True
nan
>>> True or np.nan
True
```

```python
>>> np.nan or False
nan
>>> False or np.nan
nan
```

Python logical operators are different from SQL and some other programming languages. These comparisons are all correct as specified rules within Python. 

**Note:** Python's boolean operator is different:

| Operation   | Result                        |
| ----------- | ----------------------------- |
| `x or y	`   | if x is false, then y, else x |
| `x and y`   | if x is false, then x, else y |

See also [Boolean Operations](https://docs.python.org/3/library/stdtypes.html?highlight=short%20circuit#boolean-operations-and-or-not) in Python. 

**Note:** `bool(np.nan)` is `True`. See also [Truth Value Testing](https://docs.python.org/3/library/stdtypes.html#truth-value-testing).

**Note:** When you switch `np.nan` to `float("nan")`, it still shows the same results. Even when you switch `np.nan` to `np.datetime64('NaT')`, it also shows the same pattern (although the result `nan` becomes `np.datetime64('NaT')` instead).

In PySpark, `None` comparison follows `NULL` comparison in Spark SQL.

```
+-----+-----+---------+--------+
|    x|    y|(x AND y)|(x OR y)|
+-----+-----+---------+--------+
| null| true|     null|    true|
| true| null|     null|    true|
| null|false|    false|    null|
|false| null|    false|    null|
+-----+-----+---------+--------+
```

`NaN` and `boolean` type comparison is disallowed.


### Type inference, coercion and cast

PySpark support native Python types currently. Those Python types are mapped to Spark SQL types in order to leverage Spark SQL. So, in general PySpark behaviours follow Spark SQL's behaviours whereas Pandas tend to look after Python's own behaviours and NumPy's behaviours since both types are supported in Pandas.

For type inference and coercion, for example, when we read DataFrame from CSV file,

```
spark.read.csv("data.csv", inferSchema = True)
```

```
pandas.read_csv("data.csv")
```

PySpark and Pandas both try to infer the types from each row. If types between rows are different, both try to find wider type and coerce the type. PySpark follows the rules in Spark SQL whereas Pandas defines its own rule.

For type cast, PySpark also follows the rules defined in Spark SQL (by using `cast`) whereas Pandas is rather closer to Python's casting (by using `astype`)
