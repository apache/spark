---
layout: global
title: Sketch-Based Approximate Functions
displayTitle: Sketch-Based Approximate Functions
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Spark's SQL and DataFrame APIs provide a collection of sketch-based approximate functions powered by the [Apache DataSketches](https://datasketches.apache.org/) library. These functions enable efficient probabilistic computations on large datasets with bounded memory usage and accuracy guarantees.

Sketches are compact data structures that summarize large datasets, supporting distributed aggregation through serialization and merging. This makes them ideal for use cases including (so far):
- **Approximate count distinct** (HLL and Theta sketches)
- **Approximate quantile estimation** (KLL sketches)
- **Approximate frequent items** (Top-K sketches)
- **Set operations** on distinct counts (Theta sketches)

### Table of Contents

* [HyperLogLog (HLL) Sketch Functions](#hyperloglog-hll-sketch-functions)
  * [hll_sketch_agg](#hll_sketch_agg)
  * [hll_union_agg](#hll_union_agg)
  * [hll_sketch_estimate](#hll_sketch_estimate)
  * [hll_union](#hll_union)
* [Theta Sketch Functions](#theta-sketch-functions)
  * [theta_sketch_agg](#theta_sketch_agg)
  * [theta_union_agg](#theta_union_agg)
  * [theta_intersection_agg](#theta_intersection_agg)
  * [theta_sketch_estimate](#theta_sketch_estimate)
  * [theta_union](#theta_union)
  * [theta_intersection](#theta_intersection)
  * [theta_difference](#theta_difference)
* [KLL Quantile Sketch Functions](#kll-quantile-sketch-functions)
  * [kll_sketch_agg_*](#kll_sketch_agg_)
  * [kll_sketch_to_string_*](#kll_sketch_to_string_)
  * [kll_sketch_get_n_*](#kll_sketch_get_n_)
  * [kll_sketch_merge_*](#kll_sketch_merge_)
  * [kll_sketch_get_quantile_*](#kll_sketch_get_quantile_)
  * [kll_sketch_get_rank_*](#kll_sketch_get_rank_)
* [Approximate Top-K Functions](#approximate-top-k-functions)
  * [approx_top_k_accumulate](#approx_top_k_accumulate)
  * [approx_top_k_combine](#approx_top_k_combine)
  * [approx_top_k_estimate](#approx_top_k_estimate)
* [Best Practices](#best-practices)
  * [Choosing Between HLL and Theta Sketches](#choosing-between-hll-and-theta-sketches)
  * [Accuracy vs. Memory Trade-offs](#accuracy-vs-memory-trade-offs)
  * [Storing and Reusing Sketches](#storing-and-reusing-sketches)
* [Common Use Cases and Examples](#common-use-cases-and-examples)
  * [Example: Tracking Daily Unique Users with HLL Sketches](#example-tracking-daily-unique-users-with-hll-sketches)
  * [Example: Computing Percentiles Over Time with KLL Sketches](#example-computing-percentiles-over-time-with-kll-sketches)
  * [Example: Set Operations with Theta Sketches](#example-set-operations-with-theta-sketches)
  * [Example: Finding Trending Items with Top-K Sketches](#example-finding-trending-items-with-top-k-sketches)

---

## HyperLogLog (HLL) Sketch Functions

HyperLogLog sketches provide approximate count distinct functionality with configurable accuracy and memory usage. They are well-suited for counting unique values in very large datasets.

See the [Apache DataSketches HLL documentation](https://datasketches.apache.org/docs/HLL/HLL.html) for more information.

### hll_sketch_agg

Creates an HLL sketch from input values that can later be used to estimate count distinct.

**Syntax:**
```sql
hll_sketch_agg(expr [, lgConfigK])
```

| Argument | Type | Description |
|----------|------|-------------|
| `expr` | INT, BIGINT, STRING, or BINARY | The expression whose distinct values will be counted |
| `lgConfigK` | INT (optional) | Log-base-2 of K, where K is the number of buckets. Range: 4-21. Default: 12. Higher values provide more accuracy but use more memory. |

Returns a BINARY containing the HLL sketch in updatable binary representation.

**Examples:**
```sql
-- Basic usage: create a sketch and estimate distinct count
SELECT hll_sketch_estimate(hll_sketch_agg(col))
FROM VALUES (1), (1), (2), (2), (3) tab(col);
-- Result: 3

-- With custom lgConfigK for higher accuracy
SELECT hll_sketch_estimate(hll_sketch_agg(col, 16))
FROM VALUES (50), (60), (60), (60), (75), (100) tab(col);
-- Result: 4

-- With string values
SELECT hll_sketch_estimate(hll_sketch_agg(col))
FROM VALUES ('abc'), ('def'), ('abc'), ('ghi'), ('abc') tab(col);
-- Result: 3
```

**Notes:**
- NULL values are ignored during aggregation.
- Empty strings (for STRING type) and empty byte arrays (for BINARY type) are ignored.
- The sketch can be stored and later merged with other sketches using `hll_union` or `hll_union_agg`.

---

### hll_union_agg

Aggregates multiple HLL sketches into a single merged sketch.

**Syntax:**
```sql
hll_union_agg(sketch [, allowDifferentLgConfigK])
```

| Argument | Type | Description |
|----------|------|-------------|
| `sketch` | BINARY | An HLL sketch in binary format (produced by `hll_sketch_agg`) |
| `allowDifferentLgConfigK` | BOOLEAN (optional) | If true, allows merging sketches with different lgConfigK values. Default: false. |

Returns a BINARY containing the merged HLL sketch.

**Examples:**
```sql
-- Merge sketches from different partitions
SELECT hll_sketch_estimate(hll_union_agg(sketch, true))
FROM (
  SELECT hll_sketch_agg(col) as sketch
  FROM VALUES (1) tab(col)
  UNION ALL
  SELECT hll_sketch_agg(col, 20) as sketch
  FROM VALUES (1) tab(col)
);
-- Result: 1

-- Standard merge (same lgConfigK)
SELECT hll_sketch_estimate(hll_union_agg(sketch))
FROM (
  SELECT hll_sketch_agg(col) as sketch
  FROM VALUES (1), (2) tab(col)
  UNION ALL
  SELECT hll_sketch_agg(col) as sketch
  FROM VALUES (3), (4) tab(col)
);
-- Result: 4
```

**Notes:**
- If `allowDifferentLgConfigK` is false and sketches have different lgConfigK values, an error is thrown.
- The output sketch uses the minimum lgConfigK value of all input sketches when merging sketches with different sizes.

---

### hll_sketch_estimate

Estimates the number of unique values from an HLL sketch.

**Syntax:**
```sql
hll_sketch_estimate(sketch)
```

| Argument | Type | Description |
|----------|------|-------------|
| `sketch` | BINARY | An HLL sketch in binary format |

Returns a BIGINT representing the estimated count of distinct values.

**Examples:**
```sql
SELECT hll_sketch_estimate(hll_sketch_agg(col))
FROM VALUES (1), (1), (2), (2), (3) tab(col);
-- Result: 3
```

**Errors:**
- Throws an error if the input is not a valid HLL sketch binary representation.

---

### hll_union

Merges two HLL sketches into one (scalar function).

**Syntax:**
```sql
hll_union(first, second [, allowDifferentLgConfigK])
```

| Argument | Type | Description |
|----------|------|-------------|
| `first` | BINARY | First HLL sketch |
| `second` | BINARY | Second HLL sketch |
| `allowDifferentLgConfigK` | BOOLEAN (optional) | Allow different lgConfigK values. Default: false. |

Returns a BINARY containing the merged HLL sketch.

**Examples:**
```sql
SELECT hll_sketch_estimate(
  hll_union(
    hll_sketch_agg(col1),
    hll_sketch_agg(col2)))
FROM VALUES (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) tab(col1, col2);
-- Result: 6
```

---

## Theta Sketch Functions

Theta sketches provide approximate count distinct with support for set operations (union, intersection, and difference). This makes them ideal for computing unique counts across overlapping datasets.

See the [Apache DataSketches Theta documentation](https://datasketches.apache.org/docs/Theta/ThetaSketches.html) for more information.

### theta_sketch_agg

Creates a Theta sketch from input values.

**Syntax:**
```sql
theta_sketch_agg(expr [, lgNomEntries])
```

| Argument | Type | Description |
|----------|------|-------------|
| `expr` | INT, BIGINT, FLOAT, DOUBLE, STRING, BINARY, ARRAY&lt;INT&gt;, or ARRAY&lt;BIGINT&gt; | The expression whose distinct values will be counted |
| `lgNomEntries` | INT (optional) | Log-base-2 of nominal entries. Range: 4-26. Default: 12. |

Returns a BINARY containing the Theta sketch in compact binary representation.

**Examples:**
```sql
-- Basic distinct count
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (1), (1), (2), (2), (3) tab(col);
-- Result: 3

-- With custom lgNomEntries
SELECT theta_sketch_estimate(theta_sketch_agg(col, 22))
FROM VALUES (1), (2), (3), (4), (5), (6), (7) tab(col);
-- Result: 7

-- With array values
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (ARRAY(1, 2)), (ARRAY(3, 4)), (ARRAY(1, 2)) tab(col);
-- Result: 2
```

**Notes:**
- NULL values are ignored.
- Supports a wider range of input types compared to HLL sketches.
- Empty arrays, empty strings, and empty binary values are ignored.

---

### theta_union_agg

Aggregates multiple Theta sketches using union operation.

**Syntax:**
```sql
theta_union_agg(sketch [, lgNomEntries])
```

| Argument | Type | Description |
|----------|------|-------------|
| `sketch` | BINARY | A Theta sketch in binary format |
| `lgNomEntries` | INT (optional) | Log-base-2 of nominal entries. Range: 4-26. Default: 12. |

Returns a BINARY containing the merged Theta sketch.

**Examples:**
```sql
SELECT theta_sketch_estimate(theta_union_agg(sketch, 15))
FROM (
  SELECT theta_sketch_agg(col1) as sketch
  FROM VALUES (1), (2), (3), (4), (5), (6), (7) tab(col1)
  UNION ALL
  SELECT theta_sketch_agg(col2, 20) as sketch
  FROM VALUES (5), (6), (7), (8), (9), (10), (11) tab(col2)
);
-- Result: 11
```

---

### theta_intersection_agg

Aggregates multiple Theta sketches using intersection operation (finds common distinct values).

**Syntax:**
```sql
theta_intersection_agg(sketch)
```

| Argument | Type | Description |
|----------|------|-------------|
| `sketch` | BINARY | A Theta sketch in binary format |

Returns a BINARY containing the intersected Theta sketch.

**Examples:**
```sql
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
FROM (
  SELECT theta_sketch_agg(col1) as sketch
  FROM VALUES (1), (2), (3), (4), (5), (6), (7) tab(col1)
  UNION ALL
  SELECT theta_sketch_agg(col2) as sketch
  FROM VALUES (5), (6), (7), (8), (9), (10), (11) tab(col2)
);
-- Result: 3 (values 5, 6, 7 are common)
```

---

### theta_sketch_estimate

Estimates the number of unique values from a Theta sketch.

**Syntax:**
```sql
theta_sketch_estimate(sketch)
```

| Argument | Type | Description |
|----------|------|-------------|
| `sketch` | BINARY | A Theta sketch in binary format |

Returns a BIGINT representing the estimated count of distinct values.

**Examples:**
```sql
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (1), (1), (2), (2), (3) tab(col);
-- Result: 3
```

---

### theta_union

Merges two Theta sketches using union (scalar function).

**Syntax:**
```sql
theta_union(first, second [, lgNomEntries])
```

| Argument | Type | Description |
|----------|------|-------------|
| `first` | BINARY | First Theta sketch |
| `second` | BINARY | Second Theta sketch |
| `lgNomEntries` | INT (optional) | Log-base-2 of nominal entries. Range: 4-26. Default: 12. |

Returns a BINARY containing the merged Theta sketch.

**Examples:**
```sql
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2)))
FROM VALUES (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) tab(col1, col2);
-- Result: 6
```

---

### theta_intersection

Computes the intersection of two Theta sketches (scalar function).

**Syntax:**
```sql
theta_intersection(first, second)
```

| Argument | Type | Description |
|----------|------|-------------|
| `first` | BINARY | First Theta sketch |
| `second` | BINARY | Second Theta sketch |

Returns a BINARY containing the intersected Theta sketch.

**Examples:**
```sql
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2)))
FROM VALUES (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) tab(col1, col2);
-- Result: 2 (values 1 and 5 are common)
```

---

### theta_difference

Computes the set difference of two Theta sketches (A - B).

**Syntax:**
```sql
theta_difference(first, second)
```

| Argument | Type | Description |
|----------|------|-------------|
| `first` | BINARY | First Theta sketch (A) |
| `second` | BINARY | Second Theta sketch (B) |

Returns a BINARY containing a Theta sketch representing values in A but not in B.

**Examples:**
```sql
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2)))
FROM VALUES (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) tab(col1, col2);
-- Result: 2 (values 2 and 3 are in col1 but not col2)
```

---

## KLL Quantile Sketch Functions

KLL (K-Linear-Logarithmic) sketches provide approximate quantile estimation. They are useful for computing percentiles, medians, and other order statistics on large datasets without sorting.

See the [Apache DataSketches KLL documentation](https://datasketches.apache.org/docs/KLL/KLLSketch.html) for more information.

KLL functions are type-specific to avoid precision loss:
- **BIGINT** variants: For integer types (TINYINT, SMALLINT, INT, BIGINT)
- **FLOAT** variants: For FLOAT values only
- **DOUBLE** variants: For FLOAT and DOUBLE values

### kll_sketch_agg_*

Creates a KLL sketch from numeric values for quantile estimation.

**Syntax:**
```sql
kll_sketch_agg_bigint(expr [, k])
kll_sketch_agg_float(expr [, k])
kll_sketch_agg_double(expr [, k])
```

| Argument | Type | Description |
|----------|------|-------------|
| `expr` | Numeric (see variants above) | The numeric column to summarize |
| `k` | INT (optional) | Controls accuracy and size. Range: 8-65535. Default: 200 (~1.65% normalized rank error). |

Returns a BINARY containing the KLL sketch in compact binary representation.

**Examples:**
```sql
-- Get median (0.5 quantile)
SELECT kll_sketch_get_quantile_bigint(kll_sketch_agg_bigint(col), 0.5)
FROM VALUES (1), (2), (3), (4), (5), (6), (7) tab(col);
-- Result: 4

-- With custom k for higher accuracy
SELECT kll_sketch_get_quantile_bigint(kll_sketch_agg_bigint(col, 400), 0.5)
FROM VALUES (1), (2), (3), (4), (5), (6), (7) tab(col);
-- Result: 4
```

**Notes:**
- Use the appropriate variant to avoid precision loss: use `_bigint` for integers, `_float` for floats, `_double` for doubles.
- NULL values are ignored during aggregation.

---

### kll_sketch_to_string_*

Returns a human-readable summary of the sketch.

**Syntax:**
```sql
kll_sketch_to_string_bigint(sketch)
kll_sketch_to_string_float(sketch)
kll_sketch_to_string_double(sketch)
```

| Argument | Type | Description |
|----------|------|-------------|
| `sketch` | BINARY | A KLL sketch of the corresponding type |

Returns a STRING containing a human-readable summary including sketch parameters and statistics.

---

### kll_sketch_get_n_*

Returns the number of items collected in the sketch.

**Syntax:**
```sql
kll_sketch_get_n_bigint(sketch)
kll_sketch_get_n_float(sketch)
kll_sketch_get_n_double(sketch)
```

| Argument | Type | Description |
|----------|------|-------------|
| `sketch` | BINARY | A KLL sketch of the corresponding type |

Returns a BIGINT representing the count of items in the sketch.

**Examples:**
```sql
SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(col))
FROM VALUES (1), (2), (3), (4), (5), (6), (7) tab(col);
-- Result: 7
```

---

### kll_sketch_merge_*

Merges two KLL sketches of the same type.

**Syntax:**
```sql
kll_sketch_merge_bigint(left, right)
kll_sketch_merge_float(left, right)
kll_sketch_merge_double(left, right)
```

| Argument | Type | Description |
|----------|------|-------------|
| `left` | BINARY | First KLL sketch |
| `right` | BINARY | Second KLL sketch (must be same type as left) |

Returns a BINARY containing the merged KLL sketch.

**Examples:**
```sql
-- Merge two sketches from different data partitions
SELECT kll_sketch_get_quantile_bigint(
  kll_sketch_merge_bigint(
    kll_sketch_agg_bigint(col1),
    kll_sketch_agg_bigint(col2)), 0.5)
FROM VALUES (1, 6), (2, 7), (3, 8), (4, 9), (5, 10) tab(col1, col2);
-- Result: approximately 5 (median of 1-10)
```

**Errors:**
- Throws an error if sketches are of incompatible types or formats.

---

### kll_sketch_get_quantile_*

Gets the approximate value at a given quantile rank.

**Syntax:**
```sql
kll_sketch_get_quantile_bigint(sketch, rank)
kll_sketch_get_quantile_float(sketch, rank)
kll_sketch_get_quantile_double(sketch, rank)
```

| Argument | Type | Description |
|----------|------|-------------|
| `sketch` | BINARY | A KLL sketch of the corresponding type |
| `rank` | DOUBLE or ARRAY&lt;DOUBLE&gt; | Quantile rank(s) between 0.0 and 1.0. Use 0.5 for median, 0.95 for 95th percentile, etc. |

Returns the approximate value at the given quantile:
- If `rank` is a scalar: Returns the corresponding type (BIGINT, FLOAT, or DOUBLE)
- If `rank` is an array: Returns ARRAY of the corresponding type

**Examples:**
```sql
-- Get the median
SELECT kll_sketch_get_quantile_bigint(kll_sketch_agg_bigint(col), 0.5)
FROM VALUES (1), (2), (3), (4), (5), (6), (7) tab(col);
-- Result: 4

-- Get multiple percentiles at once
SELECT kll_sketch_get_quantile_bigint(
  kll_sketch_agg_bigint(col),
  ARRAY(0.25, 0.5, 0.75, 0.95))
FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) tab(col);
-- Result: Array of values at 25th, 50th, 75th, and 95th percentiles
```

**Errors:**
- Throws an error if rank values are outside [0.0, 1.0].
- Returns NULL if the input sketch is NULL.

---

### kll_sketch_get_rank_*

Gets the normalized rank (0.0 to 1.0) of a given value in the sketch's distribution.

**Syntax:**
```sql
kll_sketch_get_rank_bigint(sketch, value)
kll_sketch_get_rank_float(sketch, value)
kll_sketch_get_rank_double(sketch, value)
```

| Argument | Type | Description |
|----------|------|-------------|
| `sketch` | BINARY | A KLL sketch of the corresponding type |
| `value` | Corresponding type (BIGINT, FLOAT, or DOUBLE) | The value to find the rank for |

Returns a DOUBLE representing the normalized rank between 0.0 and 1.0.

**Examples:**
```sql
-- Find what percentile the value 3 is at
SELECT kll_sketch_get_rank_bigint(kll_sketch_agg_bigint(col), 3)
FROM VALUES (1), (2), (3), (4), (5), (6), (7) tab(col);
-- Result: approximately 0.43 (3 is around the 43rd percentile)
```

---

## Approximate Top-K Functions

Top-K functions estimate the most frequent items (heavy hitters) in a dataset using the DataSketches Frequent Items sketch.

See the [Apache DataSketches Frequency documentation](https://datasketches.apache.org/docs/Frequency/FrequencySketches.html) for more information.

### approx_top_k_accumulate

Creates a sketch that can be stored and later combined or estimated. Useful for pre-aggregating data.

**Syntax:**
```sql
approx_top_k_accumulate(expr [, maxItemsTracked])
```

| Argument | Type | Description |
|----------|------|-------------|
| `expr` | Same as `approx_top_k` | The column to accumulate |
| `maxItemsTracked` | INT (optional) | Maximum items tracked. Range: 1 to 1,000,000. Default: 10,000. |

Returns a STRUCT containing a sketch state that can be passed to `approx_top_k_combine` or `approx_top_k_estimate`.

**Examples:**
```sql
-- Accumulate then estimate
SELECT approx_top_k_estimate(approx_top_k_accumulate(expr))
FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) tab(expr);
-- Result: [{"item":0,"count":2},{"item":4,"count":2},{"item":1,"count":2},{"item":2,"count":1},{"item":3,"count":1}]
```

---

### approx_top_k_combine

Combines multiple sketches into a single sketch.

**Syntax:**
```sql
approx_top_k_combine(state [, maxItemsTracked])
```

| Argument | Type | Description |
|----------|------|-------------|
| `state` | STRUCT | A sketch state from `approx_top_k_accumulate` or `approx_top_k_combine` |
| `maxItemsTracked` | INT (optional) | If specified, sets the combined sketch size. If not specified, all input sketches must have the same maxItemsTracked. |

Returns a STRUCT containing the combined sketch state.

**Examples:**
```sql
-- Combine sketches from different partitions
SELECT approx_top_k_estimate(approx_top_k_combine(sketch, 10000), 5)
FROM (
  SELECT approx_top_k_accumulate(expr) AS sketch
  FROM VALUES (0), (0), (1), (1) tab(expr)
  UNION ALL
  SELECT approx_top_k_accumulate(expr) AS sketch
  FROM VALUES (2), (3), (4), (4) tab(expr)
);
-- Result: [{"item":0,"count":2},{"item":4,"count":2},{"item":1,"count":2},{"item":2,"count":1},{"item":3,"count":1}]
```

**Errors:**
- Throws an error if input sketches have different `maxItemsTracked` values and no explicit value is provided.
- Throws an error if input sketches have different item data types.

---

### approx_top_k_estimate

Extracts the top K items from a sketch.

**Syntax:**
```sql
approx_top_k_estimate(state [, k])
```

| Argument | Type | Description |
|----------|------|-------------|
| `state` | STRUCT | A sketch state from `approx_top_k_accumulate` or `approx_top_k_combine` |
| `k` | INT (optional) | Number of top items to return. Default: 5. |

Returns an ARRAY&lt;STRUCT&lt;item, count&gt;&gt; containing the frequent items sorted by count descending.

**Examples:**
```sql
SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 2)
FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' tab(expr);
-- Result: [{"item":"c","count":4},{"item":"d","count":2}]
```

---

## Best Practices

### Choosing Between HLL and Theta Sketches

| Use Case | Recommended Sketch |
|----------|-------------------|
| Simple count distinct | HLL (more memory efficient) |
| Set operations (union, intersection, difference) | Theta |
| Very high cardinality with moderate accuracy | HLL with higher lgConfigK |
| Need to compute overlaps between datasets | Theta |

### Accuracy vs. Memory Trade-offs

| Sketch Type | Parameter | Effect of Increasing |
|-------------|-----------|---------------------|
| HLL | lgConfigK | Higher accuracy, more memory (2^lgConfigK bytes) |
| Theta | lgNomEntries | Higher accuracy, more memory (8 * 2^lgNomEntries bytes) |
| KLL | k | Higher accuracy, more memory |
| Top-K | maxItemsTracked | Better heavy-hitter detection, more memory |

### Storing and Reusing Sketches

Sketches can be stored in BINARY columns and later merged:

```sql
-- Create a table to store daily sketches
CREATE TABLE daily_user_sketches (
  date DATE,
  user_sketch BINARY
);

-- Insert daily sketches
INSERT INTO daily_user_sketches
SELECT current_date(), hll_sketch_agg(user_id)
FROM events;

-- Compute weekly unique users by merging daily sketches
SELECT hll_sketch_estimate(hll_union_agg(user_sketch))
FROM daily_user_sketches
WHERE date BETWEEN '2024-01-01' AND '2024-01-07';
```

---

## Common Use Cases and Examples

Sketches are particularly valuable for periodic ETL jobs where you need to maintain running statistics across multiple batches of data. The general workflow is:

1. **Aggregate** input values into a sketch using an aggregate function
2. **Store** the sketch (as BINARY) in a table
3. **Merge** new sketches with previously stored sketches
4. **Query** the final sketch to get approximate answers

### Example: Tracking Daily Unique Users with HLL Sketches

This example shows how to maintain a running count of unique users across daily batches.

```sql
-- Create a table to store daily HLL sketches
CREATE TABLE daily_user_sketches (
  event_date DATE,
  user_sketch BINARY
) USING PARQUET;

-- Day 1: Process first batch of events and store the sketch
INSERT INTO daily_user_sketches
SELECT 
  DATE'2024-01-01' as event_date,
  hll_sketch_agg(user_id) as user_sketch
FROM day1_events;

-- Day 2: Process second batch and store its sketch
INSERT INTO daily_user_sketches
SELECT 
  DATE'2024-01-02' as event_date,
  hll_sketch_agg(user_id) as user_sketch
FROM day2_events;

-- Query: Get unique users for a single day
SELECT 
  event_date,
  hll_sketch_estimate(user_sketch) as unique_users
FROM daily_user_sketches
WHERE event_date = DATE'2024-01-01';

-- Query: Get unique users across a date range (merging sketches)
SELECT hll_sketch_estimate(hll_union_agg(user_sketch)) as unique_users_in_week
FROM daily_user_sketches
WHERE event_date BETWEEN DATE'2024-01-01' AND DATE'2024-01-07';
```

### Example: Computing Percentiles Over Time with KLL Sketches

This example shows how to track response time percentiles across hourly batches.

```sql
-- Create a table to store hourly KLL sketches for response times
CREATE TABLE hourly_latency_sketches (
  hour_ts TIMESTAMP,
  latency_sketch BINARY
) USING PARQUET;

-- Process each hour's data and store the sketch
INSERT INTO hourly_latency_sketches
SELECT 
  DATE_TRUNC('hour', event_time) as hour_ts,
  kll_sketch_agg_bigint(response_time_ms) as latency_sketch
FROM hourly_events
GROUP BY DATE_TRUNC('hour', event_time);

-- Query: Get p50, p95, p99 for a specific hour
SELECT 
  hour_ts,
  kll_sketch_get_quantile_bigint(latency_sketch, 0.5) as p50_ms,
  kll_sketch_get_quantile_bigint(latency_sketch, 0.95) as p95_ms,
  kll_sketch_get_quantile_bigint(latency_sketch, 0.99) as p99_ms
FROM hourly_latency_sketches
WHERE hour_ts = TIMESTAMP'2024-01-15 14:00:00';

-- Query: Get percentiles across a full day by merging hourly sketches
WITH daily_sketch AS (
  SELECT kll_sketch_merge_bigint(
    FIRST(latency_sketch),
    COALESCE(
      AGGREGATE(SLICE(COLLECT_LIST(latency_sketch), 2, 999999), 
                FIRST(latency_sketch), 
                (acc, x) -> kll_sketch_merge_bigint(acc, x)),
      FIRST(latency_sketch)
    )
  ) as merged_sketch
  FROM hourly_latency_sketches
  WHERE DATE(hour_ts) = DATE'2024-01-15'
)
SELECT 
  kll_sketch_get_quantile_bigint(merged_sketch, 0.5) as p50_ms,
  kll_sketch_get_quantile_bigint(merged_sketch, 0.95) as p95_ms,
  kll_sketch_get_quantile_bigint(merged_sketch, 0.99) as p99_ms
FROM daily_sketch;
```

### Example: Set Operations with Theta Sketches

Theta sketches support set operations, making them useful for analyzing overlapping populations.

```sql
-- Create sketches for users who performed different actions
CREATE TABLE action_sketches (
  action_type STRING,
  user_sketch BINARY
) USING PARQUET;

-- Store sketches for each action type
INSERT INTO action_sketches
SELECT 'purchase', theta_sketch_agg(user_id) FROM purchases;

INSERT INTO action_sketches
SELECT 'add_to_cart', theta_sketch_agg(user_id) FROM cart_additions;

INSERT INTO action_sketches
SELECT 'page_view', theta_sketch_agg(user_id) FROM page_views;

-- Query: How many users purchased?
SELECT theta_sketch_estimate(user_sketch) as purchasers
FROM action_sketches WHERE action_type = 'purchase';

-- Query: How many users added to cart but did NOT purchase?
SELECT theta_sketch_estimate(
  theta_difference(
    (SELECT user_sketch FROM action_sketches WHERE action_type = 'add_to_cart'),
    (SELECT user_sketch FROM action_sketches WHERE action_type = 'purchase')
  )
) as cart_abandoners;

-- Query: How many users both viewed pages AND purchased (intersection)?
SELECT theta_sketch_estimate(
  theta_intersection(
    (SELECT user_sketch FROM action_sketches WHERE action_type = 'page_view'),
    (SELECT user_sketch FROM action_sketches WHERE action_type = 'purchase')
  )
) as engaged_purchasers;
```

### Example: Finding Trending Items with Top-K Sketches

Track the most frequently occurring items across batches.

```sql
-- Create a table to store hourly top-k sketches
CREATE TABLE hourly_search_sketches (
  hour_ts TIMESTAMP,
  search_sketch STRUCT<sketch: BINARY, maxItemsTracked: INT, itemDataType: STRING, itemDataTypeDDL: STRING>
) USING PARQUET;

-- Process each hour's search queries
INSERT INTO hourly_search_sketches
SELECT 
  DATE_TRUNC('hour', search_time) as hour_ts,
  approx_top_k_accumulate(search_term, 10000) as search_sketch
FROM search_logs
GROUP BY DATE_TRUNC('hour', search_time);

-- Query: Get top 10 searches for a specific hour
SELECT approx_top_k_estimate(search_sketch, 10) as top_searches
FROM hourly_search_sketches
WHERE hour_ts = TIMESTAMP'2024-01-15 14:00:00';

-- Query: Get top 10 searches across the full day by combining sketches
SELECT approx_top_k_estimate(
  approx_top_k_combine(search_sketch, 10000), 
  10
) as daily_top_searches
FROM hourly_search_sketches
WHERE DATE(hour_ts) = DATE'2024-01-15';
```
