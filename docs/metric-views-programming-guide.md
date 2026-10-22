---
layout: global
title: Metric Views Programming Guide
displayTitle: Metric Views Programming Guide
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

* Table of contents
{:toc}

## What is a Metric View?

A metric view is a view that defines business metrics once, as aggregate expressions over a source table or query, so that every query can ask for them at whatever level of detail it needs without restating the aggregation. The definition names the **dimensions** a query may group and filter by (region, day, product) and the **measures** it may ask for (revenue, distinct customers, a return rate). A query reads a measure with the `MEASURE()` aggregate function, and Spark evaluates the measure's expression over the source rows, grouped by whatever the query groups by.

Consider a `return_rate` defined as returned orders divided by all orders. Written by hand, it has to be restated in every report (`SUM(CAST(returned AS INT)) / COUNT(1)`), and a report that averages per-region rates to get a company-wide rate gets a different, wrong, answer. In a metric view, the ratio is written once as a measure. `SELECT MEASURE(return_rate) FROM order_metrics` returns the company-wide rate; add `GROUP BY region` and it returns each region's rate, computed from that region's rows. Same for a distinct count: `COUNT(DISTINCT customer_id)` counts each customer once at whichever grain the query asks for, and never sums daily counts into an inflated monthly one.

Metric views are ordinary catalog objects. They are created with `CREATE VIEW ... WITH METRICS LANGUAGE YAML`, listed by `SHOW VIEWS`, described by `DESCRIBE`, and dropped by `DROP VIEW`. They store no data: each query is rewritten into an aggregation over the source and runs as any Spark SQL query does. Any client that sends SQL to Spark, whether `spark-sql`, Spark Connect, or a BI tool over the Thrift JDBC/ODBC server, can query them with plain SQL.

Metric views are available since Spark 4.2.0. The statement is documented on the [CREATE METRIC VIEW](sql-ref-syntax-ddl-create-metric-view.html) reference page.

## Quick Start

Create a table of orders, define a metric view over it, and query the view at three levels of detail.

```sql
CREATE TABLE orders (
    region STRING,
    order_date DATE,
    customer_id INT,
    amount DECIMAL(10, 2),
    returned BOOLEAN) USING parquet;

INSERT INTO orders VALUES
    ('east', DATE'2026-01-03', 1, 120.00, false),
    ('east', DATE'2026-01-03', 2,  80.00, true),
    ('east', DATE'2026-01-04', 1,  45.50, false),
    ('west', DATE'2026-01-03', 3, 300.00, false),
    ('west', DATE'2026-01-04', 3,  60.00, false),
    ('west', DATE'2026-01-04', 4,  99.99, true),
    ('west', DATE'2026-01-05', 5,  10.00, false),
    ('west', DATE'2026-01-05', 6,  40.00, false),
    ('west', DATE'2026-01-05', 3,  25.00, false);

CREATE VIEW order_metrics
    WITH METRICS
    LANGUAGE YAML
    AS $$
version: 0.1
source: orders
dimensions:
  - name: region
    expr: region
  - name: order_date
    expr: order_date
measures:
  - name: order_count
    expr: COUNT(1)
  - name: revenue
    expr: SUM(amount)
  - name: customers
    expr: COUNT(DISTINCT customer_id)
  - name: return_rate
    expr: AVG(CAST(returned AS INT))
$$;
```

Without a `GROUP BY`, each measure is evaluated over every order:

```sql
SELECT MEASURE(revenue) AS revenue, MEASURE(customers) AS customers FROM order_metrics;
+-------+---------+
|revenue|customers|
+-------+---------+
|780.49 |6        |
+-------+---------+
```

Group by a dimension and the same measures are evaluated once per group:

```sql
SELECT region, MEASURE(revenue) AS revenue, MEASURE(customers) AS customers
FROM order_metrics
GROUP BY region
ORDER BY region;
+------+-------+---------+
|region|revenue|customers|
+------+-------+---------+
|east  |245.50 |2        |
|west  |534.99 |4        |
+------+-------+---------+
```

Filter by one dimension and group by another:

```sql
SELECT order_date, MEASURE(revenue) AS revenue, MEASURE(customers) AS customers
FROM order_metrics
WHERE region = 'west'
GROUP BY order_date
ORDER BY order_date;
+----------+-------+---------+
|order_date|revenue|customers|
+----------+-------+---------+
|2026-01-03|300.00 |1        |
|2026-01-04|159.99 |2        |
|2026-01-05|75.00  |3        |
+----------+-------+---------+
```

Customer 3 ordered on each of the three days. Each daily count counts them once, and the regional count of 4 counts them once, because each query evaluates `COUNT(DISTINCT customer_id)` over its own groups.

## Key Concepts

### Source

The source is the relation the measures aggregate over. It is either the name of a table or view, or a SQL query. A query source can join tables, derive columns, or aggregate another metric view (see [Modeling Patterns](#modeling-patterns)). The source's columns are not exposed by the view; a query sees only dimensions and measures.

### Dimensions

A dimension is a named, non-aggregate expression over the source: a column, or an expression such as `date_format(order_date, 'yyyy-MM')`. Dimensions are the columns a query may select, group by, filter on, and order by.

### Measures

A measure is a named aggregate expression over the source, such as `SUM(amount)`, `COUNT(DISTINCT customer_id)`, or a ratio of aggregates such as `SUM(amount) / COUNT(1)`. A measure has no value of its own. A query reads it with `MEASURE(name)`, and only inside an aggregation.

### Filter

An optional boolean expression over the source, applied to every query before the query's own `WHERE`. It can use source columns that are not dimensions, which keeps rows out of the view without exposing the column.

### Grain

The grain of a query is the set of dimensions it groups by. A measure is always evaluated at the grain of the query that calls `MEASURE()`: Spark replaces each `MEASURE(name)` in the query's `SELECT` list with the measure's expression, and the resulting aggregation runs over the source rows, grouped by the query's `GROUP BY`. `EXPLAIN` shows the rewrite:

```sql
EXPLAIN SELECT region, MEASURE(revenue) AS revenue FROM order_metrics GROUP BY region;
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[region#22], functions=[sum(amount#25)])
   +- Exchange hashpartitioning(region#22, 2), ENSURE_REQUIREMENTS, [plan_id=886]
      +- HashAggregate(keys=[region#22], functions=[partial_sum(amount#25)])
         +- FileScan parquet spark_catalog.default.orders[region#22,amount#25] ...
```

This is what makes non-additive measures safe to ask for at any grain. The distinct customer count over all orders is 6:

```sql
SELECT MEASURE(customers) AS customers FROM order_metrics;
+---------+
|customers|
+---------+
|6        |
+---------+
```

The output of `MEASURE()` is an ordinary column to an enclosing query. Summing the daily counts in an outer query is ordinary arithmetic over the inner result, and counts a customer once per day they ordered:

```sql
SELECT SUM(customers) AS summed_daily_customers
FROM (SELECT order_date, MEASURE(customers) AS customers
      FROM order_metrics
      GROUP BY order_date);
+----------------------+
|summed_daily_customers|
+----------------------+
|9                     |
+----------------------+
```

Likewise for a rate. The regional rates and their average:

```sql
SELECT region, ROUND(MEASURE(return_rate), 2) AS return_rate
FROM order_metrics
GROUP BY region
ORDER BY region;
+------+-----------+
|region|return_rate|
+------+-----------+
|east  |0.33       |
|west  |0.17       |
+------+-----------+

SELECT ROUND(AVG(return_rate), 2) AS average_of_region_rates
FROM (SELECT region, MEASURE(return_rate) AS return_rate
      FROM order_metrics
      GROUP BY region);
+-----------------------+
|average_of_region_rates|
+-----------------------+
|0.25                   |
+-----------------------+
```

The average of the two regional rates is 0.25. The overall rate is 0.22, two returned orders out of nine, because west has twice as many orders as east and its lower rate carries more weight. The average of rates is a different number from the rate, so ask for a measure at the grain you report, rather than computing it at a finer grain and aggregating the results.

```sql
SELECT ROUND(MEASURE(return_rate), 2) AS return_rate FROM order_metrics;
+-----------+
|return_rate|
+-----------+
|0.22       |
+-----------+
```

## Defining Metric Views in SQL

A metric view is created with `CREATE VIEW ... WITH METRICS LANGUAGE YAML`. The body is a YAML document inside a dollar-quoted string, `$$ ... $$`. The document has these keys and no others:

| Key | Required | Content |
|---|---|---|
| `version` | yes | The definition format version. `0.1` is the only version accepted. |
| `source` | yes | A table or view name, optionally qualified, or a SQL query. |
| `filter` | no | A boolean expression over the source, applied to every query. |
| `dimensions` | no | A list of `{ name, expr }` entries; `expr` is a non-aggregate expression. |
| `measures` | no | A list of `{ name, expr }` entries; `expr` is an aggregate expression. |

Each dimension and measure takes only `name` and `expr`. Names must be unique across both lists. The view's columns are the dimensions followed by the measures, and their types are derived when the view is created: Spark analyzes an aggregation over the source grouped by every dimension, which is why `COUNT` measures are `BIGINT`, `SUM` of a `DECIMAL(10, 2)` is `DECIMAL(20, 2)`, and `AVG` is `DOUBLE`.

```sql
DESCRIBE order_metrics;
+-----------+-------------+-------+
|col_name   |data_type    |comment|
+-----------+-------------+-------+
|region     |string       |NULL   |
|order_date |date         |NULL   |
|order_count|bigint       |NULL   |
|revenue    |decimal(20,2)|NULL   |
|customers  |bigint       |NULL   |
|return_rate|double       |NULL   |
+-----------+-------------+-------+
```

### A Table as the Source

The Quick Start view names a table. The name may be qualified (`spark_catalog.default.orders`). A persistent metric view cannot read a temporary view or a session variable, for the same reason an ordinary persistent view cannot: the definition outlives the session.

### A SQL Query as the Source, with a Filter

A query source lets the definition derive columns that the table does not have, and a filter keeps rows out of every query. Here the source classifies each order as large or small, and the filter excludes returned orders:

```sql
CREATE VIEW kept_order_metrics
    WITH METRICS
    LANGUAGE YAML
    AS $$
version: 0.1
source: |
  SELECT o.*, CASE WHEN amount >= 100 THEN 'large' ELSE 'small' END AS size
  FROM orders o
filter: NOT returned
dimensions:
  - name: region
    expr: region
  - name: size
    expr: size
measures:
  - name: revenue
    expr: SUM(amount)
$$;

SELECT region, size, MEASURE(revenue) AS revenue
FROM kept_order_metrics
GROUP BY region, size
ORDER BY region, size;
+------+-----+-------+
|region|size |revenue|
+------+-----+-------+
|east  |large|120.00 |
|east  |small|45.50  |
|west  |large|300.00 |
|west  |small|135.00 |
+------+-----+-------+
```

The YAML `|` block scalar keeps a multi-line query readable. `returned` is not a dimension, so no query can select it, but the filter can use it.

### Column Names, Comments and Properties

The statement accepts the clauses an ordinary `CREATE VIEW` does. A column list renames the view's columns in definition order (dimensions, then measures) and may comment them; `COMMENT` describes the view; `TBLPROPERTIES` attaches metadata. A query uses the names from the column list.

```sql
CREATE VIEW sales_metrics
    (sales_region COMMENT 'Region of the order', order_date, orders, revenue)
    WITH METRICS
    LANGUAGE YAML
    COMMENT 'Order counts and revenue by region and day'
    TBLPROPERTIES ('team' = 'sales')
    AS $$
version: 0.1
source: orders
dimensions:
  - name: region
    expr: region
  - name: order_date
    expr: order_date
measures:
  - name: order_count
    expr: COUNT(1)
  - name: revenue
    expr: SUM(amount)
$$;

DESCRIBE sales_metrics;
+------------+-------------+-------------------+
|col_name    |data_type    |comment            |
+------------+-------------+-------------------+
|sales_region|string       |Region of the order|
|order_date  |date         |NULL               |
|orders      |bigint       |NULL               |
|revenue     |decimal(20,2)|NULL               |
+------------+-------------+-------------------+

SELECT sales_region, MEASURE(orders) AS orders FROM sales_metrics GROUP BY sales_region ORDER BY sales_region;
+------------+------+
|sales_region|orders|
+------------+------+
|east        |3     |
|west        |6     |
+------------+------+
```

## Querying Metric Views

### In SQL

A query over a metric view is an aggregate query. The rules follow from the rewrite described under [Grain](#grain):

- Read a measure with `MEASURE(name)` in the `SELECT` list, optionally inside another expression (`ROUND(MEASURE(return_rate), 2)`). A measure selected by name alone, or `SELECT *`, has nothing to return and fails.
- Every dimension in the `SELECT` list must be in `GROUP BY` (`GROUP BY ALL` works). A query with only `MEASURE()` calls needs no `GROUP BY`.
- Filter with `WHERE` on dimensions. Source columns that are not dimensions are not part of the view; to filter on one, use the view's `filter` key or a query source.
- To filter or sort on a measure, alias it in the `SELECT` list and use the alias in `HAVING` and `ORDER BY`. `MEASURE()` itself is not resolved in `WHERE`, `HAVING` or `ORDER BY`.
- `COUNT(*)` counts source rows (after the view's filter); it does not go through `MEASURE()`.
- To combine a metric view with another table, aggregate the metric view first, in a subquery or common table expression, and join the result.

```sql
-- HAVING and ORDER BY use the alias.
SELECT region, MEASURE(revenue) AS revenue
FROM order_metrics
GROUP BY region
HAVING revenue > 300
ORDER BY revenue DESC;
+------+-------+
|region|revenue|
+------+-------+
|west  |534.99 |
+------+-------+

-- Aggregate first, then join.
WITH by_region AS (
    SELECT region, MEASURE(revenue) AS revenue
    FROM order_metrics
    GROUP BY region)
SELECT r.region, r.revenue, o.customer_id
FROM by_region r JOIN orders o ON r.region = o.region AND o.returned
ORDER BY r.region;
+------+-------+-----------+
|region|revenue|customer_id|
+------+-------+-----------+
|east  |245.50 |2          |
|west  |534.99 |4          |
+------+-------+-----------+

-- Dimensions alone, and the source row count.
SELECT region FROM order_metrics GROUP BY region ORDER BY region;
+------+
|region|
+------+
|east  |
|west  |
+------+

SELECT COUNT(*) AS source_rows FROM order_metrics;
+-----------+
|source_rows|
+-----------+
|9          |
+-----------+
```

### In Python

`spark.table()` reads a metric view like any table. `MEASURE()` is reached through SQL expressions, with `F.expr` inside `agg` or with `selectExpr`, and the SQL rules carry over: a measure needs `MEASURE()` inside an aggregation, dimension filters go in `where` before the aggregation, and measure filters go in `where` after it, on the alias.

```python
from pyspark.sql import functions as F

metrics = spark.table("order_metrics")
metrics.columns
# ['region', 'order_date', 'order_count', 'revenue', 'customers', 'return_rate']

metrics.groupBy("region").agg(
    F.expr("MEASURE(revenue)").alias("revenue"),
    F.expr("MEASURE(customers)").alias("customers"),
).orderBy("region").show()
# +------+-------+---------+
# |region|revenue|customers|
# +------+-------+---------+
# |  east| 245.50|        2|
# |  west| 534.99|        4|
# +------+-------+---------+

metrics.selectExpr("MEASURE(revenue) AS revenue", "MEASURE(customers) AS customers").show()
# +-------+---------+
# |revenue|customers|
# +-------+---------+
# | 780.49|        6|
# +-------+---------+

# A dimension filter before the aggregation.
metrics.where(F.col("region") == "west").groupBy("order_date").agg(
    F.expr("MEASURE(revenue)").alias("revenue")
).orderBy("order_date").show()
# +----------+-------+
# |order_date|revenue|
# +----------+-------+
# |2026-01-03| 300.00|
# |2026-01-04| 159.99|
# |2026-01-05|  75.00|
# +----------+-------+

# A measure filter after the aggregation, on the alias.
metrics.groupBy("region").agg(F.expr("MEASURE(revenue)").alias("revenue")).where(F.col("revenue") > 300).show()
# +------+-------+
# |region|revenue|
# +------+-------+
# |  west| 534.99|
# +------+-------+

metrics.count()
# 9
```

`metrics.show()` and `metrics.select("revenue")` fail, as `SELECT *` and a bare measure do in SQL, and `metrics.filter(F.expr("MEASURE(revenue) > 300"))` is rejected with `INVALID_WHERE_CONDITION`.

### From BI and JDBC Clients

A metric view needs nothing beyond SQL, so any client that submits SQL to Spark can use one: the Thrift JDBC/ODBC server, Spark Connect, or a BI tool's custom-SQL query. Dashboards that group by different dimensions issue the same `SELECT ... MEASURE(...) ... GROUP BY ...` shape, and each gets the measure at its own grain.

## Modeling Patterns

### Ratios and Rates as Single Measures

A ratio belongs in one measure, as a ratio of aggregates, so every grain computes it from that grain's totals. Rounding can live in the definition as well. `ROUND(SUM(amount) / COUNT(1), 2)` gives `avg_order_value` a `DECIMAL(21, 2)` type instead of the wide decimal the bare division produces. This definition replaces the Quick Start view, so drop that one first (see [Changing a Definition](#changing-a-definition)).

```sql
DROP VIEW order_metrics;

CREATE VIEW order_metrics
    WITH METRICS
    LANGUAGE YAML
    AS $$
version: 0.1
source: orders
dimensions:
  - name: region
    expr: region
  - name: order_date
    expr: order_date
  - name: order_month
    expr: date_format(order_date, 'yyyy-MM')
  - name: weekday
    expr: date_format(order_date, 'EEE')
measures:
  - name: order_count
    expr: COUNT(1)
  - name: revenue
    expr: SUM(amount)
  - name: customers
    expr: COUNT(DISTINCT customer_id)
  - name: return_rate
    expr: AVG(CAST(returned AS INT))
  - name: avg_order_value
    expr: ROUND(SUM(amount) / COUNT(1), 2)
  - name: revenue_per_customer
    expr: ROUND(SUM(amount) / COUNT(DISTINCT customer_id), 2)
$$;

SELECT region, MEASURE(avg_order_value) AS avg_order_value, MEASURE(revenue_per_customer) AS revenue_per_customer
FROM order_metrics
GROUP BY region
ORDER BY region;
+------+---------------+--------------------+
|region|avg_order_value|revenue_per_customer|
+------+---------------+--------------------+
|east  |81.83          |122.75              |
|west  |89.17          |133.75              |
+------+---------------+--------------------+
```

A measure's expression resolves against the source's columns, not against other measures. `revenue_per_customer` repeats `SUM(amount)` rather than referring to `revenue`; a measure written as `revenue / COUNT(1)` is accepted at creation but fails to resolve when queried.

### Date Dimensions

A dimension can be any non-aggregate expression, so a date column can be exposed at several calendar grains without a source query. The definition in [Ratios and Rates as Single Measures](#ratios-and-rates-as-single-measures) adds `order_month` and `weekday` from `order_date`.

```sql
SELECT weekday, MEASURE(order_count) AS orders, MEASURE(avg_order_value) AS avg_order_value
FROM order_metrics
GROUP BY weekday
ORDER BY weekday;
+-------+------+---------------+
|weekday|orders|avg_order_value|
+-------+------+---------------+
|Mon    |3     |25.00          |
|Sat    |3     |166.67         |
|Sun    |3     |68.50          |
+-------+------+---------------+

SELECT order_month, MEASURE(revenue) AS revenue, MEASURE(revenue_per_customer) AS revenue_per_customer
FROM order_metrics
GROUP BY order_month;
+-----------+-------+--------------------+
|order_month|revenue|revenue_per_customer|
+-----------+-------+--------------------+
|2026-01    |780.49 |130.08              |
+-----------+-------+--------------------+
```

### Derived Dimensions in a SQL Source

When a dimension needs more than an expression over one row, such as a join to a lookup table or a window over the source, put it in a query source, as `size` is in [kept_order_metrics](#a-sql-query-as-the-source-with-a-filter). The query runs as part of every query over the view, so keep it to what the dimensions and measures need.

### A Metric View over a Metric View

A query source may aggregate another metric view with `MEASURE()`. This is the way to define metrics about groups, such as the average revenue of a region, where the inner grain is fixed by the source and the outer measures aggregate over those groups:

```sql
CREATE VIEW region_metrics
    WITH METRICS
    LANGUAGE YAML
    AS $$
version: 0.1
source: |
  SELECT region, MEASURE(revenue) AS revenue, MEASURE(customers) AS customers
  FROM order_metrics
  GROUP BY region
measures:
  - name: regions
    expr: COUNT(1)
  - name: avg_region_revenue
    expr: ROUND(AVG(revenue), 2)
  - name: max_region_customers
    expr: MAX(customers)
$$;

SELECT MEASURE(regions) AS regions,
       MEASURE(avg_region_revenue) AS avg_region_revenue,
       MEASURE(max_region_customers) AS max_region_customers
FROM region_metrics;
+-------+------------------+--------------------+
|regions|avg_region_revenue|max_region_customers|
+-------+------------------+--------------------+
|2      |390.25            |4                   |
+-------+------------------+--------------------+
```

## Lifecycle

### Changing a Definition

There is no `ALTER VIEW` form for the YAML body. In a Data Source V2 catalog, `CREATE OR REPLACE VIEW ... WITH METRICS` replaces an existing metric view. In the session catalog (`spark_catalog`), `OR REPLACE` does not yet replace an existing metric view: the statement fails with `TABLE_OR_VIEW_ALREADY_EXISTS` and the definition is unchanged, so drop the view and create it again.

```sql
DROP VIEW order_metrics;

CREATE VIEW order_metrics
    WITH METRICS
    LANGUAGE YAML
    AS $$
version: 0.1
source: orders
dimensions:
  - name: region
    expr: region
measures:
  - name: revenue
    expr: SUM(amount)
$$;
```

### Inspecting a Definition

`DESCRIBE EXTENDED` shows the view's type, the YAML document as `View Text`, and properties that record the source and filter: `metric_view.from.type` is `ASSET` for a named table or view and `SQL` for a query, with the name or query in `metric_view.from.name` or `metric_view.from.sql`, and `metric_view.where` holds the filter. `SHOW CREATE TABLE` is not supported on a metric view; `View Text` is the way to read a definition back.

```sql
DESCRIBE EXTENDED order_metrics;
+----------------------------+--------------------------------------------------------------------------------------------------------------------------------------+-------+
|col_name                    |data_type                                                                                                                             |comment|
+----------------------------+--------------------------------------------------------------------------------------------------------------------------------------+-------+
|region                      |string                                                                                                                                |NULL   |
|revenue                     |decimal(20,2)                                                                                                                         |NULL   |
|                            |                                                                                                                                      |       |
|# Detailed Table Information|                                                                                                                                      |       |
|Catalog                     |spark_catalog                                                                                                                         |       |
|Database                    |default                                                                                                                               |       |
|Table                       |order_metrics                                                                                                                         |       |
|...                         |                                                                                                                                      |       |
|Type                        |METRIC_VIEW                                                                                                                           |       |
|View Text                   |\nversion: 0.1\nsource: orders\ndimensions:\n  - name: region\n    expr: region\nmeasures:\n  - name: revenue\n    expr: SUM(amount)\n|       |
|Language                    |YAML                                                                                                                                  |       |
|Table Properties            |[metric_view.from.name=orders, metric_view.from.type=ASSET]                                                                           |       |
+----------------------------+--------------------------------------------------------------------------------------------------------------------------------------+-------+
```

### Renaming, Properties and Dropping

`ALTER VIEW ... RENAME TO`, `SET TBLPROPERTIES` and `UNSET TBLPROPERTIES` are the `ALTER VIEW` forms that apply to a metric view; `ALTER VIEW ... AS` is for SQL views and should not be used on one. `DROP VIEW` removes the view. A metric view keeps no data, so dropping it removes only the definition.

```sql
ALTER VIEW order_metrics SET TBLPROPERTIES ('team' = 'finance');
ALTER VIEW order_metrics RENAME TO order_metrics_2026;

SELECT region, MEASURE(revenue) AS revenue FROM order_metrics_2026 GROUP BY region ORDER BY region;
+------+-------+
|region|revenue|
+------+-------+
|east  |245.50 |
|west  |534.99 |
+------+-------+

SHOW VIEWS;
+---------+------------------+-----------+
|namespace|viewName          |isTemporary|
+---------+------------------+-----------+
|default  |order_metrics_2026|false      |
+---------+------------------+-----------+

DROP VIEW order_metrics_2026;
```

## Considerations

- **No joins in a query over a metric view.** Joining a metric view with another relation in the same query is not supported. Aggregate it in a subquery, a common table expression, or an ordinary view, and join the result.
- **No temporary form.** `CREATE TEMPORARY VIEW ... WITH METRICS` is a syntax error. A metric view is always persisted in the catalog, and its source cannot be a temporary view or a session variable.
- **A measure cannot reference a measure.** Each measure's expression resolves against the source's columns. Repeat the aggregate, or move the shared expression into a query source.
- **Only `name` and `expr` on a column, and only `version`, `source`, `filter`, `dimensions` and `measures` at the top level.** There is no key for a display name, a format, a description, or a join; unknown keys are rejected.
- **No `ALTER` of the YAML body.** Replace the view instead; in the session catalog, drop and recreate it.
- **`SHOW CREATE TABLE` is not supported.** Use `DESCRIBE EXTENDED` and read `View Text`.
- **`MEASURE()` applies only to a metric view's measures.** Applying it to a column of a table or an ordinary view is not supported.
