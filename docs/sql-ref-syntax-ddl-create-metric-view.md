---
layout: global
title: CREATE METRIC VIEW
displayTitle: CREATE METRIC VIEW
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

### Description

A metric view is a persistent view whose definition is a YAML document rather than a query.
The document names a source (a table, a view, or a SQL query), an optional filter, a list of
**dimensions** (expressions a query can group and filter by) and a list of **measures**
(aggregate expressions). A query reads a measure with the `MEASURE()` aggregate function, and
Spark evaluates the measure's expression over the source at the grain of that query's `GROUP BY`,
so one definition serves every level of aggregation. `CREATE VIEW ... WITH METRICS` creates a
metric view; it stores only metadata, and `ALTER VIEW` and `DROP VIEW` change only metadata.

Metric views are always persistent: there is no `TEMPORARY` form.

### Syntax

```sql
CREATE [ OR REPLACE ] VIEW [ IF NOT EXISTS ] view_identifier
    [ ( column_name [ COMMENT column_comment ], ... ) ]
    WITH METRICS
    LANGUAGE YAML
    [ COMMENT view_comment ]
    [ TBLPROPERTIES ( property_name = property_value [ , ... ] ) ]
    AS $$ yaml_definition $$
```

### Parameters

* **OR REPLACE**

    If a view of the same name already exists, it is replaced. The existing object must be a view;
    `CREATE OR REPLACE VIEW ... WITH METRICS` on a table fails with
    `EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE` and leaves the table in place. A replacement whose
    source reads the view being replaced, directly or through another view, is rejected with
    `RECURSIVE_VIEW`, and a replacement that fails analysis for any other reason leaves the
    existing definition in place. `OR REPLACE` cannot be combined with `IF NOT EXISTS`
    (`CREATE_VIEW_WITH_IF_NOT_EXISTS_AND_REPLACE`).

* **IF NOT EXISTS**

    Creates the view only if no table or view of that name exists. When one does, the statement
    succeeds and the existing definition is kept.

* **view_identifier**

    Specifies a view name, optionally qualified with a database name (or a catalog and database).
    If the name is not qualified the view is created in the current schema.

    **Syntax:** `[ catalog_name. ] [ database_name. ] view_name`

* **( column_name [ COMMENT column_comment ], ... )**

    Optional names and comments for the view's columns, in the order the definition lists them:
    every dimension first, then every measure. The list must name exactly as many columns as the
    definition declares, or the statement fails with `CREATE_VIEW_COLUMN_ARITY_MISMATCH`. A query
    refers to a dimension or measure by the name in this list, not by its name in the YAML
    document.

* **WITH METRICS**

    Marks the view as a metric view. This clause is required; without it the statement is an
    ordinary `CREATE VIEW` and the `$$ ... $$` body is a syntax error.

* **LANGUAGE YAML**

    The language of the body. This clause is required (`MISSING_CLAUSES_FOR_OPERATION` when it is
    missing) and `YAML` is the only language accepted. `WITH METRICS`, `LANGUAGE`, `COMMENT` and
    `TBLPROPERTIES` may appear in any order.

* **COMMENT view_comment**

    A `STRING` literal describing the view.

* **TBLPROPERTIES ( property_name = property_value [ , ... ] )**

    Metadata key-value pairs stored with the view. Spark adds its own `metric_view.*` properties,
    described under [DESCRIBE EXTENDED](#what-describe-shows) below.

* **$$ yaml_definition $$**

    The definition, as a dollar-quoted string literal. A single-quoted string is not accepted. The
    document has these keys and no others; any other key, at the top level or on a column, fails
    with `INVALID_METRIC_VIEW_YAML`:

    * **version**

      Required. The version of the definition format. `0.1` is the only version accepted; any
      other value fails with `INVALID_METRIC_VIEW_YAML` and the message lists the supported
      versions.

    * **source**

      Required. Either the name of a table or view, optionally qualified, or a SQL query. Spark
      first tries to read the value as a multi-part identifier and otherwise parses it as a query.
      A query source may join tables, use subqueries, or read another metric view with
      `MEASURE()`.

      A persistent metric view cannot reference temporary views, temporary functions, or session
      variables; such a source fails with `INVALID_TEMP_OBJ_REFERENCE`. A variable read through an
      `IDENTIFIER` clause in the source is rejected the same way unless
      `spark.sql.legacy.allowSessionVariableInPersistedView` is set.

    * **filter**

      Optional. A boolean expression over the source's columns, applied before any query's own
      `WHERE` clause. It may reference source columns that are not dimensions.

    * **dimensions**

      Optional. A list of `{ name, expr }` entries. `expr` is any non-aggregate SQL expression
      over the source's columns. A dimension that contains an aggregate function fails with
      `GROUP_BY_AGGREGATE`.

    * **measures**

      Optional. A list of `{ name, expr }` entries. `expr` is a SQL aggregate expression over the
      source's columns, such as `SUM(amount)` or `SUM(amount) / COUNT(1)`. A measure whose
      expression does not aggregate fails with `MISSING_AGGREGATION` (or `MISSING_GROUP_BY` when
      the definition has no dimensions). A measure's expression
      resolves against the source's columns only: it cannot refer to another measure by name, and
      it cannot call `MEASURE()`.

      Dimension and measure names must be unique across both lists (`COLUMN_ALREADY_EXISTS`).

    Column types are not declared. When the view is created, Spark analyzes an aggregation over
    the source, grouped by every dimension and selecting every dimension and measure, and records
    the resulting types. This is why `COUNT` measures are `BIGINT`, `SUM` of a `DECIMAL(10, 2)` is
    `DECIMAL(20, 2)`, and `AVG` is `DOUBLE`.

### Querying a Metric View

The columns of a metric view are its dimensions and measures, in that order; the source's other
columns are not part of the view. In a query:

* A **measure** is read only through the `MEASURE(measure_name)` aggregate function in the
  `SELECT` list of an aggregate query, where it may be nested inside other expressions such as
  `ROUND(MEASURE(rate), 2)`. Spark replaces each call with the measure's expression and evaluates
  it over the source rows that survive the definition's `filter` and the query's `WHERE`, grouped
  by the query's `GROUP BY`. Without `GROUP BY`, a measure is evaluated over the whole source.
  Selecting a measure's name directly, or `SELECT *`, fails, because a measure holds no value
  outside `MEASURE()`.
* `MEASURE()` is not resolved in `WHERE`, `HAVING` or `ORDER BY`. Give the measure an alias in the
  `SELECT` list and refer to the alias instead.
* A **dimension** can appear in `SELECT`, `WHERE`, `GROUP BY`, `HAVING` and `ORDER BY`. A
  dimension in the `SELECT` list must also be in `GROUP BY` (`MISSING_GROUP_BY` or
  `MISSING_AGGREGATION` otherwise).
* A **source column** that is not a dimension can be used in `WHERE`, but not in `SELECT`,
  `GROUP BY` or `HAVING` (`UNRESOLVED_COLUMN`).
* `COUNT(*)` and other aggregates that take no column count the source rows after the filter;
  they do not go through `MEASURE()`.
* `MEASURE()` applies only to a metric view's measures. It cannot be applied to a column of a
  table or of an ordinary view.
* Joining a metric view with another relation in the same query is not supported. Aggregate the
  metric view in a subquery, a common table expression, or an ordinary view, and join the result.
* The output of `MEASURE()` is an ordinary column to any enclosing query. Summing or averaging it
  across groups is ordinary SQL and does not re-evaluate the measure at the outer grain; ask for
  a measure at the grain you report.

### What DESCRIBE Shows

`DESCRIBE` lists the dimensions and measures with their derived types. `DESCRIBE EXTENDED` adds a
`Type` of `METRIC_VIEW`, the YAML document as `View Text`, a `Language` of `YAML`, and
`Table Properties` holding the user's properties plus `metric_view.from.type` (`ASSET` for a table
or view name, `SQL` for a query), `metric_view.from.name` or `metric_view.from.sql`, and
`metric_view.where` when the definition has a filter. `SHOW CREATE TABLE` is not supported on a
metric view (`UNSUPPORTED_SHOW_CREATE_TABLE.ON_METRIC_VIEW`); read the definition back from
`View Text`.

### Examples

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
    ('west', DATE'2026-01-05', 5,  10.00, false);

-- A metric view over `orders` with two dimensions and four measures.
CREATE VIEW order_metrics
    (region COMMENT 'Sales region', order_date, order_count, revenue, customers, return_rate)
    WITH METRICS
    LANGUAGE YAML
    COMMENT 'Order metrics'
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
  - name: customers
    expr: COUNT(DISTINCT customer_id)
  - name: return_rate
    expr: AVG(CAST(returned AS INT))
$$;

-- Column types are derived from the source and the measure expressions.
DESCRIBE order_metrics;
+-----------+-------------+------------+
|col_name   |data_type    |comment     |
+-----------+-------------+------------+
|region     |string       |Sales region|
|order_date |date         |NULL        |
|order_count|bigint       |NULL        |
|revenue    |decimal(20,2)|NULL        |
|customers  |bigint       |NULL        |
|return_rate|double       |NULL        |
+-----------+-------------+------------+

DESCRIBE EXTENDED order_metrics;
+----------------------------+-----------------------------------------------------------------------+------------+
|col_name                    |data_type                                                              |comment     |
+----------------------------+-----------------------------------------------------------------------+------------+
|region                      |string                                                                 |Sales region|
|order_date                  |date                                                                   |NULL        |
|order_count                 |bigint                                                                 |NULL        |
|revenue                     |decimal(20,2)                                                          |NULL        |
|customers                   |bigint                                                                 |NULL        |
|return_rate                 |double                                                                 |NULL        |
|                            |                                                                       |            |
|# Detailed Table Information|                                                                       |            |
|Catalog                     |spark_catalog                                                          |            |
|Database                    |default                                                                |            |
|Table                       |order_metrics                                                          |            |
|...                         |                                                                       |            |
|Type                        |METRIC_VIEW                                                            |            |
|Comment                     |Order metrics                                                          |            |
|View Text                   |\nversion: 0.1\nsource: orders\ndimensions:\n  - name: region\n ...    |            |
|Language                    |YAML                                                                   |            |
|Table Properties            |[metric_view.from.name=orders, metric_view.from.type=ASSET, team=sales]|            |
+----------------------------+-----------------------------------------------------------------------+------------+

SHOW VIEWS;
+---------+-------------+-----------+
|namespace|viewName     |isTemporary|
+---------+-------------+-----------+
|default  |order_metrics|false      |
+---------+-------------+-----------+
```

#### Measures at every grain

```sql
-- Without GROUP BY, each measure is evaluated over the whole source.
SELECT MEASURE(order_count) AS order_count,
       MEASURE(revenue) AS revenue,
       MEASURE(customers) AS customers
FROM order_metrics;
+-----------+-------+---------+
|order_count|revenue|customers|
+-----------+-------+---------+
|7          |715.49 |5        |
+-----------+-------+---------+

-- The same measures, once per region. ORDER BY refers to the measure's alias.
SELECT region,
       MEASURE(revenue) AS revenue,
       MEASURE(customers) AS customers,
       ROUND(MEASURE(return_rate), 2) AS return_rate
FROM order_metrics
GROUP BY region
ORDER BY revenue DESC;
+------+-------+---------+-----------+
|region|revenue|customers|return_rate|
+------+-------+---------+-----------+
|west  |469.99 |3        |0.25       |
|east  |245.50 |2        |0.33       |
+------+-------+---------+-----------+

-- WHERE on a dimension, grouped by another dimension.
SELECT order_date, MEASURE(customers) AS customers
FROM order_metrics
WHERE region = 'west'
GROUP BY order_date
ORDER BY order_date;
+----------+---------+
|order_date|customers|
+----------+---------+
|2026-01-03|1        |
|2026-01-04|2        |
|2026-01-05|1        |
+----------+---------+

-- HAVING refers to the measure's alias.
SELECT region, MEASURE(revenue) AS revenue
FROM order_metrics
GROUP BY region
HAVING revenue > 300;
+------+-------+
|region|revenue|
+------+-------+
|west  |469.99 |
+------+-------+

-- WHERE may use a source column that is not a dimension.
SELECT MEASURE(revenue) AS returned_revenue FROM order_metrics WHERE returned;
+----------------+
|returned_revenue|
+----------------+
|179.99          |
+----------------+

-- Dimensions alone, grouped.
SELECT region FROM order_metrics GROUP BY region;
+------+
|region|
+------+
|east  |
|west  |
+------+

-- COUNT(*) counts source rows.
SELECT COUNT(*) FROM order_metrics;
+--------+
|count(1)|
+--------+
|7       |
+--------+
```

#### A measure is evaluated at the grain of the query that calls MEASURE()

```sql
-- Distinct customers over the whole source.
SELECT MEASURE(customers) AS customers FROM order_metrics;
+---------+
|customers|
+---------+
|5        |
+---------+

-- Distinct customers per day, then summed by the outer query. The outer SUM is ordinary SQL
-- over the inner result and counts a customer once for every day they ordered.
SELECT SUM(customers) AS sum_of_daily_customers
FROM (SELECT order_date, MEASURE(customers) AS customers
      FROM order_metrics
      GROUP BY order_date);
+----------------------+
|sum_of_daily_customers|
+----------------------+
|7                     |
+----------------------+
```

#### Queries that are rejected

```sql
-- A measure has no value outside MEASURE().
SELECT * FROM order_metrics;
  Error: the query fails during planning.

SELECT region, revenue FROM order_metrics GROUP BY region;
  [MISSING_AGGREGATION] The non-aggregating expression "revenue" is based on columns which are not participating in the GROUP BY clause. ...

-- A source column that is not a dimension cannot be selected or grouped by.
SELECT customer_id, MEASURE(revenue) FROM order_metrics GROUP BY customer_id;
  [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `customer_id` cannot be resolved. Did you mean one of the following? [`customers`, `revenue`, `order_date`, `region`, `return_rate`]. ...

-- A dimension in the SELECT list must be grouped by.
SELECT region, MEASURE(revenue) FROM order_metrics;
  [MISSING_GROUP_BY] The query does not include a GROUP BY clause. ...

-- MEASURE() is not resolved in WHERE, HAVING or ORDER BY; use an alias from the SELECT list.
SELECT region FROM order_metrics WHERE MEASURE(revenue) > 300 GROUP BY region;
  Error: `revenue` cannot be resolved in the WHERE clause.

SELECT region, MEASURE(revenue) AS revenue FROM order_metrics GROUP BY region ORDER BY MEASURE(revenue);
  Error: `revenue` cannot be resolved in the ORDER BY clause.
```

#### A SQL query as the source, with a filter

```sql
-- The source adds a derived column; the filter removes returned orders from every query.
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
|west  |small|70.00  |
+------+-----+-------+

-- The properties record the source query and the filter.
DESCRIBE EXTENDED kept_order_metrics;
+----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+-------+
|col_name                    |data_type                                                                                                                                            |comment|
+----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+-------+
|...                         |                                                                                                                                                     |       |
|Type                        |METRIC_VIEW                                                                                                                                          |       |
|Language                    |YAML                                                                                                                                                 |       |
|Table Properties            |[metric_view.from.sql=SELECT o.*, CASE WHEN amount >= 100 THEN 'large' ELSE 'small' END AS size\nFROM orders o\n, metric_view.from.type=SQL, metric_view.where=NOT returned]|       |
+----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+-------+
```

#### Definitions that are rejected

```sql
-- A measure must aggregate.
CREATE VIEW mv_bad WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
dimensions:
  - name: region
    expr: region
measures:
  - name: revenue
    expr: amount
$$;
  [MISSING_AGGREGATION] The non-aggregating expression "amount" is based on columns which are not participating in the GROUP BY clause. ...

-- A dimension must not aggregate.
CREATE VIEW mv_bad WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
dimensions:
  - name: top_region
    expr: MAX(region)
measures:
  - name: revenue
    expr: SUM(amount)
$$;
  [GROUP_BY_AGGREGATE] Aggregate functions are not allowed in GROUP BY, but found max(spark_catalog.default.orders.region). ...

-- Names are unique across dimensions and measures.
CREATE VIEW mv_bad WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
dimensions:
  - name: region
    expr: region
measures:
  - name: region
    expr: COUNT(1)
$$;
  [COLUMN_ALREADY_EXISTS] The column `region` already exists. Choose another name or rename the existing column. ...

-- A measure cannot refer to another measure. The view is created, because `revenue` is not
-- resolved until a query asks for `avg_order_value`, and that query fails.
CREATE VIEW mv_bad WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
measures:
  - name: revenue
    expr: SUM(amount)
  - name: avg_order_value
    expr: revenue / COUNT(1)
$$;

SELECT MEASURE(avg_order_value) FROM mv_bad;
  [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `revenue` cannot be resolved. Did you mean one of the following? [`region`, `amount`, `returned`, `order_date`, `customer_id`]. ...

DROP VIEW mv_bad;

-- Only `name` and `expr` are allowed on a column; only `version`, `source`, `filter`,
-- `dimensions` and `measures` are allowed at the top level.
CREATE VIEW mv_bad WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
measures:
  - name: revenue
    expr: SUM(amount)
    description: Total order amount
$$;
  [INVALID_METRIC_VIEW_YAML] Failed to parse metric view YAML: Failed to parse YAML: Unrecognized field "description" (class org.apache.spark.sql.metricview.serde.ColumnV01), not marked as ignorable (2 known properties: "name", "expr") ...

-- Only version 0.1 is accepted.
CREATE VIEW mv_bad WITH METRICS LANGUAGE YAML AS $$
version: 1.0
source: orders
measures:
  - name: revenue
    expr: SUM(amount)
$$;
  [INVALID_METRIC_VIEW_YAML] Failed to parse metric view YAML: Invalid YAML version: 1.0. Supported YAML versions: 0.1 ...

-- A persistent metric view cannot read a temporary view.
CREATE TEMPORARY VIEW recent_orders AS SELECT * FROM orders WHERE order_date >= DATE'2026-01-04';

CREATE VIEW mv_bad WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: recent_orders
measures:
  - name: revenue
    expr: SUM(amount)
$$;
  [INVALID_TEMP_OBJ_REFERENCE] Cannot create the persistent object `default`.`mv_bad` of the type VIEW because it references to the temporary object `recent_orders` of the type VIEW. ...

-- LANGUAGE YAML is required.
CREATE VIEW mv_bad WITH METRICS AS $$
version: 0.1
source: orders
$$;
  [MISSING_CLAUSES_FOR_OPERATION] Missing required clause(s) LANGUAGE for operation CREATE METRIC VIEW. ...

-- There is no temporary metric view.
CREATE TEMPORARY VIEW mv_bad WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
$$;
  [PARSE_SYNTAX_ERROR] Syntax error at or near 'METRICS'. ...
```

#### Lifecycle

```sql
-- IF NOT EXISTS keeps the existing definition.
CREATE VIEW IF NOT EXISTS order_metrics WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
measures:
  - name: revenue
    expr: SUM(amount)
$$;

-- OR REPLACE replaces it.
CREATE OR REPLACE VIEW order_metrics WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
measures:
  - name: revenue
    expr: SUM(amount)
$$;

-- SHOW CREATE TABLE is not supported; DESCRIBE EXTENDED returns the definition as View Text.
SHOW CREATE TABLE order_metrics;
  [UNSUPPORTED_SHOW_CREATE_TABLE.ON_METRIC_VIEW] Unsupported a SHOW CREATE TABLE command. The command is not supported on a metric view `order_metrics`. ...

-- ALTER VIEW can rename a metric view and set or unset its properties.
ALTER VIEW order_metrics SET TBLPROPERTIES ('team' = 'finance');
ALTER VIEW order_metrics RENAME TO order_metrics_v1;

-- There is no ALTER VIEW form for the YAML body; use CREATE OR REPLACE VIEW ... WITH METRICS.
ALTER VIEW order_metrics_v1 WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
$$;
  [PARSE_SYNTAX_ERROR] Syntax error at or near 'LANGUAGE'. ...

DROP VIEW order_metrics_v1;
DROP VIEW kept_order_metrics;
```

#### Querying from the DataFrame API

A metric view is read like any table. `MEASURE()` is available through SQL expressions
(`expr`, `selectExpr`), and the same rules apply: a measure needs `MEASURE()` inside an
aggregation, and `MEASURE()` is not accepted in `filter`/`where`. `df.count()` counts source rows,
as `COUNT(*)` does.

```python
from pyspark.sql import functions as F

metrics = spark.table("order_metrics")

metrics.columns
# ['region', 'order_date', 'order_count', 'revenue', 'customers', 'return_rate']

metrics.groupBy("region").agg(F.expr("MEASURE(revenue)").alias("revenue")).orderBy("region").show()
# +------+-------+
# |region|revenue|
# +------+-------+
# |east  |245.50 |
# |west  |469.99 |
# +------+-------+

metrics.selectExpr("MEASURE(order_count) AS order_count", "MEASURE(revenue) AS revenue").show()
# +-----------+-------+
# |order_count|revenue|
# +-----------+-------+
# |7          |715.49 |
# +-----------+-------+

metrics.count()
# 7

metrics.select("revenue").show()      # fails: a measure has no value outside MEASURE()
metrics.show()                        # fails for the same reason
metrics.filter(F.expr("MEASURE(revenue) > 300"))
# [INVALID_WHERE_CONDITION] The WHERE condition "(measure(revenue) > 300)" contains invalid expressions: measure(revenue).
```

### Related Statements

* [CREATE VIEW](sql-ref-syntax-ddl-create-view.html)
* [ALTER VIEW](sql-ref-syntax-ddl-alter-view.html)
* [DROP VIEW](sql-ref-syntax-ddl-drop-view.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
* [SHOW VIEWS](sql-ref-syntax-aux-show-views.html)
* [SHOW CREATE TABLE](sql-ref-syntax-aux-show-create-table.html)
* [Aggregate Functions](sql-ref-syntax-qry-select-aggregate.html)
