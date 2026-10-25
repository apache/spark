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
The document names a source (a table, a view other than a metric view, or a SQL query), an optional filter, a list of
**dimensions** (expressions a query can group and filter by) and a list of **measures**
(aggregate expressions). A query reads a measure with the `MEASURE()` aggregate function, and
Spark evaluates the measure's expression over the source at the grain of that query's `GROUP BY`,
so one definition serves every level of aggregation. `CREATE VIEW ... WITH METRICS` creates a
metric view; it stores only metadata, and `ALTER VIEW` and `DROP VIEW` change only metadata.

Metric views are always persistent: there is no `TEMPORARY` form.

The [Metric Views Programming Guide](metric-views-programming-guide.html) explains how a metric
view is queried, how `MEASURE()` is evaluated, and how to model dimensions and measures.

### Syntax

```sql
CREATE [ OR REPLACE ] VIEW [ IF NOT EXISTS ] view_identifier
    [ ( column_name [ COMMENT column_comment ], ... ) ]
    create_metric_view_clauses AS $[tag]$ yaml_definition $[tag]$
```

### Parameters

* **OR REPLACE**

    If a view of the same name already exists, it is replaced. Replacing an existing view requires a
    catalog that implements `ViewCatalog`; in the session catalog (`spark_catalog`), drop the view and create it
    again instead. `OR REPLACE` cannot be combined with `IF NOT EXISTS`. A Data Source V2 catalog that does
    not implement `ViewCatalog` rejects `CREATE VIEW ... WITH METRICS` with
    `MISSING_CATALOG_ABILITY.VIEWS`.

* **IF NOT EXISTS**

    Creates the view only if no table or view of that name exists. When one does, the statement
    succeeds and the existing definition is kept.

* **view_identifier**

    Specifies a view name, optionally qualified with a namespace (or a catalog and namespace).
    If the name is not qualified the view is created in the current schema.

    **Syntax:** `[ catalog_name. ] [ database_name. ] view_name`

* **( column_name [ COMMENT column_comment ], ... )**

    Optional names and comments for the view's columns, in the order the definition lists them:
    every dimension first, then every measure. The list must name exactly as many columns as the
    definition declares. A query refers to a dimension or measure by the name in this list, not by
    its name in the YAML document.

* **create_metric_view_clauses**

    These clauses are order insensitive, and each may appear at most once. `WITH METRICS` and
    `LANGUAGE YAML` are required.

    * `WITH METRICS` marks the view as a metric view.
    * `LANGUAGE YAML` names the language of the body; `YAML` is the only language accepted.
    * `[ COMMENT view_comment ]` describes the view.
    * `[ TBLPROPERTIES ( property_name = property_value [ , ... ] ) ]` adds metadata key-value
      pairs. Spark adds its own: `metric_view.from.type` (`ASSET` for a table or view name, `SQL`
      for a query), `metric_view.from.name` or `metric_view.from.sql`, and `metric_view.where` when
      the definition has a filter. These are descriptive: the SQL and filter values are truncated
      to 1024 characters, and a user-supplied value for one of these keys is overwritten.

* **yaml_definition**

    The definition, as a dollar-quoted string (`$$ ... $$`, or `$tag$ ... $tag$` with any tag).
    The document has these keys and no others;
    any other key, at the top level or on a column, fails with `INVALID_METRIC_VIEW_YAML`.

    * **version**

      Required. The version of the definition format. `0.1` is the only version accepted.

    * **source**

      Required. Either the name of a table or of a view other than a metric view, optionally
      qualified, or a SQL query. Spark first tries to read the value as a multi-part identifier and
      otherwise parses it as a query. To build on a metric view, use a query source that reads it
      with `MEASURE()`.
      A persistent metric view cannot reference temporary views or session variables
      (`INVALID_TEMP_OBJ_REFERENCE`).

    * **filter**

      Optional. A boolean expression over the source's columns, applied before any query's own
      `WHERE` clause. It may reference source columns that are not dimensions.

    * **dimensions**

      Optional. A list of `{ name, expr }` entries. `expr` is any non-aggregate SQL expression
      over the source's columns.

    * **measures**

      Optional. A list of `{ name, expr }` entries. `expr` is a SQL aggregate expression over the
      source's columns, such as `SUM(amount)` or `SUM(amount) / COUNT(1)`. A measure's expression
      resolves against the source's columns only; it cannot refer to another measure by name.

      Dimension and measure names must be unique across both lists.

    Column types are not declared. When the view is created, Spark analyzes an aggregation over
    the source, grouped by every dimension and selecting every dimension and measure, and records
    the resulting types.

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
    ('west', DATE'2026-01-05', 5,  10.00, false),
    ('west', DATE'2026-01-05', 6,  40.00, false),
    ('west', DATE'2026-01-05', 3,  25.00, false);

-- Column names and comments, a view comment and table properties.
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

-- Measures at the grain of the GROUP BY. See the programming guide for the querying rules.
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
|west  |534.99 |4        |0.17       |
|east  |245.50 |2        |0.33       |
+------+-------+---------+-----------+

-- A SQL query as the source, with a filter applied to every query.
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

-- IF NOT EXISTS keeps the existing definition.
CREATE VIEW IF NOT EXISTS order_metrics WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: orders
measures:
  - name: revenue
    expr: SUM(amount)
$$;

```

### Related Statements

* [CREATE VIEW](sql-ref-syntax-ddl-create-view.html)
* [ALTER VIEW](sql-ref-syntax-ddl-alter-view.html)
* [DROP VIEW](sql-ref-syntax-ddl-drop-view.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
* [SHOW VIEWS](sql-ref-syntax-aux-show-views.html)
* [SHOW CREATE TABLE](sql-ref-syntax-aux-show-create-table.html)
* [Aggregate Functions](sql-ref-syntax-qry-select-aggregate.html)
