---
layout: global
title: Common Table Expression (CTE)
displayTitle: Common Table Expression (CTE)
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

A common table expression (CTE) defines a temporary result set that a user can reference possibly multiple times within the scope of a SQL statement. A CTE is used mainly in a SELECT statement. It is defined with a name including an optional column names list, followed by a query expression. When a CTE references itself, it is called a recursive CTE.

### Syntax

{% highlight sql %}
WITH common_table_expression [ , ... ]
{% endhighlight %}

While `common_table_expression` is defined as
{% highlight sql %}
expression_name [ ( column_name [ , ... ] ) ] [ AS ] ( [ common_table_expression ] query )
{% endhighlight %}

### Parameters

<dl>
  <dt><code><em>expression_name</em></code></dt>
  <dd>
    Specifies a name for the common table expression.
  </dd>
</dl>
<dl>
  <dt><code><em>query</em></code></dt>
  <dd>
    A <a href="sql-ref-syntax-qry-select.html">SELECT</a> statement.
  </dd>
</dl>

### Examples

{% highlight sql %}
-- CTE with multiple column aliases
WITH t(x, y) AS (SELECT 1, 2)
SELECT * FROM t WHERE x = 1 AND y = 2;
  +---+---+
  |  x|  y|
  +---+---+
  |  1|  2|
  +---+---+

-- CTE in CTE definition
WITH t as (
    WITH t2 AS (SELECT 1)
    SELECT * FROM t2
)
SELECT * FROM t;
  +---+
  |  1|
  +---+
  |  1|
  +---+

-- CTE in subquery
SELECT max(c) FROM (
    WITH t(c) AS (SELECT 1)
    SELECT * FROM t
);
  +------+
  |max(c)|
  +------+
  |     1|
  +------+

-- CTE in subquery expression
SELECT (
    WITH t AS (SELECT 1)
    SELECT * FROM t
);
  +----------------+
  |scalarsubquery()|
  +----------------+
  |               1|
  +----------------+

-- Name conflict in nested CTE. Spark throws an AnalysisException by default.
-- SET spark.sql.legacy.ctePrecedencePolicy = CORRECTED (which is recommended),
-- inner CTE definitions take precedence over outer definitions.
SET spark.sql.legacy.ctePrecedencePolicy = CORRECTED;
WITH
    t AS (SELECT 1),
    t2 AS (
        WITH t AS (SELECT 2)
        SELECT * FROM t
    )
SELECT * FROM t2;
  +---+
  |  2|
  +---+
  |  2|
  +---+

-- SET spark.sql.legacy.ctePrecedencePolicy = LEGACY (the behavior in version 2.4 and earlier)
-- outer CTE definitions take precedence over inner definitions.
SET spark.sql.legacy.ctePrecedencePolicy = LEGACY;
WITH
    t AS (SELECT 1),
    t2 AS (
        WITH t AS (SELECT 2)
        SELECT * FROM t
    )
SELECT * FROM t2;
  +---+
  |  1|
  +---+
  |  1|
  +---+
{% endhighlight %}

### Related Statements

 * [SELECT](sql-ref-syntax-qry-select.html)
