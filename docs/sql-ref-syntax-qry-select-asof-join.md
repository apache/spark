---
layout: global
title: ASOF JOIN
displayTitle: ASOF JOIN
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

`ASOF JOIN` combines each row from the left relation with at most one row from the
right relation. The match is the closest row on the right that satisfies a required
`MATCH_CONDITION` comparison and an optional `ON` or `USING` filter.

`ASOF JOIN` is asymmetric: the left relation drives per-row lookups into the right,
and reversing the operands changes the result.

`ASOF JOIN` is disabled by default. Set `spark.sql.join.asofJoin.enabled` to `true`
to enable the syntax. When disabled, `ASOF JOIN` fails at parse time with
`UNSUPPORTED_FEATURE.ASOF_JOIN`.

Only `INNER` (the default) and `LEFT OUTER` join types are supported.

### Syntax

```sql
[ asof_join_type ] ASOF JOIN right_table_reference join_criteria

asof_join_type
  { [ INNER ] |
    LEFT [ OUTER ] }

join_criteria
  MATCH_CONDITION ( expr1 comparison_operator expr2 )
  [ ON boolean_expression | USING ( column_name [, ...] ) ]

comparison_operator
  { >= | > | <= | < }
```

### Parameters

* **right_table_reference**

    The table reference on the right of the join; searched once per left row.

* **asof_join_type**

    The outer form of the `ASOF JOIN`.
    * `INNER` (default): drops left rows with no qualifying match.
    * `LEFT [ OUTER ]`: retains such rows with `NULL` right-side columns.

* **join_criteria**

    A required `MATCH_CONDITION` clause and an optional `ON` or `USING` clause that
    filters the join input before the closest-match selection. When both `ON` and
    `USING` are omitted, every left row is matched against the entire right table on
    the `MATCH_CONDITION` alone.

    * **MATCH_CONDITION ( expr1 comparison_operator expr2 )**

        Specifies the ordered comparison that determines the closest match. One of
        `expr1` and `expr2` must reference only the left table; the other must
        reference only the right table.

        * **expr1**, **expr2**

            Scalar expressions of an orderable, mutually comparable type. Each must be
            deterministic; subqueries, aggregate functions, window functions, and
            generator functions are not permitted. `STRUCT` operands compare
            lexicographically field-by-field, enabling multi-column match conditions
            (see Examples).

        * **comparison_operator**

            One of `>=`, `>`, `<=`, `<`. Equality operators (`=` and `<>`) are not
            permitted.

    * **ON boolean_expression**

        An expression with a return type of `BOOLEAN` that specifies how rows from the
        two relations are matched before the closest-match selection. If the result is
        `true`, the rows are considered a match. See [JOIN](sql-ref-syntax-qry-select-join.html).

    * **USING ( column_name [, ...] )**

        Matches rows by comparing equality for the list of columns, which must exist in
        both relations. See [JOIN](sql-ref-syntax-qry-select-join.html).

### Notes

* **Direction of match.** Let *L* be the operand of `MATCH_CONDITION` that references
  the left table and *R* the operand that references the right table. The operator
  determines which row on the right is closest:
  * *L* `>=` *R* (equivalently *R* `<=` *L*): the right row with the largest *R* not
    greater than *L* (last-preceding).
  * *L* `>` *R*: the largest *R* strictly less than *L* (last strictly-preceding).
  * *L* `<=` *R*: the smallest *R* not less than *L* (first-following).
  * *L* `<` *R*: the smallest *R* strictly greater than *L* (first strictly-following).

* **NULL.** A `NULL` in either operand never satisfies the `MATCH_CONDITION`
  comparison. Left rows whose operand is `NULL` are dropped under `INNER ASOF` and
  retained with `NULL` right-side columns under `LEFT ASOF`.

* **Ties.** If multiple right rows share the same *R* value that is closest to *L*
  after applying `ON`/`USING`, the choice among tied rows is not deterministic. Add a
  tie-breaker either as an equi-key in `ON`/`USING` or as a trailing field of a
  `STRUCT`-valued `MATCH_CONDITION` (see Examples).

### Examples

```sql
SET spark.sql.join.asofJoin.enabled=true;

CREATE OR REPLACE TEMP VIEW trades(trade_time, symbol, quantity) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:05', 'AAPL', 100),
         (TIMESTAMP '2026-06-29 10:00:11', 'AAPL', 200),
         (TIMESTAMP '2026-06-29 10:00:12', 'MSFT',  50),
         (TIMESTAMP '2026-06-29 09:59:59', 'GOOG',  30);

CREATE OR REPLACE TEMP VIEW quotes(quote_time, symbol, bid_price) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 180.10),
         (TIMESTAMP '2026-06-29 10:00:07', 'AAPL', 180.15),
         (TIMESTAMP '2026-06-29 10:00:10', 'AAPL', 180.20),
         (TIMESTAMP '2026-06-29 10:00:08', 'MSFT', 420.50);

-- Attach the most recent (last-preceding) quote to each trade.
-- The GOOG trade has no matching quote and is dropped by INNER ASOF JOIN.
SELECT t.trade_time, t.symbol, t.quantity, q.bid_price
FROM trades t
ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;
+-------------------+------+--------+---------+
|         trade_time|symbol|quantity|bid_price|
+-------------------+------+--------+---------+
|2026-06-29 10:00:05|  AAPL|     100|   180.10|
|2026-06-29 10:00:11|  AAPL|     200|   180.20|
|2026-06-29 10:00:12|  MSFT|      50|   420.50|
+-------------------+------+--------+---------+

-- Preserve unmatched left rows with LEFT ASOF JOIN.
SELECT t.trade_time, t.symbol, q.bid_price
FROM trades t
LEFT ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;
+-------------------+------+---------+
|         trade_time|symbol|bid_price|
+-------------------+------+---------+
|2026-06-29 09:59:59|  GOOG|     NULL|
|2026-06-29 10:00:05|  AAPL|   180.10|
|2026-06-29 10:00:11|  AAPL|   180.20|
|2026-06-29 10:00:12|  MSFT|   420.50|
+-------------------+------+---------+

-- USING is equivalent to ON symbol equality.
SELECT trade_time, symbol, bid_price
FROM trades
ASOF JOIN quotes
  MATCH_CONDITION (trades.trade_time >= quotes.quote_time)
  USING (symbol);

-- Find the next scheduled maintenance window for each alert.
-- A <= operator selects the first-following right row.
CREATE OR REPLACE TEMP VIEW alerts(alert_time, host) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 'db-01');

CREATE OR REPLACE TEMP VIEW maintenance(window_start, host) AS
  VALUES (TIMESTAMP '2026-06-29 08:00:00', 'db-01'),
         (TIMESTAMP '2026-06-29 12:00:00', 'db-01');

SELECT a.alert_time, a.host, m.window_start
FROM alerts a
ASOF JOIN maintenance m
  MATCH_CONDITION (a.alert_time <= m.window_start)
  ON a.host = m.host;
+-------------------+-----+-------------------+
|         alert_time| host|       window_start|
+-------------------+-----+-------------------+
|2026-06-29 10:00:00|db-01|2026-06-29 12:00:00|
+-------------------+-----+-------------------+

-- Multi-column MATCH_CONDITION using tuple comparison. Both sides carry a
-- (time, seq) pair; ties on time are broken lexicographically by seq.
CREATE OR REPLACE TEMP VIEW deploys(deploy_ts, seq, service, version) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 1, 'api', 'v1.0'),
         (TIMESTAMP '2026-06-29 10:00:00', 2, 'api', 'v1.1'),
         (TIMESTAMP '2026-06-29 10:05:00', 1, 'api', 'v1.2');

CREATE OR REPLACE TEMP VIEW requests(req_ts, seq, service) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 5, 'api'),
         (TIMESTAMP '2026-06-29 10:03:00', 1, 'api');

SELECT r.req_ts, r.seq, d.version
FROM requests r
ASOF JOIN deploys d
  MATCH_CONDITION ((r.req_ts, r.seq) >= (d.deploy_ts, d.seq))
  ON r.service = d.service;
+-------------------+---+-------+
|             req_ts|seq|version|
+-------------------+---+-------+
|2026-06-29 10:00:00|  5|   v1.1|
|2026-06-29 10:03:00|  1|   v1.1|
+-------------------+---+-------+
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
* [JOIN](sql-ref-syntax-qry-select-join.html)
