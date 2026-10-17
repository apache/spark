-- Tests for GROUPS window frames (SPARK-58980).
-- Canonical data reused throughout: batch_id has a tie (3, 25) / (3, 30) so peer-group
-- semantics are actually exercised, matching DataFrameWindowFramesSuite's test matrix.
CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM VALUES
(1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40)
AS t(batch_id, amount);

-- Case 1: `n PRECEDING` counts peer groups, not rows or key distance.
-- Expected: 25,25,45,75,75,95
SELECT batch_id, sum(amount) OVER (ORDER BY batch_id
GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM t ORDER BY batch_id, amount;

-- Case 2: CURRENT ROW means the current row's entire peer group.
-- Expected: 25,25,20,55,55,40
SELECT batch_id, sum(amount) OVER (ORDER BY batch_id
GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS total FROM t ORDER BY batch_id, amount;

-- Case 3: `n FOLLOWING` advances one peer group.
-- Expected: 45,45,75,95,95,40
SELECT batch_id, sum(amount) OVER (ORDER BY batch_id
GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS total FROM t ORDER BY batch_id, amount;

-- Unbounded-both-ends GROUPS matches the equivalent RANGE query (no offset bounds).
SELECT batch_id, sum(amount) OVER (ORDER BY batch_id
GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total
FROM t ORDER BY batch_id, amount;

-- Case 5: DESC ordering - offsets follow window order, not key order.
-- Expected: 40,95,95,75,45,45
SELECT batch_id, sum(amount) OVER (ORDER BY batch_id DESC
GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM t ORDER BY batch_id DESC, amount;

-- Case 4: multi-column ORDER BY with an offset - GROUPS has no RANGE_FRAME_MULTI_ORDER
-- analogue, since multi-order support is a headline reason for the feature.
CREATE OR REPLACE TEMPORARY VIEW t_multi AS SELECT * FROM VALUES
('2024-01-01', 1, 10), ('2024-01-01', 1, 20), ('2024-01-02', 2, 30),
('2024-01-03', 3, 45), ('2024-01-03', 3, 45)
AS t_multi(trade_date, batch_id, amount);

SELECT sum(amount) OVER (ORDER BY trade_date, batch_id
GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total
FROM t_multi ORDER BY trade_date, batch_id;

-- NULLs form one peer group: NULLS FIRST.
CREATE OR REPLACE TEMPORARY VIEW t_nulls AS SELECT * FROM VALUES
(CAST(1 AS INT), 10), (1, 15), (2, 20), (CAST(NULL AS INT), 30)
AS t_nulls(batch_id, amount);

-- Case 6: NULLs form one peer group.
-- Expected: 25,25,20,30
SELECT batch_id, sum(amount) OVER (ORDER BY batch_id NULLS FIRST
GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS total
FROM t_nulls ORDER BY batch_id NULLS FIRST, amount;

-- 0 PRECEDING / 0 FOLLOWING are equivalent to CURRENT ROW.
SELECT batch_id,
sum(amount) OVER (ORDER BY batch_id
  GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS current_row_total,
sum(amount) OVER (ORDER BY batch_id
  GROUPS BETWEEN 0 PRECEDING AND 0 FOLLOWING) AS zero_offset_total
FROM t ORDER BY batch_id, amount;

-- Analysis error: GROUPS requires ORDER BY (test matrix case 7).
SELECT batch_id, count(amount) OVER(
GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t ORDER BY batch_id;

-- Analysis error: null offsets are rejected (test matrix case 8).
SELECT batch_id, sum(amount) OVER (ORDER BY batch_id
GROUPS BETWEEN CAST(NULL AS INT) PRECEDING AND CURRENT ROW) AS total FROM t;

-- `groups` remains usable as an identifier (test matrix case 9).
SELECT 1 AS groups, 2 AS other ORDER BY groups;
CREATE OR REPLACE TEMPORARY VIEW t_ident AS SELECT * FROM VALUES
(1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40)
AS t_ident(groups, amount);

SELECT groups, amount FROM t_ident ORDER BY groups;
SELECT groups, sum(amount) OVER (ORDER BY groups
GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM t_ident ORDER BY groups, amount;

-- Regression guard (test matrix case 10): existing ROWS and RANGE results are unchanged.
-- Expected: rows_total 10,30,60 ; range_total 10,30,40.
CREATE OR REPLACE TEMPORARY VIEW t_regression AS SELECT * FROM VALUES
(1, 10, 10), (2, 20, 20), (3, 30, 10)
AS t_regression(key, row_amount, range_amount);

SELECT
sum(row_amount) OVER (ORDER BY key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_total,
sum(range_amount) OVER (ORDER BY key RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  AS range_total
FROM t_regression;
