-- WITH inside CTE
CREATE TABLE cte_tbl USING csv AS WITH s AS (SELECT 42 AS col) SELECT * FROM s;

SELECT * FROM cte_tbl;

-- WITH inside CREATE VIEW
CREATE TEMPORARY VIEW cte_view AS WITH s AS (SELECT 42 AS col) SELECT * FROM s;

SELECT * FROM cte_view;

-- INSERT inside WITH
WITH s AS (SELECT 43 AS col)
INSERT INTO cte_tbl SELECT * FROM S;

SELECT * FROM cte_tbl;

-- WITH inside INSERT
INSERT INTO cte_tbl WITH s AS (SELECT 44 AS col) SELECT * FROM s;

SELECT * FROM cte_tbl;

CREATE TABLE cte_tbl2 (col INT) USING csv;
-- Multi-INSERT
WITH s AS (SELECT 45 AS col)
FROM s
INSERT INTO cte_tbl SELECT col
INSERT INTO cte_tbl2 SELECT col;

SELECT * FROM cte_tbl;
SELECT * FROM cte_tbl2;

DROP TABLE cte_tbl;
DROP TABLE cte_tbl2;
