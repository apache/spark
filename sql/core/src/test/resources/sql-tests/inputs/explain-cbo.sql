--SET spark.sql.cbo.enabled=true
CREATE TABLE t1(a INT, b INT) USING PARQUET;
CREATE TABLE t2(c INT, d INT) USING PARQUET;

ANALYZE TABLE t1 COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE t2 COMPUTE STATISTICS FOR ALL COLUMNS;

EXPLAIN COST WITH max_store_sales AS
(
  SELECT max(csales) tpcds_cmax
  FROM (
    SELECT sum(b) csales
    FROM t1 WHERE a < 100
  ) x
),
best_ss_customer AS
(
  SELECT c
  FROM t2
  WHERE d > (SELECT * FROM max_store_sales)
)
SELECT c FROM best_ss_customer;
