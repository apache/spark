--SET spark.sql.cbo.enabled=true
--SET spark.sql.maxMetadataStringLength = 500

CREATE TABLE explain_temp1(a INT, b INT) USING PARQUET;
CREATE TABLE explain_temp2(c INT, d INT) USING PARQUET;

ANALYZE TABLE explain_temp1 COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE explain_temp2 COMPUTE STATISTICS FOR ALL COLUMNS;

EXPLAIN COST WITH max_store_sales AS
(
  SELECT max(csales) tpcds_cmax
  FROM (
    SELECT sum(b) csales
    FROM explain_temp1 WHERE a < 100
  ) x
),
best_ss_customer AS
(
  SELECT c
  FROM explain_temp2
  WHERE d > (SELECT * FROM max_store_sales)
)
SELECT c FROM best_ss_customer;

DROP TABLE explain_temp1;
DROP TABLE explain_temp2;
