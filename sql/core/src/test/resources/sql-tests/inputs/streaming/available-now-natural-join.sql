DROP TABLE IF EXISTS streaming_sql_golden_natural_join_events;
DROP TABLE IF EXISTS streaming_sql_golden_natural_join_customers;
DROP TABLE IF EXISTS streaming_sql_golden_natural_join_sink;

CREATE TABLE streaming_sql_golden_natural_join_events (
  event_id INT, customer_id INT, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_natural_join_events VALUES
  (1, 10, 25),
  (2, 20, 40),
  (3, 99, 15);

CREATE TABLE streaming_sql_golden_natural_join_customers (
  customer_id INT, tier STRING) USING parquet;
INSERT INTO streaming_sql_golden_natural_join_customers VALUES
  (10, 'gold'),
  (20, 'silver');

CREATE TABLE streaming_sql_golden_natural_join_sink (
  customer_id INT, event_id INT, amount INT, tier STRING) USING parquet;

CREATE FLOW streaming_sql_golden_natural_join_flow AS
INSERT INTO streaming_sql_golden_natural_join_sink BY NAME
SELECT customer_id, event_id, amount, tier
FROM STREAM(streaming_sql_golden_natural_join_events)
NATURAL INNER JOIN streaming_sql_golden_natural_join_customers;

REFRESH FLOW streaming_sql_golden_natural_join_flow;

SELECT * FROM streaming_sql_golden_natural_join_sink ORDER BY event_id;

WITH batch_output AS (
  SELECT customer_id, event_id, amount, tier
  FROM streaming_sql_golden_natural_join_events
  NATURAL INNER JOIN streaming_sql_golden_natural_join_customers
),
streaming_output AS (
  SELECT customer_id, event_id, amount, tier FROM streaming_sql_golden_natural_join_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

DROP TABLE streaming_sql_golden_natural_join_events;
DROP TABLE streaming_sql_golden_natural_join_customers;
DROP TABLE streaming_sql_golden_natural_join_sink;
