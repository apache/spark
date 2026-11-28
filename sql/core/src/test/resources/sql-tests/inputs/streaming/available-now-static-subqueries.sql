DROP TABLE IF EXISTS streaming_sql_golden_subquery_events;
DROP TABLE IF EXISTS streaming_sql_golden_subquery_lookup;
DROP TABLE IF EXISTS streaming_sql_golden_subquery_config;
DROP TABLE IF EXISTS streaming_sql_golden_scalar_subquery_sink;
DROP TABLE IF EXISTS streaming_sql_golden_in_subquery_sink;
DROP TABLE IF EXISTS streaming_sql_golden_exists_subquery_sink;

CREATE TABLE streaming_sql_golden_subquery_events (
  event_id INT, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_subquery_events VALUES
  (1, 10),
  (2, 20),
  (3, 30),
  (4, 40);

CREATE TABLE streaming_sql_golden_subquery_lookup (
  event_id INT, minimum_amount INT) USING parquet;
INSERT INTO streaming_sql_golden_subquery_lookup VALUES
  (1, 5),
  (2, 25),
  (4, 35);

CREATE TABLE streaming_sql_golden_subquery_config (
  multiplier INT) USING parquet;
INSERT INTO streaming_sql_golden_subquery_config VALUES (2), (3);

CREATE TABLE streaming_sql_golden_scalar_subquery_sink (
  event_id INT, scaled_amount INT) USING parquet;

CREATE FLOW streaming_sql_golden_scalar_subquery_flow AS
INSERT INTO streaming_sql_golden_scalar_subquery_sink BY NAME
SELECT event_id,
  amount * (SELECT MAX(multiplier) FROM streaming_sql_golden_subquery_config) AS scaled_amount
FROM STREAM(streaming_sql_golden_subquery_events);

REFRESH FLOW streaming_sql_golden_scalar_subquery_flow;

SELECT * FROM streaming_sql_golden_scalar_subquery_sink ORDER BY event_id;

WITH batch_output AS (
  SELECT event_id,
    amount * (SELECT MAX(multiplier) FROM streaming_sql_golden_subquery_config) AS scaled_amount
  FROM streaming_sql_golden_subquery_events
),
streaming_output AS (
  SELECT event_id, scaled_amount FROM streaming_sql_golden_scalar_subquery_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

CREATE TABLE streaming_sql_golden_in_subquery_sink (
  event_id INT, amount INT) USING parquet;

CREATE FLOW streaming_sql_golden_in_subquery_flow AS
INSERT INTO streaming_sql_golden_in_subquery_sink BY NAME
SELECT event_id, amount
FROM STREAM(streaming_sql_golden_subquery_events)
WHERE event_id IN (SELECT event_id FROM streaming_sql_golden_subquery_lookup);

REFRESH FLOW streaming_sql_golden_in_subquery_flow;

SELECT * FROM streaming_sql_golden_in_subquery_sink ORDER BY event_id;

WITH batch_output AS (
  SELECT event_id, amount
  FROM streaming_sql_golden_subquery_events
  WHERE event_id IN (SELECT event_id FROM streaming_sql_golden_subquery_lookup)
),
streaming_output AS (
  SELECT event_id, amount FROM streaming_sql_golden_in_subquery_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

CREATE TABLE streaming_sql_golden_exists_subquery_sink (
  event_id INT, amount INT) USING parquet;

CREATE FLOW streaming_sql_golden_exists_subquery_flow AS
INSERT INTO streaming_sql_golden_exists_subquery_sink BY NAME
SELECT event_id, amount
FROM STREAM(streaming_sql_golden_subquery_events) AS events
WHERE EXISTS (
  SELECT 1
  FROM streaming_sql_golden_subquery_lookup AS lookup
  WHERE lookup.event_id = events.event_id AND events.amount >= lookup.minimum_amount);

REFRESH FLOW streaming_sql_golden_exists_subquery_flow;

SELECT * FROM streaming_sql_golden_exists_subquery_sink ORDER BY event_id;

WITH batch_output AS (
  SELECT event_id, amount
  FROM streaming_sql_golden_subquery_events AS events
  WHERE EXISTS (
    SELECT 1
    FROM streaming_sql_golden_subquery_lookup AS lookup
    WHERE lookup.event_id = events.event_id AND events.amount >= lookup.minimum_amount
  )
),
streaming_output AS (
  SELECT event_id, amount FROM streaming_sql_golden_exists_subquery_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

DROP TABLE streaming_sql_golden_subquery_events;
DROP TABLE streaming_sql_golden_subquery_lookup;
DROP TABLE streaming_sql_golden_subquery_config;
DROP TABLE streaming_sql_golden_scalar_subquery_sink;
DROP TABLE streaming_sql_golden_in_subquery_sink;
DROP TABLE streaming_sql_golden_exists_subquery_sink;
