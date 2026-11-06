DROP TABLE IF EXISTS streaming_sql_golden_runtime_filter_events;
DROP TABLE IF EXISTS streaming_sql_golden_runtime_filter_customers;
DROP TABLE IF EXISTS streaming_sql_golden_runtime_filter_inner_sink;
DROP TABLE IF EXISTS streaming_sql_golden_runtime_filter_outer_sink;
DROP TABLE IF EXISTS streaming_sql_golden_runtime_filter_stream_sink;

CREATE TABLE streaming_sql_golden_runtime_filter_events (
  event_id INT, customer_id INT, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_runtime_filter_events
VALUES (1, 10, 25), (2, 20, 40), (3, 99, 15);

CREATE TABLE streaming_sql_golden_runtime_filter_customers (
  customer_id INT, tier STRING) USING parquet;
INSERT INTO streaming_sql_golden_runtime_filter_customers
VALUES (10, 'gold'), (20, 'silver');

CREATE TABLE streaming_sql_golden_runtime_filter_inner_sink (
  event_id INT, tier STRING, amount INT) USING parquet;
CREATE TABLE streaming_sql_golden_runtime_filter_outer_sink (
  event_id INT, tier STRING, amount INT) USING parquet;
CREATE TABLE streaming_sql_golden_runtime_filter_stream_sink (
  event_id INT, tier STRING, amount INT) USING parquet;

-- The static side can be hinted to prune the stream. The rows of the join do not change.
CREATE FLOW streaming_sql_golden_runtime_filter_inner_flow AS
INSERT INTO streaming_sql_golden_runtime_filter_inner_sink BY NAME
SELECT /*+ RUNTIME_FILTER(customers) */ events.event_id, customers.tier, events.amount
FROM STREAM(streaming_sql_golden_runtime_filter_events) AS events
JOIN streaming_sql_golden_runtime_filter_customers AS customers
  ON events.customer_id = customers.customer_id;

REFRESH FLOW streaming_sql_golden_runtime_filter_inner_flow;

SELECT * FROM streaming_sql_golden_runtime_filter_inner_sink ORDER BY event_id;

-- The preserved side of an outer join cannot be pruned, so the hint is ignored with a warning and
-- the unmatched event stays.
CREATE FLOW streaming_sql_golden_runtime_filter_outer_flow AS
INSERT INTO streaming_sql_golden_runtime_filter_outer_sink BY NAME
SELECT /*+ RUNTIME_FILTER(customers) */
  events.event_id, coalesce(customers.tier, 'unknown') AS tier, events.amount
FROM STREAM(streaming_sql_golden_runtime_filter_events) AS events
LEFT JOIN streaming_sql_golden_runtime_filter_customers AS customers
  ON events.customer_id = customers.customer_id;

REFRESH FLOW streaming_sql_golden_runtime_filter_outer_flow;

SELECT * FROM streaming_sql_golden_runtime_filter_outer_sink ORDER BY event_id;

-- A stream cannot be evaluated again, so a hint on it is ignored with a warning.
CREATE FLOW streaming_sql_golden_runtime_filter_stream_flow AS
INSERT INTO streaming_sql_golden_runtime_filter_stream_sink BY NAME
SELECT /*+ RUNTIME_FILTER(events) */ events.event_id, customers.tier, events.amount
FROM STREAM(streaming_sql_golden_runtime_filter_events) AS events
JOIN streaming_sql_golden_runtime_filter_customers AS customers
  ON events.customer_id = customers.customer_id;

REFRESH FLOW streaming_sql_golden_runtime_filter_stream_flow;

SELECT * FROM streaming_sql_golden_runtime_filter_stream_sink ORDER BY event_id;

DROP TABLE streaming_sql_golden_runtime_filter_events;
DROP TABLE streaming_sql_golden_runtime_filter_customers;
DROP TABLE streaming_sql_golden_runtime_filter_inner_sink;
DROP TABLE streaming_sql_golden_runtime_filter_outer_sink;
DROP TABLE streaming_sql_golden_runtime_filter_stream_sink;
