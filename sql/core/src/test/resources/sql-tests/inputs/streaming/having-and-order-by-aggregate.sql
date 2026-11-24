-- ORDER BY an aggregate function on top of a HAVING clause that uses an aggregate expression that
-- the SELECT list does not compute. A flow appends to its target table, and a streaming query can
-- sort an aggregation only in complete output mode, so the flow fails like any other ORDER BY of a
-- streaming aggregation.

DROP TABLE IF EXISTS streaming_sql_golden_having_order_by_events;
DROP TABLE IF EXISTS streaming_sql_golden_having_order_by_sink;

CREATE TABLE streaming_sql_golden_having_order_by_events (
  event_time TIMESTAMP, category STRING, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_having_order_by_events VALUES
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 2),
  (TIMESTAMP '2024-01-01 00:00:05', 'books', 3),
  (TIMESTAMP '2024-01-01 00:01:02', 'games', 7);

CREATE TABLE streaming_sql_golden_having_order_by_sink (
  window_start TIMESTAMP, category STRING) USING parquet;

CREATE FLOW streaming_sql_golden_having_order_by_flow AS
INSERT INTO streaming_sql_golden_having_order_by_sink BY NAME
SELECT window(event_time, '1 minute').start AS window_start, category
FROM STREAM(streaming_sql_golden_having_order_by_events)
  WATERMARK event_time DELAY OF INTERVAL 10 SECONDS
GROUP BY window(event_time, '1 minute'), category
HAVING sum(amount) > 2
ORDER BY max(amount);

REFRESH FLOW streaming_sql_golden_having_order_by_flow;

DROP TABLE streaming_sql_golden_having_order_by_events;
DROP TABLE streaming_sql_golden_having_order_by_sink;
