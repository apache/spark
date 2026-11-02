DROP TABLE IF EXISTS streaming_sql_golden_lateral_events;
DROP TABLE IF EXISTS streaming_sql_golden_lateral_adjustments;
DROP TABLE IF EXISTS streaming_sql_golden_lateral_sink;

CREATE TABLE streaming_sql_golden_lateral_events (
  event_id INT, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_lateral_events VALUES
  (1, 10),
  (2, 20),
  (3, 30);

CREATE TABLE streaming_sql_golden_lateral_adjustments (
  event_id INT, adjustment INT) USING parquet;
INSERT INTO streaming_sql_golden_lateral_adjustments VALUES
  (1, 5),
  (2, 3);

CREATE TABLE streaming_sql_golden_lateral_sink (
  event_id INT, amount INT, adjusted_amount INT) USING parquet;

CREATE FLOW streaming_sql_golden_lateral_flow AS
INSERT INTO streaming_sql_golden_lateral_sink BY NAME
SELECT events.event_id, events.amount,
  events.amount + adjustments.adjustment AS adjusted_amount
FROM STREAM(streaming_sql_golden_lateral_events) AS events,
LATERAL (
  SELECT adjustment
  FROM streaming_sql_golden_lateral_adjustments
  WHERE streaming_sql_golden_lateral_adjustments.event_id = events.event_id
) AS adjustments;

REFRESH FLOW streaming_sql_golden_lateral_flow;

SELECT * FROM streaming_sql_golden_lateral_sink ORDER BY event_id;

DROP TABLE streaming_sql_golden_lateral_events;
DROP TABLE streaming_sql_golden_lateral_adjustments;
DROP TABLE streaming_sql_golden_lateral_sink;
