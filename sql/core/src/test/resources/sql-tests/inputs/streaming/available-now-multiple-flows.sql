DROP TABLE IF EXISTS streaming_sql_golden_multiple_flows_source;
DROP TABLE IF EXISTS streaming_sql_golden_multiple_flows_intermediate;
DROP TABLE IF EXISTS streaming_sql_golden_multiple_flows_sink;

CREATE TABLE streaming_sql_golden_multiple_flows_source
  (id INT, value STRING) USING parquet;
INSERT INTO streaming_sql_golden_multiple_flows_source
VALUES (1, 'one'), (2, 'two');

CREATE TABLE streaming_sql_golden_multiple_flows_intermediate
  (id INT, value STRING) USING parquet;
CREATE TABLE streaming_sql_golden_multiple_flows_sink
  (id INT, value STRING) USING parquet;

CREATE FLOW streaming_sql_golden_multiple_flows_downstream AS
INSERT INTO streaming_sql_golden_multiple_flows_sink BY NAME
SELECT id, concat(value, '!') AS value
FROM STREAM(streaming_sql_golden_multiple_flows_intermediate);

CREATE FLOW streaming_sql_golden_multiple_flows_upstream AS
INSERT INTO streaming_sql_golden_multiple_flows_intermediate BY NAME
SELECT id, upper(value) AS value
FROM STREAM(streaming_sql_golden_multiple_flows_source);

-- Start the downstream flow before its upstream flow has produced any rows.
REFRESH FLOW streaming_sql_golden_multiple_flows_downstream;
SELECT * FROM streaming_sql_golden_multiple_flows_sink ORDER BY id;

REFRESH FLOW streaming_sql_golden_multiple_flows_upstream;
SELECT * FROM streaming_sql_golden_multiple_flows_intermediate ORDER BY id;
SELECT * FROM streaming_sql_golden_multiple_flows_sink ORDER BY id;

REFRESH FLOW streaming_sql_golden_multiple_flows_downstream;
SELECT * FROM streaming_sql_golden_multiple_flows_sink ORDER BY id;

INSERT INTO streaming_sql_golden_multiple_flows_source VALUES (3, 'three');

-- Interleave the refreshes in the opposite order. The downstream flow has no new input yet.
REFRESH FLOW streaming_sql_golden_multiple_flows_downstream;
SELECT * FROM streaming_sql_golden_multiple_flows_sink ORDER BY id;

REFRESH FLOW streaming_sql_golden_multiple_flows_upstream;
REFRESH FLOW streaming_sql_golden_multiple_flows_downstream;
SELECT * FROM streaming_sql_golden_multiple_flows_sink ORDER BY id;

DROP TABLE streaming_sql_golden_multiple_flows_source;
DROP TABLE streaming_sql_golden_multiple_flows_intermediate;
DROP TABLE streaming_sql_golden_multiple_flows_sink;
