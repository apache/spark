SET spark.sql.binByRelationOperator.enabled = true;
SET TIME ZONE 'UTC';

CREATE OR REPLACE TEMP VIEW metrics AS
SELECT * FROM VALUES
  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100.0D),
  (TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:12:00', 300.0D)
AS metrics(ts_start, ts_end, value);


-- Basic split: one row fits a single bin, the other spans multiple bins.
SELECT * FROM metrics BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
  DISTRIBUTE UNIFORM (value)
);


-- Composability: feed BIN BY into a downstream GROUP BY.
SELECT bin_start, SUM(value) AS total
FROM metrics BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
  DISTRIBUTE UNIFORM (value)
)
GROUP BY bin_start
ORDER BY bin_start;


-- Custom ALIGN TO origin and renamed output columns.
SELECT * FROM VALUES
  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:08:00', 100.0D)
  AS t(ts_start, ts_end, value)
BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  ALIGN TO TIMESTAMP '2024-01-01 00:01:00'
  DISTRIBUTE UNIFORM (value)
  BIN_START AS window_start
  BIN_END AS window_end
  BIN_DISTRIBUTE_RATIO AS frac
);


-- Analysis errors: duplicate DISTRIBUTE column and a non-timestamp RANGE column.
SELECT * FROM metrics BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  DISTRIBUTE UNIFORM (value, value)
);

SELECT * FROM metrics BIN BY (
  RANGE value TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  DISTRIBUTE UNIFORM (value)
);
