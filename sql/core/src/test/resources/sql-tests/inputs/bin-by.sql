-- BIN BY relation operator: end-to-end execution and output snapshots.
-- Per-row edge cases (inverted / NULL / zero-length) and DST variants are covered by BinBySuite;
-- this file snapshots the main scenarios plus representative analysis errors.

-- Enable the operator (gated off by default) and pin the session zone to UTC so the TIMESTAMP
-- literals and LTZ output are zone-independent. These are SQL statements, not `--SET` header
-- directives, so they also apply under ThriftServerQueryTestSuite (which ignores header configs).
SET spark.sql.binByRelationOperator.enabled = true;
SET TIME ZONE 'UTC';

CREATE OR REPLACE TEMP VIEW metrics AS
SELECT * FROM VALUES
  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100.0D),
  (TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:12:00', 300.0D)
AS metrics(ts_start, ts_end, value);


-- Single-bin pass-through (row 1) and a multi-bin split with proportional redistribution (row 2).
-- The input range columns pass through unclipped on every sub-row.
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
