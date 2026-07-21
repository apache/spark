-- FVT Category 10: ASOF JOIN feature gating (FVT-ASOF-10-*)

--SET spark.sql.join.asofJoin.enabled=false

-- FVT-ASOF-10-001: ASOF JOIN rejected when feature flag is disabled
SELECT * FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL') AS t(ts, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 09:00:00', 'AAPL') AS q(ts, symbol)
  MATCH_CONDITION (t.ts >= q.ts)
  ON t.symbol = q.symbol;
