-- FVT Category 8: ASOF JOIN expression integration (FVT-ASOF-8-*)

--SET spark.sql.join.asofJoin.enabled=true

CREATE OR REPLACE TEMP VIEW trades(trade_time, symbol, quantity) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:05', 'AAPL', 100),
         (TIMESTAMP '2026-06-29 10:00:11', 'AAPL', 200),
         (TIMESTAMP '2026-06-29 10:00:12', 'MSFT',  50),
         (TIMESTAMP '2026-06-29 09:59:59', 'GOOG',  30);

CREATE OR REPLACE TEMP VIEW quotes(quote_time, symbol, bid_price) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 180.10),
         (TIMESTAMP '2026-06-29 10:00:07', 'AAPL', 180.15),
         (TIMESTAMP '2026-06-29 10:00:10', 'AAPL', 180.20),
         (TIMESTAMP '2026-06-29 10:00:08', 'MSFT', 420.50);

-- FVT-ASOF-8-001: window function on ASOF result
SELECT t.trade_time, t.symbol, q.bid_price,
       row_number() OVER (PARTITION BY t.symbol ORDER BY t.trade_time) AS rn
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.symbol, t.trade_time;

-- FVT-ASOF-8-002: aggregate over ASOF — covered by FVT-ASOF-6-003

-- FVT-ASOF-8-003: deterministic function in MATCH_CONDITION operand
SELECT t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (year(t.trade_time) >= year(q.quote_time))
  ON t.symbol = q.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-8-004: CAST inside MATCH_CONDITION operand
SELECT t.k, r.k AS matched_k
FROM VALUES (100) AS t(k) ASOF JOIN VALUES (99), (101) AS r(k)
  MATCH_CONDITION (CAST(t.k AS STRING) >= CAST(r.k AS STRING));

-- FVT-ASOF-8-005: CASE inside MATCH_CONDITION operand
SELECT t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (
    CASE WHEN t.symbol = 'MSFT' THEN t.trade_time - INTERVAL 1 HOUR
         ELSE t.trade_time END >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-8-006: deterministic SQL UDF rejected in MATCH_CONDITION (UNSUPPORTED_SQL_UDF_USAGE)
CREATE OR REPLACE FUNCTION shift_ts(ts TIMESTAMP) RETURNS TIMESTAMP
RETURN ts - INTERVAL 1 HOUR;

SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (shift_ts(t.trade_time) >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-8-006a: builtin interval transform (same operand transform, expressed with a builtin instead of a SQL UDF)
SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time - INTERVAL 1 HOUR >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-8-007: non-deterministic SQL UDF rejected in MATCH_CONDITION (UNSUPPORTED_SQL_UDF_USAGE)
CREATE OR REPLACE FUNCTION jitter_ts(ts TIMESTAMP) RETURNS TIMESTAMP NOT DETERMINISTIC
RETURN ts + INTERVAL 1 SECOND;

SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (jitter_ts(t.trade_time) >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-8-008: CURRENT_TIMESTAMP in MATCH_CONDITION operand (query-foldable constant)
SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (current_timestamp() >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-8-008a: literal constant in MATCH_CONDITION operand (right)
SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= TIMESTAMP '2026-06-29 10:00:00')
  ON t.symbol = q.symbol;

-- FVT-ASOF-8-008b: literal constant in MATCH_CONDITION operand (left)
SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (TIMESTAMP '2026-06-29 10:00:00' >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-8-009: QUALIFY on ASOF — covered by FVT-ASOF-6-007

-- FVT-ASOF-8-010: date_trunc on both MATCH_CONDITION operands
SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (
    date_trunc('hour', t.trade_time) >= date_trunc('hour', q.quote_time))
  ON t.symbol = q.symbol;
