-- FVT Category 3: ASOF JOIN error conditions (FVT-ASOF-3-*)

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

-- FVT-ASOF-3-001: scalar subquery in MATCH_CONDITION operand
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= (SELECT max(trade_time) FROM trades))
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-002: aggregate in MATCH_CONDITION operand
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= max(q.quote_time))
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-003: window function in MATCH_CONDITION operand
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= lead(q.quote_time) OVER (
    PARTITION BY q.symbol ORDER BY q.quote_time))
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-004: generator in MATCH_CONDITION operand
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= EXPLODE(ARRAY(q.quote_time)))
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-005: non-deterministic function in MATCH_CONDITION operand
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (rand() >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-006: = rejected in MATCH_CONDITION (parse time)
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time = q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-007: <> rejected in MATCH_CONDITION (parse time)
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time <> q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-008: != rejected in MATCH_CONDITION (parse time)
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time != q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-009: IS DISTINCT FROM rejected in MATCH_CONDITION (parse time)
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time IS DISTINCT FROM q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-009a: IS NOT DISTINCT FROM rejected in MATCH_CONDITION (parse time)
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time IS NOT DISTINCT FROM q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-010: MAP operand rejected
SELECT * FROM VALUES (MAP('a', 1)) AS t(m) ASOF JOIN VALUES (MAP('a', 1)) AS r(m)
  MATCH_CONDITION (t.m >= r.m);

-- FVT-ASOF-3-011: ARRAY of non-orderable elements rejected
SELECT * FROM VALUES (ARRAY(MAP('a', 1))) AS t(a) ASOF JOIN VALUES (ARRAY(MAP('a', 1))) AS r(a)
  MATCH_CONDITION (t.a >= r.a);

-- FVT-ASOF-3-012: incompatible types in MATCH_CONDITION
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.symbol)
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-013: STRUCT with non-orderable field rejected
SELECT * FROM VALUES (named_struct('a', 1, 'm', MAP('a', 1))) AS t(s) ASOF JOIN
     VALUES (named_struct('a', 1, 'm', MAP('a', 1))) AS r(s)
  MATCH_CONDITION (t.s >= r.s);

-- FVT-ASOF-3-014: operand references both sides
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time + (q.quote_time - t.trade_time) >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-015: both operands reference same table
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= t.trade_time - INTERVAL 1 HOUR)
  ON t.symbol = q.symbol;

-- FVT-ASOF-3-017: aggregate in ON
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol AND q.bid_price > avg(q.bid_price);

-- FVT-ASOF-3-018: window function in ON
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
    AND row_number() OVER (PARTITION BY q.symbol ORDER BY q.quote_time) = 1;

-- FVT-ASOF-3-019: generator in ON
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON EXPLODE(ARRAY(t.symbol, q.symbol)) IS NOT NULL;

-- FVT-ASOF-3-020: non-deterministic function in ON
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol AND RAND() > 0.5;

-- FVT-ASOF-3-021: non-boolean ON
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol;

-- FVT-ASOF-3-022: both ON and USING
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
  USING (symbol);

-- FVT-ASOF-3-023: right operand references only right table
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (q.quote_time >= q.quote_time - INTERVAL 1 HOUR)
  ON t.symbol = q.symbol;
