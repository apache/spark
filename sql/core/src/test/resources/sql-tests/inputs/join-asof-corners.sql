-- FVT Category 5: ASOF JOIN corner cases (FVT-ASOF-5-*)

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

-- FVT-ASOF-5-001: empty right, INNER
SELECT count(*) AS cnt
FROM trades t ASOF JOIN (SELECT * FROM quotes WHERE 1 = 0) q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-5-002: empty right, LEFT
SELECT count(*) AS cnt
FROM trades t LEFT ASOF JOIN (SELECT * FROM quotes WHERE 1 = 0) q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-5-003: empty left, INNER
SELECT count(*) AS cnt
FROM (SELECT * FROM trades WHERE 1 = 0) t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-5-004: empty left, LEFT
SELECT count(*) AS cnt
FROM (SELECT * FROM trades WHERE 1 = 0) t LEFT ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-5-005: both empty
SELECT count(*) AS inner_cnt
FROM (SELECT * FROM trades WHERE 1 = 0) t ASOF JOIN (SELECT * FROM quotes WHERE 1 = 0) q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS left_cnt
FROM (SELECT * FROM trades WHERE 1 = 0) t LEFT ASOF JOIN (SELECT * FROM quotes WHERE 1 = 0) q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-5-006: single-row both sides, matching
SELECT t.ts, r.v
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', 1) AS t(ts, k) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 09:00:00', 42) AS r(ts, v)
  MATCH_CONDITION (t.ts >= r.ts);

-- FVT-ASOF-5-007: single-row both sides, non-matching
SELECT count(*) AS inner_cnt
FROM VALUES (TIMESTAMP '2026-06-29 08:00:00', 1) AS t(ts, k) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 09:00:00', 42) AS r(ts, v)
  MATCH_CONDITION (t.ts >= r.ts);

SELECT count(*) AS left_cnt
FROM VALUES (TIMESTAMP '2026-06-29 08:00:00', 1) AS t(ts, k) LEFT ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 09:00:00', 42) AS r(ts, v)
  MATCH_CONDITION (t.ts >= r.ts);

-- FVT-ASOF-5-008: all-NULL left operand column, INNER
SELECT count(*) AS inner_cnt
FROM (SELECT CAST(NULL AS TIMESTAMP) AS trade_time, symbol, quantity FROM trades) t ASOF JOIN
     quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS left_cnt
FROM (SELECT CAST(NULL AS TIMESTAMP) AS trade_time, symbol, quantity FROM trades) t LEFT ASOF JOIN
     quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-5-009: all-NULL right operand column
SELECT count(*) AS inner_cnt
FROM trades t ASOF JOIN
     (SELECT CAST(NULL AS TIMESTAMP) AS quote_time, symbol, bid_price FROM quotes) q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS left_cnt
FROM trades t LEFT ASOF JOIN
     (SELECT CAST(NULL AS TIMESTAMP) AS quote_time, symbol, bid_price FROM quotes) q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-5-010: duplicate right rows at exact match boundary
SELECT count(*) AS cnt
FROM VALUES (TIMESTAMP '2026-06-29 10:00:10', 'AAPL') AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:10', 'AAPL', 1.0),
            (TIMESTAMP '2026-06-29 10:00:10', 'AAPL', 2.0) AS q(quote_time, symbol, bid_price)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  USING (symbol);

-- FVT-ASOF-5-011: duplicate right rows at same preceding time
SELECT count(*) AS cnt
FROM VALUES (TIMESTAMP '2026-06-29 10:00:12', 'AAPL') AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:07', 'AAPL', 1.0),
            (TIMESTAMP '2026-06-29 10:00:07', 'AAPL', 2.0) AS q(quote_time, symbol, bid_price)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  USING (symbol);

-- FVT-ASOF-5-012: self-join
SELECT a.trade_time, b.trade_time AS prev_trade_time
FROM trades a ASOF JOIN trades b
  MATCH_CONDITION (a.trade_time > b.trade_time)
  ON a.symbol = b.symbol
ORDER BY a.trade_time, b.trade_time;

-- FVT-ASOF-5-013: dense right — every left row matches
SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-5-014: no left row matches (all left precede all right)
SELECT count(*) AS inner_cnt
FROM VALUES (TIMESTAMP '2026-06-29 08:00:00', 'AAPL') AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 1.0) AS q(quote_time, symbol, bid_price)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  USING (symbol);

SELECT count(*) AS left_cnt
FROM VALUES (TIMESTAMP '2026-06-29 08:00:00', 'AAPL') AS t(trade_time, symbol) LEFT ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 1.0) AS q(quote_time, symbol, bid_price)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  USING (symbol);

-- FVT-ASOF-5-015: single right row per equi-key
SELECT t.symbol, q.bid_price
FROM trades t ASOF JOIN
     (SELECT quote_time, symbol, bid_price FROM quotes WHERE symbol = 'MSFT') q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-5-016: NaN in DOUBLE operand (ordinary ordering)
SELECT t.k, r.k AS matched_k
FROM VALUES (CAST('NaN' AS DOUBLE)) AS t(k) ASOF JOIN
     VALUES (CAST(1.0 AS DOUBLE)), (CAST('NaN' AS DOUBLE)) AS r(k)
  MATCH_CONDITION (t.k >= r.k);
