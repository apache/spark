-- FVT Category 9: ASOF JOIN cross-feature interaction (FVT-ASOF-9-*)

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

-- FVT-ASOF-9-001: two ASOF JOINs chained
SELECT t.symbol, q.bid_price, q2.bid_price AS earlier_bid
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ASOF JOIN quotes q2
  MATCH_CONDITION (q.quote_time >= q2.quote_time)
  ON q.symbol = q2.symbol AND q2.bid_price < q.bid_price
WHERE t.symbol = 'AAPL'
ORDER BY t.trade_time;

-- FVT-ASOF-9-002: ASOF followed by regular INNER JOIN
SELECT t.symbol, q.bid_price, s.sector
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
INNER JOIN VALUES ('AAPL', 'Tech'), ('MSFT', 'Tech') AS s(symbol, sector)
  ON t.symbol = s.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-9-003: regular LEFT JOIN after ASOF
SELECT t.symbol, q.bid_price, s.sector
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
LEFT JOIN VALUES ('AAPL', 'Tech'), ('MSFT', 'Tech') AS s(symbol, sector)
  ON t.symbol = s.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-9-004: regular JOIN inside ASOF right side
SELECT t.symbol, qs.bid_price, qs.sector
FROM trades t ASOF JOIN (
  SELECT q.quote_time, q.symbol, q.bid_price, s.sector
  FROM quotes q
  INNER JOIN VALUES ('AAPL', 'Tech'), ('MSFT', 'Tech') AS s(symbol, sector)
    ON q.symbol = s.symbol
) qs
  MATCH_CONDITION (t.trade_time >= qs.quote_time)
  ON t.symbol = qs.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-9-005: ASOF result UNPIVOT
SELECT symbol, metric, val
FROM (
  SELECT t.symbol, q.bid_price, t.quantity
  FROM trades t ASOF JOIN quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
) src
UNPIVOT (val FOR metric IN (bid_price, quantity))
ORDER BY symbol, metric;

-- FVT-ASOF-9-006: ASOF composed with LATERAL
SELECT t.trade_time, t.symbol, l.doubled
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol,
LATERAL (SELECT q.bid_price * 2 AS doubled) l
ORDER BY t.trade_time;

-- FVT-ASOF-9-007: session variable in MATCH_CONDITION operand
DECLARE v_offset INTERVAL DAY TO SECOND;
SET VARIABLE v_offset = INTERVAL 1 HOUR;

SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time + v_offset)
  ON t.symbol = q.symbol;

-- FVT-ASOF-9-008: parameter marker via EXECUTE IMMEDIATE
EXECUTE IMMEDIATE
  'SELECT count(*) AS cnt FROM trades t ASOF JOIN quotes q
     MATCH_CONDITION (t.trade_time >= q.quote_time + ?)
     ON t.symbol = q.symbol'
USING INTERVAL 1 HOUR;

-- FVT-ASOF-9-009: both sides are filtered subqueries
SELECT t.symbol, q.bid_price
FROM (SELECT * FROM trades WHERE quantity > 50) t ASOF JOIN
     (SELECT * FROM quotes WHERE bid_price > 100) q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-9-010: CTE containing ASOF, then second ASOF on CTE output
WITH first_match AS (
  SELECT t.trade_time, t.symbol, q.bid_price
  FROM trades t ASOF JOIN quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
)
SELECT fm.symbol, fm.bid_price, e.extra_val
FROM first_match fm ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00', 1.0),
            (TIMESTAMP '2026-06-29 10:00:08', 2.0) AS e(ts, extra_val)
  MATCH_CONDITION (fm.trade_time >= e.ts)
ORDER BY fm.trade_time;
