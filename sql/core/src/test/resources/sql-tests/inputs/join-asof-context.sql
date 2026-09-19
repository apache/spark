-- FVT Category 6: ASOF JOIN query-context integration (FVT-ASOF-6-*)

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

CREATE OR REPLACE TEMP VIEW alerts(alert_time, host) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 'db-01');

CREATE OR REPLACE TEMP VIEW maintenance(window_start, host) AS
  VALUES (TIMESTAMP '2026-06-29 08:00:00', 'db-01'),
         (TIMESTAMP '2026-06-29 12:00:00', 'db-01');

-- FVT-ASOF-6-001: WHERE after ASOF
SELECT t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
WHERE q.bid_price > 200
ORDER BY t.symbol;

-- FVT-ASOF-6-002: filter left before ASOF via subquery
SELECT t.trade_time, t.symbol, q.bid_price
FROM (SELECT * FROM trades WHERE symbol = 'AAPL') t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-6-003: GROUP BY on ASOF result
SELECT t.symbol, avg(q.bid_price) AS avg_bid
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
GROUP BY t.symbol
ORDER BY t.symbol;

-- FVT-ASOF-6-004: HAVING after GROUP BY
SELECT t.symbol, count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
GROUP BY t.symbol
HAVING count(*) > 1
ORDER BY t.symbol;

-- FVT-ASOF-6-005: ORDER BY on result
SELECT t.trade_time, t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-6-006: LIMIT / OFFSET
SELECT t.trade_time, t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.trade_time
LIMIT 2 OFFSET 1;

-- FVT-ASOF-6-007: QUALIFY on ASOF result
SELECT t.trade_time, t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
QUALIFY row_number() OVER (PARTITION BY t.symbol ORDER BY t.trade_time DESC) = 1
ORDER BY t.symbol;

-- FVT-ASOF-6-008: DISTINCT
SELECT DISTINCT q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY bid_price;

-- FVT-ASOF-6-009: column aliasing
SELECT t.trade_time AS trd, q.bid_price AS bid
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY trd;

-- FVT-ASOF-6-010: table aliasing on right
SELECT t.trade_time, x.bid_price
FROM trades t ASOF JOIN quotes AS x
  MATCH_CONDITION (t.trade_time >= x.quote_time)
  ON t.symbol = x.symbol
ORDER BY t.trade_time;

-- FVT-ASOF-6-011: scalar subquery over ASOF in projection
SELECT (
  SELECT max(q.bid_price)
  FROM trades t ASOF JOIN quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
) AS max_bid;

-- FVT-ASOF-6-012: EXISTS with ASOF inside
SELECT count(*) AS cnt
FROM trades t
WHERE EXISTS (
  SELECT 1
  FROM trades t2 ASOF JOIN quotes q
    MATCH_CONDITION (t2.trade_time >= q.quote_time)
    ON t2.symbol = q.symbol
  WHERE q.bid_price > 200
);

-- FVT-ASOF-6-013: IN subquery from ASOF
SELECT symbol, bid_price
FROM quotes
WHERE bid_price IN (
  SELECT q.bid_price
  FROM trades t ASOF JOIN quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
)
ORDER BY symbol, bid_price;

-- FVT-ASOF-6-014: UNION ALL of two ASOF directions
SELECT t.trade_time, t.symbol, q.quote_time, 'backward' AS direction
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
UNION ALL
SELECT a.alert_time, a.host, m.window_start, 'forward' AS direction
FROM alerts a ASOF JOIN maintenance m
  MATCH_CONDITION (a.alert_time <= m.window_start)
  ON a.host = m.host
ORDER BY trade_time, symbol;

-- FVT-ASOF-6-015: INTERSECT
SELECT t.symbol
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
INTERSECT
SELECT symbol FROM quotes;

-- FVT-ASOF-6-016: EXCEPT
SELECT t.symbol
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
EXCEPT
SELECT 'GOOG';

-- FVT-ASOF-6-017: LATERAL baseline vs ASOF row equality (AAPL)
SELECT l.trade_time, l.lateral_bid, a.asof_bid
FROM (
  SELECT t.trade_time, l.bid_price AS lateral_bid
  FROM trades t,
  LATERAL (
    SELECT q.bid_price
    FROM quotes q
    WHERE q.symbol = t.symbol AND q.quote_time <= t.trade_time
    ORDER BY q.quote_time DESC
    LIMIT 1
  ) l
  WHERE t.symbol = 'AAPL'
) l
JOIN (
  SELECT t.trade_time, q.bid_price AS asof_bid
  FROM trades t ASOF JOIN quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
  WHERE t.symbol = 'AAPL'
) a ON l.trade_time = a.trade_time
ORDER BY l.trade_time;

-- FVT-ASOF-6-018: PIVOT on ASOF result
SELECT *
FROM (
  SELECT t.symbol, q.bid_price, extract(hour FROM q.quote_time) AS h
  FROM trades t ASOF JOIN quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
) PIVOT (max(bid_price) FOR h IN (10))
ORDER BY symbol;

-- FVT-ASOF-6-019: UNPIVOT on ASOF result
SELECT symbol, hour_bucket, bid_price
FROM (
  SELECT t.symbol,
         CASE WHEN extract(hour FROM q.quote_time) = 10 THEN q.bid_price END AS hour_10
  FROM trades t ASOF JOIN quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
) src
UNPIVOT (bid_price FOR hour_bucket IN (hour_10))
ORDER BY symbol, hour_bucket;
