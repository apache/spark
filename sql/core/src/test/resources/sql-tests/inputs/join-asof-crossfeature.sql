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

-- FVT-ASOF-9-011: scalar subquery, outer reference in the left input
SELECT s.symbol,
  (SELECT max(q.bid_price)
   FROM (SELECT * FROM trades t WHERE t.symbol = s.symbol) t
   ASOF JOIN quotes q
     MATCH_CONDITION (t.trade_time >= q.quote_time)
     ON t.symbol = q.symbol) AS max_bid
FROM VALUES ('AAPL'), ('GOOG'), ('MSFT') AS s(symbol)
ORDER BY s.symbol;

-- FVT-ASOF-9-012: scalar subquery, outer reference in the right input
SELECT o.max_bid,
  (SELECT sum(q.bid_price)
   FROM trades t
   ASOF JOIN (SELECT * FROM quotes q WHERE q.bid_price <= o.max_bid) q
     MATCH_CONDITION (t.trade_time >= q.quote_time)
     ON t.symbol = q.symbol) AS total_bid
FROM VALUES (180.12), (500.00) AS o(max_bid)
ORDER BY o.max_bid;

-- FVT-ASOF-9-013: scalar subquery, outer reference in the ON condition
SELECT o.max_bid,
  (SELECT sum(q.bid_price)
   FROM trades t ASOF JOIN quotes q
     MATCH_CONDITION (t.trade_time >= q.quote_time)
     ON t.symbol = q.symbol AND q.bid_price <= o.max_bid) AS total_bid
FROM VALUES (180.12), (500.00) AS o(max_bid)
ORDER BY o.max_bid;

-- FVT-ASOF-9-014: outer references in both inputs
SELECT o.symbol, o.max_bid,
  (SELECT sum(q.bid_price)
   FROM (SELECT * FROM trades t WHERE t.symbol = o.symbol) t
   ASOF JOIN (SELECT * FROM quotes q WHERE q.bid_price <= o.max_bid) q
     MATCH_CONDITION (t.trade_time >= q.quote_time)
     ON t.symbol = q.symbol) AS total_bid
FROM VALUES ('AAPL', 180.12), ('AAPL', 500.00), ('MSFT', 180.12) AS o(symbol, max_bid)
ORDER BY o.symbol, o.max_bid;

-- FVT-ASOF-9-015: IN subquery, outer reference in the left input
SELECT t0.trade_time, t0.symbol,
  t0.quantity IN (
    SELECT t.quantity
    FROM (SELECT * FROM trades t WHERE t.symbol = t0.symbol) t
    ASOF JOIN quotes q
      MATCH_CONDITION (t.trade_time >= q.quote_time)
      ON t.symbol = q.symbol) AS has_quote
FROM trades t0
ORDER BY t0.trade_time;

-- FVT-ASOF-9-016: EXISTS subquery, outer reference in the right input
SELECT o.max_bid
FROM VALUES (100.00), (180.17), (500.00) AS o(max_bid)
WHERE EXISTS (
  SELECT 1
  FROM trades t
  ASOF JOIN (SELECT * FROM quotes q WHERE q.bid_price <= o.max_bid) q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
  WHERE q.bid_price = 180.15)
ORDER BY o.max_bid;

-- FVT-ASOF-9-017: LEFT ASOF JOIN, outer reference in the left input
SELECT s.symbol,
  (SELECT count(*)
   FROM (SELECT * FROM trades t WHERE t.symbol = s.symbol) t
   LEFT ASOF JOIN quotes q
     MATCH_CONDITION (t.trade_time >= q.quote_time)
     ON t.symbol = q.symbol) AS trade_count
FROM VALUES ('AAPL'), ('GOOG'), ('MSFT') AS s(symbol)
ORDER BY s.symbol;

-- FVT-ASOF-9-018: LEFT ASOF JOIN rejects an outer reference in the right input, like LEFT JOIN
SELECT o.max_bid,
  (SELECT count(*)
   FROM trades t
   LEFT ASOF JOIN (SELECT * FROM quotes q WHERE q.bid_price <= o.max_bid) q
     MATCH_CONDITION (t.trade_time >= q.quote_time)
     ON t.symbol = q.symbol) AS trade_count
FROM VALUES (180.12) AS o(max_bid);

-- FVT-ASOF-9-019: MATCH_CONDITION rejects an outer reference
SELECT o.lag,
  (SELECT count(*)
   FROM trades t ASOF JOIN quotes q
     MATCH_CONDITION (t.trade_time >= q.quote_time + o.lag)
     ON t.symbol = q.symbol) AS matched
FROM VALUES (INTERVAL '1' SECOND) AS o(lag);

-- FVT-ASOF-9-020: NULL outer value, outer reference in the right input
SELECT o.max_bid,
  (SELECT sum(q.bid_price)
   FROM trades t
   ASOF JOIN (SELECT * FROM quotes q WHERE o.max_bid IS NULL OR q.bid_price <= o.max_bid) q
     MATCH_CONDITION (t.trade_time >= q.quote_time)
     ON t.symbol = q.symbol) AS total_bid
FROM VALUES (180.12), (NULL) AS o(max_bid)
ORDER BY o.max_bid;

-- FVT-ASOF-9-021: outer reference below an aggregate in the right input
SELECT o.max_bid,
  (SELECT sum(q.bid_price)
   FROM trades t
   ASOF JOIN (SELECT symbol, max(quote_time) AS quote_time, max(bid_price) AS bid_price
              FROM quotes WHERE bid_price <= o.max_bid
              GROUP BY symbol) q
     MATCH_CONDITION (t.trade_time >= q.quote_time)
     ON t.symbol = q.symbol) AS total_bid
FROM VALUES (180.12), (500.00) AS o(max_bid)
ORDER BY o.max_bid;

-- FVT-ASOF-9-022: outer reference below a LIMIT in the right input
SELECT o.max_bid,
  (SELECT sum(q.bid_price)
   FROM trades t
   ASOF JOIN (SELECT * FROM quotes q WHERE q.bid_price <= o.max_bid
              ORDER BY q.quote_time LIMIT 2) q
     MATCH_CONDITION (t.trade_time >= q.quote_time)
     ON t.symbol = q.symbol) AS total_bid
FROM VALUES (180.12), (500.00) AS o(max_bid)
ORDER BY o.max_bid;
