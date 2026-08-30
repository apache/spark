-- FVT Category 7: ASOF JOIN container integration (FVT-ASOF-7-*)

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

-- FVT-ASOF-7-001: temp view hosting ASOF
CREATE OR REPLACE TEMP VIEW asof_matched_v AS
SELECT t.trade_time, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS cnt FROM asof_matched_v;

-- Permanent backing tables for cases that cannot reference temp views (7-002, 7-006, 7-007).
DROP TABLE IF EXISTS asof_perm_trades;
DROP TABLE IF EXISTS asof_perm_quotes;
CREATE TABLE asof_perm_trades USING parquet AS SELECT * FROM trades;
CREATE TABLE asof_perm_quotes USING parquet AS SELECT * FROM quotes;

-- FVT-ASOF-7-002: permanent view hosting ASOF
CREATE OR REPLACE VIEW asof_perm_v AS
SELECT t.trade_time, q.bid_price
FROM asof_perm_trades t ASOF JOIN asof_perm_quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS cnt FROM asof_perm_v;

-- FVT-ASOF-7-003: deferred — materialized view / streaming table (platform follow-up)

-- FVT-ASOF-7-004: regular CTE
WITH matched AS (
  SELECT t.trade_time, t.symbol, q.bid_price
  FROM trades t ASOF JOIN quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
)
SELECT count(*) AS cnt FROM matched;

-- FVT-ASOF-7-005: recursive CTE with ASOF in recursive leg
WITH RECURSIVE chain AS (
  SELECT trade_time AS ts, symbol, 1 AS depth
  FROM trades
  WHERE symbol = 'AAPL' AND trade_time = TIMESTAMP '2026-06-29 10:00:05'
  UNION ALL
  SELECT q.quote_time, c.symbol, c.depth + 1
  FROM chain c ASOF JOIN quotes q
    MATCH_CONDITION (c.ts >= q.quote_time)
    ON c.symbol = q.symbol
  WHERE c.depth < 2
)
SELECT count(*) AS cnt FROM chain;

-- FVT-ASOF-7-006: scalar SQL UDF wrapping ASOF count
CREATE OR REPLACE FUNCTION asof_match_count() RETURNS INT RETURN (
  SELECT count(*)
  FROM asof_perm_trades t ASOF JOIN asof_perm_quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol
);

SELECT asof_match_count() AS cnt;

-- FVT-ASOF-7-007: SQL table function returning ASOF rows
CREATE OR REPLACE FUNCTION asof_matches()
RETURNS TABLE (trade_time TIMESTAMP, bid_price DOUBLE)
RETURN
  SELECT t.trade_time, q.bid_price
  FROM asof_perm_trades t ASOF JOIN asof_perm_quotes q
    MATCH_CONDITION (t.trade_time >= q.quote_time)
    ON t.symbol = q.symbol;

SELECT count(*) AS cnt FROM asof_matches();

-- FVT-ASOF-7-008: excluded — CREATE PROCEDURE is not supported in Spark SQL

-- FVT-ASOF-7-009: CTAS from ASOF result
DROP TABLE IF EXISTS asof_ctas_tgt;
CREATE TABLE asof_ctas_tgt USING parquet AS
SELECT t.trade_time, t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS cnt FROM asof_ctas_tgt;

-- FVT-ASOF-7-010: INSERT INTO from ASOF
DROP TABLE IF EXISTS asof_insert_tgt;
CREATE TABLE asof_insert_tgt (trade_time TIMESTAMP, symbol STRING, bid_price DOUBLE) USING parquet;

INSERT INTO asof_insert_tgt
SELECT t.trade_time, t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS cnt FROM asof_insert_tgt;

-- FVT-ASOF-7-011: excluded — MERGE on parquet rejects row-level DML (ASOF USING subquery not evaluated)
-- FVT-ASOF-7-012a/7-012b: excluded — UPDATE on parquet rejects row-level DML (ASOF subquery not evaluated)
-- FVT-ASOF-7-013: excluded — DELETE on parquet rejects row-level DML (ASOF subquery not evaluated)

-- FVT-ASOF-7-014: cursor over ASOF SELECT (requires scripting + cursorEnabled)
--SET spark.sql.scripting.cursorEnabled=true

--QUERY-DELIMITER-START
BEGIN
  DECLARE fetched_time TIMESTAMP;
  DECLARE fetched_bid DOUBLE;
  DECLARE cur CURSOR FOR
    SELECT t.trade_time, q.bid_price
    FROM trades t ASOF JOIN quotes q
      MATCH_CONDITION (t.trade_time >= q.quote_time)
      ON t.symbol = q.symbol
    ORDER BY t.trade_time;
  OPEN cur;
  FETCH cur INTO fetched_time, fetched_bid;
  CLOSE cur;
  SELECT fetched_time, fetched_bid;
END;
--QUERY-DELIMITER-END
