-- FVT Category 2: ASOF JOIN semantic behavior (FVT-ASOF-2-*)

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

CREATE OR REPLACE TEMP VIEW deploys(deploy_ts, seq, service, version) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 1, 'api', 'v1.0'),
         (TIMESTAMP '2026-06-29 10:00:00', 2, 'api', 'v1.1'),
         (TIMESTAMP '2026-06-29 10:05:00', 1, 'api', 'v1.2');

CREATE OR REPLACE TEMP VIEW requests(req_ts, seq, service) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 5, 'api'),
         (TIMESTAMP '2026-06-29 10:03:00', 1, 'api');

-- FVT-ASOF-2-001: >= selects largest preceding right row
SELECT t.trade_time, q.quote_time
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol AND t.symbol = 'AAPL'
    AND t.trade_time = TIMESTAMP '2026-06-29 10:00:11';

-- FVT-ASOF-2-002: > excludes exact equality
SELECT t.trade_time, q.quote_time
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', 'X') AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00', 'X'),
            (TIMESTAMP '2026-06-29 09:00:00', 'X') AS q(quote_time, symbol)
  MATCH_CONDITION (t.trade_time > q.quote_time)
  USING (symbol);

-- FVT-ASOF-2-003: >= at exact equality selects equal row
SELECT t.trade_time, q.quote_time
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', 'X') AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00', 'X'),
            (TIMESTAMP '2026-06-29 09:00:00', 'X') AS q(quote_time, symbol)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  USING (symbol);

-- FVT-ASOF-2-004: <= at exact equality selects equal row
SELECT t.trade_time, q.quote_time
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', 'X') AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00', 'X'),
            (TIMESTAMP '2026-06-29 11:00:00', 'X') AS q(quote_time, symbol)
  MATCH_CONDITION (t.trade_time <= q.quote_time)
  USING (symbol);

-- FVT-ASOF-2-005: < at exact equality picks next-following row
SELECT t.trade_time, q.quote_time
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', 'X') AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00', 'X'),
            (TIMESTAMP '2026-06-29 11:00:00', 'X') AS q(quote_time, symbol)
  MATCH_CONDITION (t.trade_time < q.quote_time)
  USING (symbol);

-- FVT-ASOF-2-006: NULL left operand, INNER drops row
SELECT count(*) AS cnt
FROM VALUES (CAST(NULL AS TIMESTAMP), 'AAPL', 100) AS t(trade_time, symbol, quantity) ASOF JOIN
     quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-2-007: NULL left operand, LEFT retains with NULL right columns
SELECT t.trade_time, t.symbol, t.quantity, q.quote_time, q.symbol, q.bid_price
FROM VALUES (CAST(NULL AS TIMESTAMP), 'AAPL', 100) AS t(trade_time, symbol, quantity) LEFT ASOF JOIN
     quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-2-008: NULL right operand never satisfies MATCH_CONDITION
SELECT count(*) AS cnt
FROM trades t ASOF JOIN
     (SELECT CAST(NULL AS TIMESTAMP) AS quote_time, symbol, bid_price FROM quotes) q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-2-009: ON filters right search space before closest-match
SELECT t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol AND q.bid_price > 200
ORDER BY t.symbol;

-- FVT-ASOF-2-011: INNER cardinality bound
SELECT count(*) AS asof_cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS trades_cnt FROM trades;

-- FVT-ASOF-2-012: LEFT cardinality equals left input
SELECT count(*) AS cnt
FROM trades t LEFT ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-2-013: tie at boundary — assert cardinality only
SELECT count(*) AS cnt
FROM VALUES (TIMESTAMP '2026-06-29 10:00:10', 'AAPL') AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:10', 'AAPL', 1.0),
            (TIMESTAMP '2026-06-29 10:00:10', 'AAPL', 2.0) AS q(quote_time, symbol, bid_price)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  USING (symbol);

-- FVT-ASOF-2-014: tie broken by trailing field of STRUCT operand
SELECT r.req_ts, r.seq, d.version
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', 5, 'api') AS r(req_ts, seq, service) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00', 1, 'api', 'v1.0'),
            (TIMESTAMP '2026-06-29 10:00:00', 2, 'api', 'v1.1') AS d(deploy_ts, seq, service, version)
  MATCH_CONDITION ((r.req_ts, r.seq) >= (d.deploy_ts, d.seq))
  ON r.service = d.service;

-- FVT-ASOF-2-015: GOOG unmatched — INNER drops, LEFT retains
SELECT count(*) AS inner_cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
WHERE t.symbol = 'GOOG';

SELECT count(*) AS left_cnt
FROM trades t LEFT ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
WHERE t.symbol = 'GOOG';

-- FVT-ASOF-2-016: non-equi range predicate in ON
SELECT t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON abs(unix_timestamp(t.trade_time) - unix_timestamp(q.quote_time)) < 30
ORDER BY t.symbol, t.trade_time;

-- FVT-ASOF-2-017: disjunction in ON
SELECT count(*) AS cnt
FROM VALUES (CAST(NULL AS STRING), TIMESTAMP '2026-06-29 10:00:00') AS t(symbol, trade_time) ASOF JOIN
     VALUES (CAST(NULL AS STRING), TIMESTAMP '2026-06-29 09:00:00') AS q(symbol, quote_time)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol OR (t.symbol IS NULL AND q.symbol IS NULL);

-- FVT-ASOF-2-018: ON predicate with expression over both tables
SELECT t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON substr(t.symbol, 1, 1) = substr(q.symbol, 1, 1)
ORDER BY t.symbol, t.trade_time;

-- FVT-ASOF-2-019: ON TRUE matches whole right
SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON TRUE;

-- FVT-ASOF-2-020: ON FALSE empties right search space
SELECT count(*) AS inner_cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON FALSE;

SELECT count(*) AS left_cnt
FROM trades t LEFT ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON FALSE;

-- FVT-ASOF-2-021: correlated subquery in ON
SELECT t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
    AND q.bid_price > (SELECT avg(bid_price) FROM quotes qq WHERE qq.symbol = q.symbol)
ORDER BY t.symbol, t.trade_time;

-- FVT-ASOF-2-010: USING is equivalent to ON symbol equality
SELECT count(*) AS on_cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS using_cnt
FROM trades ASOF JOIN quotes
  MATCH_CONDITION (trades.trade_time >= quotes.quote_time)
  USING (symbol);

-- FVT-ASOF-2-022: operand-order symmetry inside MATCH_CONDITION
SELECT t.trade_time, q.quote_time
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (q.quote_time <= t.trade_time)
  ON t.symbol = q.symbol AND t.symbol = 'AAPL'
    AND t.trade_time = TIMESTAMP '2026-06-29 10:00:11';

-- FVT-ASOF-2-023: join operand asymmetry — driving side changes row count
SELECT count(*) AS trades_driving_cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

SELECT count(*) AS quotes_driving_cnt
FROM quotes q ASOF JOIN trades t
  MATCH_CONDITION (q.quote_time <= t.trade_time)
  ON q.symbol = t.symbol;

-- FVT-ASOF-2-024: NULL equi-key in ON never matches
SELECT count(*) AS cnt
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', CAST(NULL AS STRING)) AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 09:00:00', CAST(NULL AS STRING)) AS q(quote_time, symbol)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-2-025: NULL equi-key in USING never matches
SELECT count(*) AS cnt
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', CAST(NULL AS STRING)) AS t(trade_time, symbol) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 09:00:00', CAST(NULL AS STRING)) AS q(quote_time, symbol)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  USING (symbol);

-- FVT-ASOF-2-026: multiple USING columns
SELECT t.trade_time, q.quote_time
FROM VALUES (TIMESTAMP '2026-06-29 10:00:05', 'AAPL', 100) AS t(trade_time, symbol, quantity) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 100) AS q(quote_time, symbol, quantity)
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  USING (symbol, quantity);
