-- FVT Category 1: ASOF JOIN grammar coverage (FVT-ASOF-1-*)

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

CREATE OR REPLACE TEMP VIEW alerts(alert_time, host) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 'db-01');

CREATE OR REPLACE TEMP VIEW maintenance(window_start, host) AS
  VALUES (TIMESTAMP '2026-06-29 08:00:00', 'db-01'),
         (TIMESTAMP '2026-06-29 12:00:00', 'db-01');

-- FVT-ASOF-1-001: default INNER with MATCH_CONDITION and ON
SELECT t.trade_time, t.symbol, q.bid_price
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.trade_time, t.symbol;

-- FVT-ASOF-1-002: explicit INNER ASOF JOIN
SELECT t.trade_time, t.symbol, q.bid_price
FROM trades t INNER ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.trade_time, t.symbol;

-- FVT-ASOF-1-003: LEFT ASOF JOIN retains unmatched left rows
SELECT t.trade_time, t.symbol, q.bid_price
FROM trades t LEFT ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol
ORDER BY t.trade_time, t.symbol;

-- FVT-ASOF-1-004: LEFT OUTER ASOF JOIN
SELECT count(*) AS cnt
FROM trades t LEFT OUTER ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-1-005: MATCH_CONDITION with USING
SELECT trade_time, symbol, bid_price
FROM trades ASOF JOIN quotes
  MATCH_CONDITION (trades.trade_time >= quotes.quote_time)
  USING (symbol)
ORDER BY trade_time, symbol;

-- FVT-ASOF-1-006: MATCH_CONDITION alone, no ON or USING
SELECT s.ts, s.value AS s_value, r.value AS r_value
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', 1) AS s(ts, value) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 09:00:00', 42) AS r(ts, value)
  MATCH_CONDITION (s.ts >= r.ts);

-- FVT-ASOF-1-008: MATCH_CONDITION with >
SELECT count(*) AS cnt
FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time > q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-1-009: MATCH_CONDITION with <=
SELECT a.alert_time, m.window_start
FROM alerts a ASOF JOIN maintenance m
  MATCH_CONDITION (a.alert_time <= m.window_start)
  ON a.host = m.host;

-- FVT-ASOF-1-010: MATCH_CONDITION with <
SELECT a.alert_time, m.window_start
FROM alerts a ASOF JOIN maintenance m
  MATCH_CONDITION (a.alert_time < m.window_start)
  ON a.host = m.host;

-- FVT-ASOF-1-011: MATCH_CONDITION with tuple / STRUCT operands
SELECT r.req_ts, r.seq, d.version
FROM requests r ASOF JOIN deploys d
  MATCH_CONDITION ((r.req_ts, r.seq) >= (d.deploy_ts, d.seq))
  ON r.service = d.service
ORDER BY r.req_ts, r.seq;

-- FVT-ASOF-1-012: scalar transformation inside MATCH_CONDITION operand
SELECT e.event_time, e.tenant_id, d.effective_time
FROM VALUES (TIMESTAMP '2026-06-29 11:00:00', 1) AS e(event_time, tenant_id) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 09:00:00', 1) AS d(effective_time, tenant_id)
  MATCH_CONDITION (e.event_time >= d.effective_time + INTERVAL 1 HOUR)
  ON e.tenant_id = d.tenant_id;

-- FVT-ASOF-1-013: RIGHT ASOF JOIN rejected
SELECT * FROM trades t RIGHT ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-1-014: FULL ASOF JOIN rejected
SELECT * FROM trades t FULL ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-1-015: CROSS ASOF JOIN rejected
SELECT * FROM trades t CROSS ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time);

-- FVT-ASOF-1-016: NATURAL ASOF JOIN rejected
SELECT * FROM trades NATURAL ASOF JOIN quotes
  MATCH_CONDITION (trades.trade_time >= quotes.quote_time);

-- FVT-ASOF-1-019: LEFT SEMI ASOF JOIN rejected
SELECT * FROM trades t LEFT SEMI ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time)
  ON t.symbol = q.symbol;

-- FVT-ASOF-1-017: missing MATCH_CONDITION
SELECT * FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol;

-- FVT-ASOF-1-018: multiple comparisons in MATCH_CONDITION
SELECT * FROM trades t ASOF JOIN quotes q
  MATCH_CONDITION (t.trade_time >= q.quote_time AND t.symbol = q.symbol)
  ON t.symbol = q.symbol;
