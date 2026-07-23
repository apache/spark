-- FVT Category 4: ASOF JOIN data type coverage (FVT-ASOF-4-*)

--SET spark.sql.join.asofJoin.enabled=true

CREATE OR REPLACE TEMP VIEW deploys(deploy_ts, seq, service, version) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 1, 'api', 'v1.0'),
         (TIMESTAMP '2026-06-29 10:00:00', 2, 'api', 'v1.1'),
         (TIMESTAMP '2026-06-29 10:05:00', 1, 'api', 'v1.2');

CREATE OR REPLACE TEMP VIEW requests(req_ts, seq, service) AS
  VALUES (TIMESTAMP '2026-06-29 10:00:00', 5, 'api'),
         (TIMESTAMP '2026-06-29 10:03:00', 1, 'api');

-- FVT-ASOF-4-001: DATE operand
SELECT t.d, r.d AS matched_d
FROM VALUES (DATE '2026-06-29') AS t(d) ASOF JOIN
     VALUES (DATE '2026-06-28'), (DATE '2026-06-29') AS r(d)
  MATCH_CONDITION (t.d >= r.d);

-- FVT-ASOF-4-002: TIMESTAMP anchor — covered by trades/quotes in categories 1-3

-- FVT-ASOF-4-003: TIMESTAMP_NTZ operand
SELECT t.ts, r.ts AS matched_ts
FROM VALUES (TIMESTAMP_NTZ '2026-06-29 10:00:00') AS t(ts) ASOF JOIN
     VALUES (TIMESTAMP_NTZ '2026-06-29 09:00:00') AS r(ts)
  MATCH_CONDITION (t.ts >= r.ts);

-- FVT-ASOF-4-004: INT operand
SELECT t.k, r.k AS matched_k
FROM VALUES (10) AS t(k) ASOF JOIN VALUES (5), (7) AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-004a: TINYINT operand
SELECT t.k, r.k AS matched_k
FROM VALUES (CAST(10 AS TINYINT)) AS t(k) ASOF JOIN VALUES (CAST(5 AS TINYINT)) AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-004b: SMALLINT operand
SELECT t.k, r.k AS matched_k
FROM VALUES (CAST(10 AS SMALLINT)) AS t(k) ASOF JOIN VALUES (CAST(5 AS SMALLINT)) AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-004c: BIGINT operand
SELECT t.k, r.k AS matched_k
FROM VALUES (CAST(10 AS BIGINT)) AS t(k) ASOF JOIN VALUES (CAST(5 AS BIGINT)) AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-005: DECIMAL operand
SELECT t.k, r.k AS matched_k
FROM VALUES (CAST(10.50 AS DECIMAL(10, 2))) AS t(k) ASOF JOIN
     VALUES (CAST(5.25 AS DECIMAL(10, 2))), (CAST(7.75 AS DECIMAL(10, 2))) AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-006: DOUBLE operand
SELECT t.k, r.k AS matched_k
FROM VALUES (CAST(10.0 AS DOUBLE)) AS t(k) ASOF JOIN
     VALUES (CAST(5.0 AS DOUBLE)), (CAST(7.0 AS DOUBLE)) AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-007: STRING operand (UTF8_BINARY ordering)
SELECT t.k, r.k AS matched_k
FROM VALUES ('c') AS t(k) ASOF JOIN VALUES ('a'), ('b') AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-008: BINARY operand
SELECT hex(t.k) AS left_k, hex(r.k) AS matched_k
FROM VALUES (X'0F') AS t(k) ASOF JOIN VALUES (X'0A'), (X'0C') AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-009: INTERVAL YEAR TO MONTH operand
SELECT t.iv, r.iv AS matched_iv
FROM VALUES (INTERVAL '2-0' YEAR TO MONTH) AS t(iv) ASOF JOIN
     VALUES (INTERVAL '1-0' YEAR TO MONTH), (INTERVAL '1-6' YEAR TO MONTH) AS r(iv)
  MATCH_CONDITION (t.iv >= r.iv);

-- FVT-ASOF-4-010: INTERVAL DAY TO SECOND operand
SELECT t.iv, r.iv AS matched_iv
FROM VALUES (INTERVAL '2' DAY) AS t(iv) ASOF JOIN
     VALUES (INTERVAL '1' DAY), (INTERVAL '36' HOUR) AS r(iv)
  MATCH_CONDITION (t.iv >= r.iv);

-- FVT-ASOF-4-011: STRUCT tuple MATCH_CONDITION
SELECT r.req_ts, r.seq, d.version
FROM requests r ASOF JOIN deploys d
  MATCH_CONDITION ((r.req_ts, r.seq) >= (d.deploy_ts, d.seq))
  ON r.service = d.service
ORDER BY r.req_ts, r.seq;

-- FVT-ASOF-4-012: nested whole STRUCT column MATCH_CONDITION
SELECT r.k.tag, r.k.inner_seq
FROM VALUES (named_struct(
  'inner_ts', TIMESTAMP '2026-06-29 10:00:00',
  'inner_seq', 2,
  'tag', 'a')) AS t(k)
ASOF JOIN (
  SELECT * FROM VALUES
    (named_struct(
      'inner_ts', TIMESTAMP '2026-06-29 09:00:00',
      'inner_seq', 1,
      'tag', 'a')),
    (named_struct(
      'inner_ts', TIMESTAMP '2026-06-29 10:00:00',
      'inner_seq', 1,
      'tag', 'a')) AS r(k)
) r
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-013: coercion TINYINT vs BIGINT
SELECT t.k, r.k AS matched_k
FROM VALUES (CAST(10 AS TINYINT)) AS t(k) ASOF JOIN VALUES (CAST(5 AS BIGINT)) AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-014: coercion INT vs DOUBLE
SELECT t.k, r.k AS matched_k
FROM VALUES (10) AS t(k) ASOF JOIN VALUES (CAST(5.5 AS DOUBLE)) AS r(k)
  MATCH_CONDITION (t.k >= r.k);

-- FVT-ASOF-4-015: coercion DATE vs TIMESTAMP
SELECT t.d, r.ts AS matched_ts
FROM VALUES (DATE '2026-06-29') AS t(d) ASOF JOIN
     VALUES (TIMESTAMP '2026-06-29 10:00:00'), (TIMESTAMP '2026-06-28 08:00:00') AS r(ts)
  MATCH_CONDITION (t.d >= r.ts);

-- FVT-ASOF-4-016: coercion TIMESTAMP vs TIMESTAMP_NTZ
SELECT t.ts, r.ts_ntz AS matched_ts_ntz
FROM VALUES (TIMESTAMP '2026-06-29 10:00:00') AS t(ts) ASOF JOIN
     VALUES (TIMESTAMP_NTZ '2026-06-29 09:00:00') AS r(ts_ntz)
  MATCH_CONDITION (t.ts >= r.ts_ntz);

-- FVT-ASOF-4-017: ARRAY<INT> operand
SELECT t.a, r.a AS matched_a
FROM VALUES (ARRAY(1, 3)) AS t(a) ASOF JOIN
     VALUES (ARRAY(1, 2)), (ARRAY(1, 4)) AS r(a)
  MATCH_CONDITION (t.a >= r.a);

-- FVT-ASOF-4-018: ARRAY<STRUCT> whole column MATCH_CONDITION
SELECT r.a
FROM VALUES (ARRAY(named_struct('seq', 1, 'val', 3))) AS t(a)
ASOF JOIN (
  SELECT * FROM VALUES
    (ARRAY(named_struct('seq', 1, 'val', 2))),
    (ARRAY(named_struct('seq', 1, 'val', 4))) AS r(a)
) r
  MATCH_CONDITION (t.a >= r.a);

-- FVT-ASOF-4-018a: scalar STRUCT tuple MATCH_CONDITION
SELECT r.c1, r.c2
FROM VALUES (1, 2) AS t(c1, c2) ASOF JOIN VALUES (1, 1) AS r(c1, c2)
  MATCH_CONDITION ((t.c1, t.c2) >= (r.c1, r.c2));

-- FVT-ASOF-4-019: MAP operand rejection — covered by FVT-ASOF-3-010

-- FVT-ASOF-4-020: ARRAY<MAP> rejection — covered by FVT-ASOF-3-011
