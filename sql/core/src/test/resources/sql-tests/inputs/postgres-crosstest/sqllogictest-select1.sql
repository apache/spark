-- First 6 queries of the sql-logic-tests with some minor changes for compatibility, available here:
-- https://www.sqlite.org/sqllogictest/file?name=test/select1.test&ci=tip.

CREATE VIEW t1(a, b, c, d, e) AS VALUES
    (103,102,100,101,104),
    (107,106,108,109,105),
    (110,114,112,111,113),
    (116,119,117,115,118),
    (123,122,124,120,121),
    (127,128,129,126,125),
    (132,134,131,133,130),
    (138,136,139,135,137),
    (144,141,140,142,143),
    (145,149,146,148,147),
    (151,150,153,154,152),
    (155,157,159,156,158),
    (161,160,163,164,162),
    (167,169,168,165,166),
    (171,170,172,173,174),
    (177,176,179,178,175),
    (181,180,182,183,184),
    (187,188,186,189,185),
    (190,194,193,192,191),
    (199,197,198,196,195),
    (200,202,203,201,204),
    (208,209,205,206,207),
    (214,210,213,212,211),
    (218,215,216,217,219),
    (223,221,222,220,224),
    (226,227,228,229,225),
    (234,231,232,230,233),
    (237,236,239,235,238),
    (242,244,240,243,241),
    (246,248,247,249,245);

SELECT CASE WHEN c > (SELECT avg(c) FROM t1) THEN a * 2 ELSE b * 10 END
  FROM t1
 ORDER BY 1;

SELECT a + b * 2 + c * 3 + d * 4 + e * 5,
       CAST((a + b + c + d + e) / 5 AS INT)
  FROM t1
 ORDER BY 1,2;

SELECT a + b * 2 + c * 3 + d * 4 + e * 5,
       CASE WHEN a < b - 3 THEN 111 WHEN a <= b THEN 222
        WHEN a < b + 3 THEN 333 ELSE 444 END,
       abs(b - c),
       CAST((a + b + c + d + e) / 5 AS INT),
       a + b * 2 + c * 3
  FROM t1
 WHERE (e > c OR e < d)
   AND d>e
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b < t1.b)
 ORDER BY 4,2,1,3,5;

SELECT c,
       d-e,
       CASE a + 1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a + b * 2 + c * 3 + d * 4,
       e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR c BETWEEN b - 2 AND d + 2
    OR (e > c OR e < d)
 ORDER BY 1,5,3,2,4;

SELECT a + b * 2 + c * 3 + d * 4,
       CAST((a + b + c + d + e) / 5 AS INT),
       abs(a),
       e,
       CASE a + 1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d
  FROM t1
 WHERE b > c
   AND c > d
 ORDER BY 3,4,5,1,2,6;

SELECT a + b * 2 + c * 3 + d * 4,
       CASE a + 1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c > t1.c AND x.d < t1.d),
       c
  FROM t1
 WHERE (c <= d - 2 OR c >= d + 2)
 ORDER BY 4,2,1,3;

DROP VIEW IF EXISTS t1;
