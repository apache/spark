-- Test cases with utf8_binary
SELECT listagg(c1) WITHIN GROUP (ORDER BY c1 COLLATE utf8_binary) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
WITH t(c1) AS (SELECT listagg(DISTINCT col1 COLLATE utf8_binary) FROM (VALUES ('a'), ('A'), ('b'), ('B'))) SELECT len(c1), regexp_count(c1, 'a'), regexp_count(c1, 'b'), regexp_count(c1, 'A'), regexp_count(c1, 'B') FROM t;
WITH t(c1) AS (SELECT listagg(col1) WITHIN GROUP (ORDER BY col1) FROM (VALUES ('abc  '), ('abc '), ('abc\n'), ('abc'), ('x'))) SELECT replace(replace(c1, ' ', ''), '\n', '$') FROM t;
-- Test cases with utf8_lcase. Lower expression added for determinism
SELECT lower(listagg(c1) WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase)) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
WITH t(c1) AS (SELECT lower(listagg(DISTINCT col1 COLLATE utf8_lcase)) FROM (VALUES ('a'), ('A'), ('b'), ('B'))) SELECT len(c1), regexp_count(c1, 'a'), regexp_count(c1, 'b') FROM t;
SELECT lower(listagg(DISTINCT c1 COLLATE utf8_lcase) WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase)) FROM (VALUES ('a'), ('B'), ('b'), ('A')) AS t(c1);
-- Test cases with unicode_rtrim.
WITH t(c1) AS (SELECT replace(listagg(DISTINCT col1 COLLATE unicode_rtrim) COLLATE utf8_binary, ' ', '') FROM (VALUES ('xbc  '), ('xbc '), ('a'), ('xbc'))) SELECT len(c1), regexp_count(c1, 'a'), regexp_count(c1, 'xbc') FROM t;
WITH t(c1) AS (SELECT listagg(col1) WITHIN GROUP (ORDER BY col1 COLLATE unicode_rtrim) FROM (VALUES ('abc '), ('abc\n'), ('abc'), ('x'))) SELECT replace(replace(c1, ' ', ''), '\n', '$') FROM t;

-- Error case with collations
SELECT listagg(DISTINCT c1 COLLATE utf8_lcase) WITHIN GROUP (ORDER BY c1 COLLATE utf8_binary) FROM (VALUES ('a'), ('b'), ('A'), ('B')) AS t(c1);

-- LISTAGG DISTINCT cast safety with collations:
-- string -> string (safe): explicit cast to same collation
SELECT listagg(DISTINCT CAST(col AS STRING)) WITHIN GROUP (ORDER BY col) FROM VALUES ('ABC'), ('abc'), ('ABC') AS t(col);
-- string -> string (unsafe): cast to non-binary-equality collation on target
SELECT listagg(DISTINCT CAST(col AS STRING COLLATE UTF8_LCASE)) WITHIN GROUP (ORDER BY col) FROM VALUES ('ABC'), ('abc'), ('ABC') AS t(col);

-- binary -> string (safe): cast to default STRING (UTF8_BINARY)
SELECT listagg(DISTINCT CAST(col AS STRING)) WITHIN GROUP (ORDER BY col) FROM VALUES (X'414243'), (X'616263'), (X'414243') AS t(col);  -- ABC, abc, ABC
-- binary -> string (unsafe): cast to non-binary-equality collation on target
SELECT listagg(DISTINCT CAST(col AS STRING COLLATE UTF8_LCASE)) WITHIN GROUP (ORDER BY col) FROM VALUES (X'414243'), (X'616263'), (X'414243') AS t(col);  -- ABC, abc, ABC

-- string -> binary (safe): UTF8_BINARY source, BinaryType target
SELECT listagg(DISTINCT CAST(col AS BINARY)) WITHIN GROUP (ORDER BY col) FROM VALUES ('ABC'), ('abc'), ('ABC') AS t(col);
-- string -> binary (unsafe): non-binary-equality source (UTF8_LCASE), BinaryType target
SELECT listagg(DISTINCT CAST(col AS BINARY)) WITHIN GROUP (ORDER BY col) FROM (SELECT col COLLATE UTF8_LCASE AS col FROM VALUES ('ABC'), ('abc'), ('ABC') AS t(col))
