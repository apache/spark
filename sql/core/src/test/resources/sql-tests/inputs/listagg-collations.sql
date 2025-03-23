-- Test cases with utf8_binary
SELECT listagg(c1) WITHIN GROUP (ORDER BY c1 COLLATE utf8_binary) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
SELECT listagg(DISTINCT c1 COLLATE utf8_binary) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
WITH t(c1) AS (SELECT listagg(col1) WITHIN GROUP (ORDER BY col1) FROM (VALUES ('abc  '), ('abc '), ('abc\n'), ('abc'), ('x'))) SELECT replace(replace(c1, ' ', ''), '\n', '$') FROM t;
-- Test cases with utf8_lcase. Lower expression added for determinism
SELECT lower(listagg(c1) WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase)) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
SELECT lower(listagg(DISTINCT c1 COLLATE utf8_lcase)) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
SELECT lower(listagg(DISTINCT c1 COLLATE utf8_lcase) WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase)) FROM (VALUES ('a'), ('B'), ('b'), ('A')) AS t(c1);
-- Test cases with unicode_rtrim.
SELECT rtrim(listagg(DISTINCT c1 COLLATE unicode_rtrim)) FROM (VALUES ('xbc  '), ('xbc '), ('a'), ('xbc')) AS t(c1);
WITH t(c1) AS (SELECT listagg(col1) WITHIN GROUP (ORDER BY col1 COLLATE unicode_rtrim) FROM (VALUES ('abc '), ('abc\n'), ('abc'), ('x'))) SELECT replace(replace(c1, ' ', ''), '\n', '$') FROM t;

-- Error case with collations
SELECT listagg(DISTINCT c1 COLLATE utf8_lcase) WITHIN GROUP (ORDER BY c1 COLLATE utf8_binary) FROM (VALUES ('a'), ('b'), ('A'), ('B')) AS t(c1);
