-- Test cases with collations
SELECT listagg(c1) WITHIN GROUP (ORDER BY c1 COLLATE utf8_binary) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
SELECT listagg(c1) WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
SELECT listagg(DISTINCT c1 COLLATE utf8_binary) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
SELECT listagg(DISTINCT c1 COLLATE utf8_lcase) FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1);
SELECT listagg(DISTINCT c1 COLLATE utf8_lcase) WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase) FROM (VALUES ('a'), ('B'), ('b'), ('A')) AS t(c1);
SELECT listagg(DISTINCT c1 COLLATE unicode_rtrim) FROM (VALUES ('abc  '), ('abc '), ('x'), ('abc')) AS t(c1);
SELECT listagg(c1) WITHIN GROUP (ORDER BY c1) FROM (VALUES ('abc  '), ('abc '), ('abc\n'), ('abc'), ('x')) AS t(c1);
SELECT listagg(c1) WITHIN GROUP (ORDER BY c1 COLLATE unicode_rtrim) FROM (VALUES ('abc  '), ('abc '), ('abc\n'), ('abc'), ('x')) AS t(c1);

-- Error case with collations
SELECT listagg(DISTINCT c1 COLLATE utf8_lcase) WITHIN GROUP (ORDER BY c1 COLLATE utf8_binary) FROM (VALUES ('a'), ('b'), ('A'), ('B')) AS t(c1);