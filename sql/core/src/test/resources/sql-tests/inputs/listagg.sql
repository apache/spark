-- Create temporary views
CREATE TEMP VIEW df AS
SELECT * FROM (VALUES ('a', 'b'), ('a', 'c'), ('b', 'c'), ('b', 'd'), (NULL, NULL)) AS t(a, b);

CREATE TEMP VIEW df2 AS
SELECT * FROM (VALUES (1, true), (2, false), (3, false)) AS t(a, b);

-- Test cases for listagg function
SELECT listagg(b) FROM df GROUP BY a;
SELECT string_agg(b) FROM df GROUP BY a;
SELECT listagg(b, NULL) FROM df GROUP BY a;
SELECT listagg(b) FROM df WHERE 1 != 1;
SELECT listagg(b, '|') FROM df GROUP BY a;
SELECT listagg(a) FROM df;
SELECT listagg(DISTINCT a) FROM df;
SELECT listagg(a) WITHIN GROUP (ORDER BY a) FROM df;
SELECT listagg(a) WITHIN GROUP (ORDER BY a DESC) FROM df;
SELECT listagg(a) WITHIN GROUP (ORDER BY a DESC) OVER (PARTITION BY b) FROM df;
SELECT listagg(a) WITHIN GROUP (ORDER BY b) FROM df;
SELECT listagg(a) WITHIN GROUP (ORDER BY b DESC) FROM df;
SELECT listagg(a, '|') WITHIN GROUP (ORDER BY b DESC) FROM df;
SELECT listagg(a) WITHIN GROUP (ORDER BY b DESC, a ASC) FROM df;
SELECT listagg(a) WITHIN GROUP (ORDER BY b DESC, a DESC) FROM df;
SELECT listagg(c1) FROM (VALUES (X'DEAD'), (X'BEEF')) AS t(c1);
SELECT listagg(c1, NULL) FROM (VALUES (X'DEAD'), (X'BEEF')) AS t(c1);
SELECT listagg(c1, X'42') FROM (VALUES (X'DEAD'), (X'BEEF')) AS t(c1);
SELECT listagg(a), listagg(b, ',') FROM df2;

-- Error cases
SELECT listagg(c1) FROM (VALUES (ARRAY('a', 'b'))) AS t(c1);
SELECT listagg(c1, ', ') FROM (VALUES (X'DEAD'), (X'BEEF')) AS t(c1);
SELECT listagg(b, a) FROM df GROUP BY a;
SELECT listagg(a) OVER (ORDER BY a) FROM df;
SELECT listagg(a) WITHIN GROUP (ORDER BY a) OVER (ORDER BY a) FROM df;
SELECT string_agg(a) WITHIN GROUP (ORDER BY a) OVER (ORDER BY a) FROM df;
SELECT listagg(DISTINCT a) OVER (ORDER BY a) FROM df;
SELECT listagg(DISTINCT a) WITHIN GROUP (ORDER BY b) FROM df;
SELECT listagg(DISTINCT a) WITHIN GROUP (ORDER BY a, b) FROM df;