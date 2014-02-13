EXPLAIn
SELECT a, b FROM (
  SELECT key a, value b
  FROM src
) src1
ORDER BY a LIMIT 1;

SELECT a, b FROM (
  SELECT key a, value b
  FROM src
) src1
ORDER BY a LIMIT 1;
