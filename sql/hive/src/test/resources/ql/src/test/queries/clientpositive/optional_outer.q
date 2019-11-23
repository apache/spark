EXPLAIN SELECT * FROM src a LEFT OUTER JOIN src b on (a.key=b.key);
EXPLAIN SELECT * FROM src a LEFT JOIN src b on (a.key=b.key);

EXPLAIN SELECT * FROM src a RIGHT OUTER JOIN src b on (a.key=b.key);
EXPLAIN SELECT * FROM src a RIGHT JOIN src b on (a.key=b.key);

EXPLAIN SELECT * FROM src a FULL OUTER JOIN src b on (a.key=b.key);
EXPLAIN SELECT * FROM src a FULL JOIN src b on (a.key=b.key);
