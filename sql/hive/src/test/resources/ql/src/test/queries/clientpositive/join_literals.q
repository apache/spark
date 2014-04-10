-- Test Joins with a variety of literals in the on clause

SELECT COUNT(*) FROM src a JOIN src b ON a.key = b.key AND a.key = 0L;

SELECT COUNT(*) FROM src a JOIN src b ON a.key = b.key AND a.key = 0S;

SELECT COUNT(*) FROM src a JOIN src b ON a.key = b.key AND a.key = 0Y;

SELECT COUNT(*) FROM src a JOIN src b ON a.key = b.key AND a.key = 0BD;
