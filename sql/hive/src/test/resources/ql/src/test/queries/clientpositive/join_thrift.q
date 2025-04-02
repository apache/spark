DESCRIBE src_thrift;

EXPLAIN
SELECT s1.aint, s2.lintstring
FROM src_thrift s1
JOIN src_thrift s2
ON s1.aint = s2.aint;

SELECT s1.aint, s2.lintstring
FROM src_thrift s1
JOIN src_thrift s2
ON s1.aint = s2.aint;
