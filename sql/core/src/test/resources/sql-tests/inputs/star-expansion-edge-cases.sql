-- Tests for edge cases in star expansion

-- named_struct does not wrap group star expr in struct.
-- There is such logic in analyzer code, but it seems, it's unreachable
CREATE TEMPORARY VIEW t2_star(col1 INT, col2 STRING) AS VALUES (1, 'a'), (2, 'b');
CREATE TEMPORARY VIEW t3_star(col1 INT) AS VALUES (1), (2);

SELECT named_struct(*) AS a FROM t2_star;
SELECT named_struct('key', *) AS a FROM t3_star;
SELECT named_struct('key', *) AS a FROM t2_star;
SELECT named_struct('key', struct(*)) AS a FROM t2_star;

-- Star should not be resolved in unexpected places such as join condition or having clause
SELECT * FROM t2_star AS l INNER JOIN t2_star as r ON l.col1 in (r.*);
SELECT * FROM t2_star AS l INNER JOIN t2_star as r ON struct(l.col1, l.col2) in (struct(r.*));
SELECT named_struct('a',1) AS s GROUP BY 1 HAVING s.* > 1;
SELECT named_struct('a',1) AS s GROUP BY 1 HAVING 1 in (s.*);

-- in with multiple stars
CREATE TEMPORARY VIEW t7_star(col1 INT, col2 INT, col3 INT) AS VALUES (1, 2, 3), (4, 5, 6);
SELECT * FROM t2_star WHERE col1 IN (*, *);
SELECT * FROM t7_star WHERE col1 in (* EXCEPT (col1, col2), * EXCEPT (col1, col3));

-- star can be resolved to an empty sequence
SELECT 42, col1.* FROM (SELECT struct() as col1);

-- assert for count(table.*) is case sensitive
SELECT count(my_table.*), count(*) FROM VALUES(1, 2, NULL) my_table(a, b, c);
SELECT COUNT(my_table.*), count(*) FROM VALUES(1, 2, NULL) my_table(a, b, c);

-- Cleanup
DROP VIEW t2_star;
DROP VIEW t3_star;
DROP VIEW t7_star;
