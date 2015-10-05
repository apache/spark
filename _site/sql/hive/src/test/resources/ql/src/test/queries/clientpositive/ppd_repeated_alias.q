drop table pokes;
drop table pokes2;
create table pokes (foo int, bar int, blah int);
create table pokes2 (foo int, bar int, blah int);

-- Q1: predicate should not be pushed on the right side of a left outer join
explain
SELECT a.foo as foo1, b.foo as foo2, b.bar
FROM pokes a LEFT OUTER JOIN pokes2 b
ON a.foo=b.foo
WHERE b.bar=3;

-- Q2: predicate should not be pushed on the right side of a left outer join
explain
SELECT * FROM
    (SELECT a.foo as foo1, b.foo as foo2, b.bar
    FROM pokes a LEFT OUTER JOIN pokes2 b
    ON a.foo=b.foo) a
WHERE a.bar=3;

-- Q3: predicate should be pushed
explain
SELECT * FROM
    (SELECT a.foo as foo1, b.foo as foo2, a.bar
    FROM pokes a JOIN pokes2 b
    ON a.foo=b.foo) a
WHERE a.bar=3;

-- Q4: here, the filter c.bar should be created under the first join but above the second
explain select c.foo, d.bar from (select c.foo, b.bar, c.blah from pokes c left outer join pokes b on c.foo=b.foo) c left outer join pokes d where d.foo=1 and c.bar=2;

drop table pokes;
drop table pokes2;
