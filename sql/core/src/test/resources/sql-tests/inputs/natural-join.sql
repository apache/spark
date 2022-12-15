create temporary view nt1 as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3)
  as nt1(k, v1);

create temporary view nt2 as select * from values
  ("one", 1),
  ("two", 22),
  ("one", 5)
  as nt2(k, v2);

create temporary view nt3 as select * from values
  ("one", 4),
  ("two", 5),
  ("one", 6)
  as nt3(k, v3);

create temporary view nt4 as select * from values
  ("one", 7),
  ("two", 8),
  ("one", 9)
  as nt4(k, v4);

SELECT * FROM nt1 natural join nt2;

SELECT * FROM nt1 natural join nt2 where k = "one";

SELECT * FROM nt1 natural left join nt2 order by v1, v2;

SELECT * FROM nt1 natural right join nt2 order by v1, v2;

SELECT count(*) FROM nt1 natural full outer join nt2;

SELECT k FROM nt1 natural join nt2;

SELECT k FROM nt1 natural join nt2 where k = "one";

SELECT nt1.* FROM nt1 natural join nt2;

SELECT nt2.* FROM nt1 natural join nt2;

SELECT sbq.* from (SELECT * FROM nt1 natural join nt2) sbq;

SELECT sbq.k from (SELECT * FROM nt1 natural join nt2) sbq;

SELECT nt1.*, nt2.* FROM nt1 natural join nt2;

SELECT *, nt2.k FROM nt1 natural join nt2;

SELECT nt1.k, nt2.k FROM nt1 natural join nt2;

SELECT k FROM (SELECT nt2.k FROM nt1 natural join nt2);

SELECT nt1.k, nt2.k FROM nt1 natural join nt2 where k = "one";

SELECT * FROM (SELECT * FROM nt1 natural join nt2);

SELECT * FROM (SELECT nt1.*, nt2.* FROM nt1 natural join nt2);

SELECT * FROM (SELECT nt1.v1, nt2.k FROM nt1 natural join nt2);

SELECT nt2.k FROM (SELECT * FROM nt1 natural join nt2);

SELECT * FROM nt1 natural join nt2 natural join nt3;

SELECT nt1.*, nt2.*, nt3.* FROM nt1 natural join nt2 natural join nt3;

SELECT nt1.*, nt2.*, nt3.* FROM nt1 natural join nt2 join nt3 on nt2.k = nt3.k;

SELECT * FROM nt1 natural join nt2 join nt3 on nt1.k = nt3.k;

SELECT * FROM nt1 natural join nt2 join nt3 on nt2.k = nt3.k;

SELECT nt1.*, nt2.*, nt3.*, nt4.* FROM nt1 natural join nt2 natural join nt3 natural join nt4;
