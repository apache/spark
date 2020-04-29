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


SELECT * FROM nt1 natural join nt2 where k = "one";

SELECT * FROM nt1 natural left join nt2 order by v1, v2;

SELECT * FROM nt1 natural right join nt2 order by v1, v2;

SELECT count(*) FROM nt1 natural full outer join nt2;
