create temporary view nt1 as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3)
  as nt1(k, v1);

create temporary view nt2 as select * from values
  ("one", 1),
  ("two", 22),
  ("one", 5),
  ("four", 4)
  as nt2(k, v2);

SELECT * FROM nt1 left outer join nt2 using (k);

SELECT k FROM nt1 left outer join nt2 using (k);

SELECT nt1.*, nt2.* FROM nt1 left outer join nt2 using (k);

SELECT nt1.k, nt2.k FROM nt1 left outer join nt2 using (k);

SELECT nt1.k, nt2.k FROM nt1 left outer join nt2 using (k) ORDER BY nt2.k;

SELECT k, nt1.k FROM nt1 left outer join nt2 using (k);

SELECT k, nt2.k FROM nt1 left outer join nt2 using (k);

SELECT * FROM nt1 left semi join nt2 using (k);

SELECT k FROM nt1 left semi join nt2 using (k);

SELECT nt1.* FROM nt1 left semi join nt2 using (k);

SELECT nt1.k FROM nt1 left semi join nt2 using (k);

SELECT k, nt1.k FROM nt1 left semi join nt2 using (k);

SELECT * FROM nt1 right outer join nt2 using (k);

SELECT k FROM nt1 right outer join nt2 using (k);

SELECT nt1.*, nt2.* FROM nt1 right outer join nt2 using (k);

SELECT nt1.k, nt2.k FROM nt1 right outer join nt2 using (k);

SELECT k, nt1.k FROM nt1 right outer join nt2 using (k);

SELECT k, nt2.k FROM nt1 right outer join nt2 using (k);

SELECT * FROM nt1 full outer join nt2 using (k);

SELECT k FROM nt1 full outer join nt2 using (k);

SELECT nt1.*, nt2.* FROM nt1 full outer join nt2 using (k);

SELECT nt1.k, nt2.k FROM nt1 full outer join nt2 using (k);

SELECT k, nt1.k FROM nt1 full outer join nt2 using (k);

SELECT k, nt2.k FROM nt1 full outer join nt2 using (k);

SELECT * FROM nt1 full outer join nt2 using (k);

SELECT k FROM nt1 inner join nt2 using (k);

SELECT nt1.*, nt2.* FROM nt1 inner join nt2 using (k);

SELECT nt1.k, nt2.k FROM nt1 inner join nt2 using (k);

SELECT k, nt1.k FROM nt1 inner join nt2 using (k);

SELECT k, nt2.k FROM nt1 inner join nt2 using (k);
