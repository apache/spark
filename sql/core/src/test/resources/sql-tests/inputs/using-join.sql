create temporary view ut1 as select * from values
  ("r1c1", "r1c2", "t1r1c3"),
  ("r2c1", "r2c2", "t1r2c3"),
  ("r3c1x", "r3c2", "t1r3c3")
  as ut1(c1, c2, c3);

create temporary view ut2 as select * from values
  ("r1c1", "r1c2", "t2r1c3"),
  ("r2c1", "r2c2", "t2r2c3"),
  ("r3c1y", "r3c2", "t2r3c3")
  as ut2(c1, c2, c3);

create temporary view ut3 as select * from values
  (CAST(null as String), "r1c2", "t3r1c3"),
  ("r2c1", "r2c2", "t3r2c3"),
  ("r3c1y", "r3c2", "t3r3c3")
  as ut3(c1, c2, c3);

-- inner join with one using column
SELECT * FROM ut1 join ut2 using (c1);

-- inner join with two using columns
SELECT * FROM ut1 join ut2 using (c1, c2);

-- Left outer join with one using column.
SELECT * FROM ut1 left join ut2 using (c1);

-- Right outer join with one using column.
SELECT * FROM ut1 right join ut2 using (c1);

-- Full outer join with one using column.
SELECT * FROM ut1 full outer join ut2 using (c1);

-- Full outer join with null value in join column.
SELECT * FROM ut1 full outer join ut3 using (c1);

-- Self join with using columns.
SELECT * FROM ut1 join ut1 using (c1);

-- clean up the temporary tables
drop view ut1;
drop view ut2;
drop view ut3;