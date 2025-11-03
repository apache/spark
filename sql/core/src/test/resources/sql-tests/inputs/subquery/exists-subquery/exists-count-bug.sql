--ONLY_IF spark
create temporary view t1(c1, c2) as values (0, 1), (1, 2);
create temporary view t2(c1, c2) as values (0, 2), (0, 3);
create temporary view t3(c1, c2) as values (0, 3), (1, 4), (2, 5);

select * from t1 where exists (select count(*) from t2 where t2.c1 = t1.c1);

select * from t1 where not exists (select count(*) from t2 where t2.c1 = t1.c1);

select *, exists (select count(*) from t2 where t2.c1 = t1.c1) from t1;

select *, not exists (select count(*) from t2 where t2.c1 = t1.c1) from t1;

select * from t1 where
 exists(select count(*) + 1 from t2 where t2.c1 = t1.c1) OR
 not exists (select count(*) - 1 from t2 where t2.c1 = t1.c1);


select * from t1 where
 (exists(select count(*) + 1 from t2 where t2.c1 = t1.c1) OR
 not exists(select count(*) - 1 from t2 where t2.c1 = t1.c1)) AND
 exists(select count(*) from t2 where t2.c1 = t1.c2);

select * from t1 where exists (select count(*) from t2 where t1.c1 = 100);


-- With legacy behavior flag set, some answers are not correct.
set spark.sql.optimizer.decorrelateExistsSubqueryLegacyIncorrectCountHandling.enabled = true;
select * from t1 where exists (select count(*) from t2 where t2.c1 = t1.c1);

select * from t1 where not exists (select count(*) from t2 where t2.c1 = t1.c1);

select *, exists (select count(*) from t2 where t2.c1 = t1.c1) from t1;

select *, not exists (select count(*) from t2 where t2.c1 = t1.c1) from t1;

select * from t1 where
 exists(select count(*) + 1 from t2 where t2.c1 = t1.c1) OR
 not exists (select count(*) - 1 from t2 where t2.c1 = t1.c1);

select * from t1 where exists (select count(*) from t2 where t1.c1 = 100);

set spark.sql.optimizer.decorrelateExistsSubqueryLegacyIncorrectCountHandling.enabled = false;
