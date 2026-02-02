--ONLY_IF spark
create temporary view t1(c1, c2) as values (0, 1), (1, 2);
create temporary view t2(c1, c2) as values (0, 2), (0, 3);
create temporary view t3(c1, c2) as values (0, 3), (1, 4), (2, 5);

select * from t1 where c1 in (select count(*) + 1 from t2 where t2.c1 = t1.c1);

select *, c1 in (select count(*) + 1 from t2 where t2.c1 = t1.c1)
from t1;

select *, c1 not in (select count(*) + 1 from t2 where t2.c1 = t1.c1)
from t1;

select * from t1 where
 c1 in (select count(*) + 1 from t2 where t2.c1 = t1.c1) OR
 c2 in (select count(*) - 1 from t2 where t2.c1 = t1.c1);

select * from t1 where
 (c1 in (select count(*) + 1 from t2 where t2.c1 = t1.c1) OR
 c2 in (select count(*) - 1 from t2 where t2.c1 = t1.c1)) AND
 c1 NOT in (select count(*) from t2 where t2.c1 = t1.c2);

select * from t1 where c1 in (select 1 from t2 where t2.c1 = t1.c1 having count(*) = 0);

select * from t1 where c1 not in (select 1 from t2 where t2.c1 = t1.c1 having count(*) = 0);


select * from t1 where c1 in (select count(*) from t1 join t3 using (c1) where t3.c1 = t1.c2);

select * from t1 where c1 not in (select count(*) + 1 from t1 join t3 using (c1) where t3.c1 = t1.c2);


-- With legacy behavior flag set, some answers are not correct.
set spark.sql.optimizer.decorrelateExistsSubqueryLegacyIncorrectCountHandling.enabled = true;
select * from t1 where c1 in (select count(*) + 1 from t2 where t2.c1 = t1.c1);

select *, c1 in (select count(*) + 1 from t2 where t2.c1 = t1.c1)
from t1;

select *, c1 not in (select count(*) + 1 from t2 where t2.c1 = t1.c1)
from t1;

select * from t1 where c1 in (select 1 from t2 where t2.c1 = t1.c1 having count(*) = 0);

select * from t1 where c1 not in (select 1 from t2 where t2.c1 = t1.c1 having count(*) = 0);


select * from t1 where c1 in (select count(*) from t1 join t3 using (c1) where t3.c1 = t1.c2);

select * from t1 where c1 not in (select count(*) + 1 from t1 join t3 using (c1) where t3.c1 = t1.c2);
set spark.sql.optimizer.decorrelateExistsSubqueryLegacyIncorrectCountHandling.enabled = false;
