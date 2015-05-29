-- union case: all subqueries are a map-only jobs, 3 way union, same input for all sub-queries, followed by reducesink

explain 
  select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION  ALL  
                        select s2.key as key, s2.value as value from src s2 UNION ALL
                        select s3.key as key, s3.value as value from src s3) unionsrc;

  select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION  ALL  
                        select s2.key as key, s2.value as value from src s2 UNION ALL
                        select s3.key as key, s3.value as value from src s3) unionsrc;
