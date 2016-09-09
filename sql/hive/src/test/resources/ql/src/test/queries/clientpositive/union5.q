set hive.map.aggr = true;

-- union case: both subqueries are map-reduce jobs on same input, followed by reduce sink

explain 
  select unionsrc.key, count(1) FROM (select 'tst1' as key, count(1) as value from src s1
                                    UNION  ALL  
                                      select 'tst2' as key, count(1) as value from src s2) unionsrc group by unionsrc.key;

select unionsrc.key, count(1) FROM (select 'tst1' as key, count(1) as value from src s1
                                  UNION  ALL  
                                    select 'tst2' as key, count(1) as value from src s2) unionsrc group by unionsrc.key;
