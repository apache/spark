set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- Since the inputs are small, it should be automatically converted to mapjoin

explain select srcpart.key from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key);

explain select count(*) from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key) group by ds;

select count(*) from srcpart join src src on (srcpart.value=src.value) join src src1 on (srcpart.key=src1.key) group by ds;
