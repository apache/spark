set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=10000000;
explain select src.key from src where src.key in ( select distinct key from src);

set hive.auto.convert.join=false;
