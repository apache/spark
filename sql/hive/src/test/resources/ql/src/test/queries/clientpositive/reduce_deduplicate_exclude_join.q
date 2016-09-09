set hive.optimize.reducededuplication=true;
set hive.auto.convert.join=true;
explain select * from (select * from src cluster by key) a join src b on a.key = b.key limit 1;
