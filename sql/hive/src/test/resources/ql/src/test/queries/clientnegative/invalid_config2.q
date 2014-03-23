set hive.mapred.supports.subdirectories=false;
set hive.optimize.union.remove=true;

select count(1) from src;
