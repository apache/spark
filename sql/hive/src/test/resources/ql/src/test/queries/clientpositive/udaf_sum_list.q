-- HIVE-5279
-- GenericUDAFSumList has Converter which does not have default constructor
-- After
create temporary function sum_list as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSumList';

select sum_list(array(key, key)) from src;
