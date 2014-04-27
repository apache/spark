EXPLAIN
CREATE TEMPORARY FUNCTION test_udf_get_java_string AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaString';

CREATE TEMPORARY FUNCTION test_udf_get_java_string AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaString';

select * from src where value = test_udf_get_java_string("val_66"); 
select * from (select * from src where value = 'val_66' or value = 'val_8') t where value <> test_udf_get_java_string("val_8"); 


DROP TEMPORARY FUNCTION test_udf_get_java_boolean;
