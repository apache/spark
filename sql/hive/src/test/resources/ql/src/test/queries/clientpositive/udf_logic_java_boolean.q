EXPLAIN
CREATE TEMPORARY FUNCTION test_udf_get_java_boolean AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaBoolean';

CREATE TEMPORARY FUNCTION test_udf_get_java_boolean AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaBoolean';

select 1 from src where test_udf_get_java_boolean("false") and True limit 1; 
select 1 from src where test_udf_get_java_boolean("true") and True limit 1; 
select 1 from src where True and test_udf_get_java_boolean("false") limit 1; 
select 1 from src where False and test_udf_get_java_boolean("false") limit 1; 
select 1 from src where test_udf_get_java_boolean("true") and test_udf_get_java_boolean("true") limit 1; 
select 1 from src where test_udf_get_java_boolean("true") and test_udf_get_java_boolean("false") limit 1;
select 1 from src where test_udf_get_java_boolean("false") and test_udf_get_java_boolean("true") limit 1; 
select 1 from src where test_udf_get_java_boolean("false") and test_udf_get_java_boolean("false") limit 1; 

select 1 from src where test_udf_get_java_boolean("false") or True limit 1; 
select 1 from src where test_udf_get_java_boolean("true") or True limit 1; 
select 1 from src where True or test_udf_get_java_boolean("false") limit 1; 
select 1 from src where False or test_udf_get_java_boolean("false") limit 1; 
select 1 from src where test_udf_get_java_boolean("true") or test_udf_get_java_boolean("true") limit 1; 
select 1 from src where test_udf_get_java_boolean("true") or test_udf_get_java_boolean("false") limit 1;
select 1 from src where test_udf_get_java_boolean("false") or test_udf_get_java_boolean("true") limit 1; 
select 1 from src where test_udf_get_java_boolean("false") or test_udf_get_java_boolean("false") limit 1; 

select 1 from src where not(test_udf_get_java_boolean("false")) limit 1; 
select 1 from src where not(test_udf_get_java_boolean("true")) limit 1; 


DROP TEMPORARY FUNCTION test_udf_get_java_boolean;
