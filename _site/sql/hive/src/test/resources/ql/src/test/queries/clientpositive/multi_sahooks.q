set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.metadata.DummySemanticAnalyzerHook1;

drop table tbl_sahook;
create table tbl_sahook (c string);
desc extended tbl_sahook;
drop table tbl_sahook;

set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.metadata.DummySemanticAnalyzerHook1,org.apache.hadoop.hive.ql.metadata.DummySemanticAnalyzerHook;

drop table tbl_sahooks;
create table tbl_sahooks (c string);
desc extended tbl_sahooks;
drop table tbl_sahooks;

set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.metadata.DummySemanticAnalyzerHook,org.apache.hadoop.hive.ql.metadata.DummySemanticAnalyzerHook1;

drop table tbl_sahooks;
create table tbl_sahooks (c string);
desc extended tbl_sahooks;
drop table tbl_sahooks;

set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.metadata.DummySemanticAnalyzerHook1,org.apache.hadoop.hive.ql.metadata.DummySemanticAnalyzerHook1;

drop table tbl_sahooks;
create table tbl_sahooks (c string);
desc extended tbl_sahooks;

set hive.semantic.analyzer.hook=;
drop table tbl_sahooks;

