
set hive.auto.convert.join=true;
set hive.auto.convert.join.use.nonstaged=false;

add jar ${system:maven.local.repository}/org/apache/hive/hcatalog/hive-hcatalog-core/${system:hive.version}/hive-hcatalog-core-${system:hive.version}.jar;

CREATE TABLE t1 (a string, b string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
;
LOAD DATA LOCAL INPATH "../../data/files/sample.json" INTO TABLE t1;
select * from src join t1 on src.key =t1.a;
drop table t1;
set hive.auto.convert.join=false;

