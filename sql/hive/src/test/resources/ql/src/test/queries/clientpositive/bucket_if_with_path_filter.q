dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/bmjpathfilter;

create table t1 (dt string) location '${system:test.tmp.dir}/bmjpathfilter/t1';
Create table t2 (dt string) stored as orc; 
dfs -touchz ${system:test.tmp.dir}/bmjpathfilter/t1/_SUCCESS;

SET hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat; 
SET hive.optimize.bucketmapjoin=true; 

SELECT /*+ MAPJOIN(b) */ a.dt FROM t1 a JOIN t2 b ON (a.dt = b.dt);
 
SET hive.optimize.bucketmapjoin=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

dfs -rmr ${system:test.tmp.dir}/bmjpathfilter;
