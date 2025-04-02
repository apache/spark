CREATE TABLE test_src(key int, value string) stored as RCFILE;
set hive.io.rcfile.record.interval=5;
set hive.io.rcfile.record.buffer.size=100;
set hive.exec.compress.output=true;
INSERT OVERWRITE table test_src SELECT * FROM src;

set hive.io.rcfile.tolerate.corruptions=true;
SELECT key, value FROM test_src order by key;
