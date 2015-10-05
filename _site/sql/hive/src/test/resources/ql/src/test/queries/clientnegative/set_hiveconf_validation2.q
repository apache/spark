-- should fail: hive.fetch.task.conversion accepts minimal or more
desc src;

set hive.conf.validation=true;
set hive.fetch.task.conversion=true;
