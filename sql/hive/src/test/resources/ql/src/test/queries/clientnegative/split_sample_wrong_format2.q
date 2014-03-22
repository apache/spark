set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

select key from src tablesample(1K);
