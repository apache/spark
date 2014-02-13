set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

select key from src tablesample(105 percent);
