CREATE TABLE dest(key INT, value STRING) STORED AS TEXTFILE;

SET hive.output.file.extension=.txt;
INSERT OVERWRITE TABLE dest SELECT src.* FROM src;

dfs -cat ${system:test.warehouse.dir}/dest/*.txt
