USE default;
-- Mismatch between the number of matching groups and columns, throw run time exception. Ideally this should throw a compile time exception. See JIRA-3023 for more details.
 CREATE TABLE serde_regex(
  host STRING,
  identity STRING,
  user STRING,
  time STRING,
  request STRING,
  status STRING,
  size STRING,
  referer STRING,
  agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)"  
)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/apache.access.log" INTO TABLE serde_regex;
LOAD DATA LOCAL INPATH "../../data/files/apache.access.2.log" INTO TABLE serde_regex;

-- raise an exception 
SELECT * FROM serde_regex ORDER BY time;