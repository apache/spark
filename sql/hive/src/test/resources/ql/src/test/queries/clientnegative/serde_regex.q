USE default;
--  This should fail because Regex SerDe doesn't support STRUCT
CREATE TABLE serde_regex(
  host STRING,
  identity STRING,
  user STRING,
  time TIMESTAMP,
  request STRING,
  status INT,
  size INT,
  referer STRING,
  agent STRING,
  strct STRUCT<a:INT, b:STRING>)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\"))?")
STORED AS TEXTFILE;
