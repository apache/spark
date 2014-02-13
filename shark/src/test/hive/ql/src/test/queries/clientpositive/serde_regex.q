EXPLAIN
CREATE TABLE serde_regex(
  host STRING,
  identity STRING,
  user STRING,
  time STRING,
  request STRING,
  status STRING,
  size INT,
  referer STRING,
  agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\"))?"
)
STORED AS TEXTFILE;

CREATE TABLE serde_regex(
  host STRING,
  identity STRING,
  user STRING,
  time STRING,
  request STRING,
  status STRING,
  size INT,
  referer STRING,
  agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\"))?"
)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../data/files/apache.access.log" INTO TABLE serde_regex;
LOAD DATA LOCAL INPATH "../data/files/apache.access.2.log" INTO TABLE serde_regex;

SELECT * FROM serde_regex ORDER BY time;

SELECT host, size, status, time from serde_regex ORDER BY time;

DROP TABLE serde_regex;

EXPLAIN
CREATE TABLE serde_regex1(
  key decimal,
  value int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*)"
)
STORED AS TEXTFILE;

CREATE TABLE serde_regex1(
  key decimal,
  value int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*)"
)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../data/files/kv7.txt" INTO TABLE serde_regex1;

SELECT key, value FROM serde_regex1 ORDER BY key, value;

DROP TABLE serde_regex1;
