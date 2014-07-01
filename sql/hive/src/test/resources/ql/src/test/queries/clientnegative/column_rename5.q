set hive.mapred.supports.subdirectories=true;

CREATE TABLE skewedtable (key STRING, value STRING) SKEWED BY (key) ON (1,5,6);

ALTER TABLE skewedtable CHANGE key key_new STRING;

