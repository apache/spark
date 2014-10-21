set hive.mapred.supports.subdirectories=true;

CREATE TABLE skewed_table (key STRING, value STRING) SKEWED BY (key,key) ON ((1),(5),(6));
