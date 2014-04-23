set hive.mapred.supports.subdirectories=true;

CREATE TABLE skewed_table (key STRING, value STRING) SKEWED BY (key) ON ((1),(5,8),(6));
