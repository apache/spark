set mapred.input.dir.recursive=true;

CREATE TABLE skewedtable (key STRING, value STRING) SKEWED BY (key) ON (1,5,6);
