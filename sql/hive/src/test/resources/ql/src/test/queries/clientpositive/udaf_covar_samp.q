DROP TABLE covar_tab;
CREATE TABLE covar_tab (a INT, b INT, c INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/covar_tab.txt' OVERWRITE
INTO TABLE covar_tab;

DESCRIBE FUNCTION covar_samp;
DESCRIBE FUNCTION EXTENDED covar_samp;
SELECT covar_samp(b, c) FROM covar_tab WHERE a < 1;
SELECT covar_samp(b, c) FROM covar_tab WHERE a < 3;
SELECT covar_samp(b, c) FROM covar_tab WHERE a = 3;
SELECT a, covar_samp(b, c) FROM covar_tab GROUP BY a ORDER BY a;
SELECT covar_samp(b, c) FROM covar_tab;

DROP TABLE covar_tab;
