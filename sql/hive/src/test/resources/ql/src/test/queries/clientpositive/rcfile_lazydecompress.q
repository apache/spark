
CREATE table rcfileTableLazyDecompress (key STRING, value STRING) STORED AS RCFile;

FROM src
INSERT OVERWRITE TABLE rcfileTableLazyDecompress SELECT src.key, src.value LIMIT 10;

SELECT key, value FROM rcfileTableLazyDecompress where key > 238 ORDER BY key ASC, value ASC;

SELECT key, value FROM rcfileTableLazyDecompress where key > 238 and key < 400 ORDER BY key ASC, value ASC;

SELECT key, count(1) FROM rcfileTableLazyDecompress where key > 238 group by key ORDER BY key ASC;

set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;

FROM src
INSERT OVERWRITE TABLE rcfileTableLazyDecompress SELECT src.key, src.value LIMIT 10;

SELECT key, value FROM rcfileTableLazyDecompress where key > 238 ORDER BY key ASC, value ASC;

SELECT key, value FROM rcfileTableLazyDecompress where key > 238 and key < 400 ORDER BY key ASC, value ASC;

SELECT key, count(1) FROM rcfileTableLazyDecompress where key > 238 group by key ORDER BY key ASC;

set mapreduce.output.fileoutputformat.compress=false;
set hive.exec.compress.output=false;

