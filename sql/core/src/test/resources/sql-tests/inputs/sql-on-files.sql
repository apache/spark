-- Parquet
SELECT * FROM parquet.``;
SELECT * FROM parquet.`/file/not/found`;
SELECT * FROM parquet.`src/test/resources/test-data/dec-in-fixed-len.parquet` LIMIT 1;

-- ORC
SELECT * FROM orc.``;
SELECT * FROM orc.`/file/not/found`;
SELECT * FROM orc.`src/test/resources/test-data/before_1582_date_v2_4.snappy.orc` LIMIT 1;

-- CSV
SELECT * FROM csv.``;
SELECT * FROM csv.`/file/not/found`;
SELECT * FROM csv.`src/test/resources/test-data/cars.csv` LIMIT 1;

-- JSON
SELECT * FROM json.``;
SELECT * FROM json.`/file/not/found`;
SELECT * FROM json.`src/test/resources/test-data/with-map-fields.json` LIMIT 1;
