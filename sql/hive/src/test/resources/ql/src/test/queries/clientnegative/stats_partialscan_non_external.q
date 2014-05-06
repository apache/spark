
CREATE EXTERNAL TABLE external_table (key int, value string);

-- we do not support analyze table ... partialscan on EXTERNAL tables yet
analyze table external_table compute statistics partialscan;
