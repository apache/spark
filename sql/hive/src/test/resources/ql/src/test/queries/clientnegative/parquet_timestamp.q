drop table if exists parquet_timestamp;

create table parquet_timestamp (t timestamp) stored as parquet;
