drop table if exists parquet_decimal;

create table parquet_decimal (t decimal(4,2)) stored as parquet;
