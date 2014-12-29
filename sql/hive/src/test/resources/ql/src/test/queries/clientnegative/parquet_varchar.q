drop table if exists parquet_varchar;

create table parquet_varchar (t varchar(10)) stored as parquet;
