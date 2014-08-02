-- Create table
create table if not exists test_invalid_column(key string, value string ) partitioned by (year string, month string) stored as textfile ;

select * from test_invalid_column  where test_invalid_column=123;
