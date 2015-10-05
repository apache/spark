

create table partition_vs_table(key string, value string) partitioned by (ds string);

insert overwrite table partition_vs_table partition(ds='100') select key, value from src;

alter table partition_vs_table add columns (newcol string);

insert overwrite table partition_vs_table partition(ds='101') select key, value, key from src;

select key, value, newcol from partition_vs_table
order by key, value, newcol;

