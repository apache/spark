create table alter_table_not_sorted (a int, b int) clustered by (a) sorted by (a) into 4 buckets;
desc formatted alter_table_not_sorted;

alter table alter_table_not_sorted not sorted;
desc formatted alter_table_not_sorted;

drop table alter_table_not_sorted;
