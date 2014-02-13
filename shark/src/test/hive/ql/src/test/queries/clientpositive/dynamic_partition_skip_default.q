create table dynamic_part_table(intcol int) partitioned by (partcol1 int, partcol2 int);

set hive.exec.dynamic.partition.mode=nonstrict;

insert into table dynamic_part_table partition(partcol1, partcol2) select 1, 1, 1 from src where key=150;

insert into table dynamic_part_table partition(partcol1, partcol2) select 1, NULL, 1 from src where key=150;

insert into table dynamic_part_table partition(partcol1, partcol2) select 1, 1, NULL from src where key=150;

insert into table dynamic_part_table partition(partcol1, partcol2) select 1, NULL, NULL from src where key=150;

explain extended select intcol from dynamic_part_table where partcol1=1 and partcol2=1;

set hive.exec.dynamic.partition.mode=strict;

explain extended select intcol from dynamic_part_table where partcol1=1 and partcol2=1;

explain extended select intcol from dynamic_part_table where (partcol1=1 and partcol2=1)or (partcol1=1 and partcol2='__HIVE_DEFAULT_PARTITION__');
