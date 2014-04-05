
create table dest1_din2(key int, value string) partitioned by (ds string);

from src
insert overwrite table dest1_din2 partition (ds='1') select key, value
insert overwrite table dest1_din2 partition (ds='1') select key, value;
