
create table dest1_din1(key int, value string);

from src
insert overwrite table dest1_din1 select key, value
insert overwrite table dest1_din1 select key, value;

