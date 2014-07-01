drop table ba_test;

-- this query tests ba_table1.q + nested queries with multiple operations on binary data types + union on binary types 
create table ba_test (ba_key binary, ba_val binary) ;

describe extended ba_test;

from src insert overwrite table ba_test select cast (src.key as binary), cast (src.value as binary);

select * from ( select key  from src where key < 50 union all select cast(ba_key as string) as key from ba_test limit 50) unioned order by key limit 10;

drop table ba_test;


