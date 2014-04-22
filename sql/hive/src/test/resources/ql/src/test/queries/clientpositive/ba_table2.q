drop table ba_test;

-- All the test in ba_test1.q + using LazyBinarySerde instead of LazySimpleSerde

create table ba_test (ba_key binary, ba_val binary) ;
alter table ba_test set serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe';

describe extended ba_test;

from src insert overwrite table ba_test select cast (src.key as binary), cast (src.value as binary);

select ba_key, ba_val from ba_test order by ba_val limit 10;

drop table ba_test;


