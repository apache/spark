drop table ba_test;

-- Everything in ba_table1.q + columnar serde in RCFILE.

create table ba_test (ba_key binary, ba_val binary) stored as rcfile;
alter table ba_test set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';

describe extended ba_test;

from src insert overwrite table ba_test select cast (src.key as binary), cast (src.value as binary);

select ba_key, ba_val from ba_test order by ba_key limit 10;

drop table ba_test;


