CREATE TEMPORARY FUNCTION test_max AS 'org.apache.hadoop.hive.ql.udf.UDAFTestMax';

create table dest_grouped_old1 as select 1+1, 2+2 as zz, src.key, test_max(length(src.value)), count(src.value), sin(count(src.value)), count(sin(src.value)), unix_timestamp(), CAST(SUM(IF(value > 10, value, 1)) AS INT), if(src.key > 1,
1,
0)
 from src group by src.key;
describe dest_grouped_old1;

create table dest_grouped_old2 as select distinct src.key from src;
describe dest_grouped_old2;

set hive.autogen.columnalias.prefix.label=column_;
set hive.autogen.columnalias.prefix.includefuncname=true;

create table dest_grouped_new1 as select 1+1, 2+2 as zz, ((src.key % 2)+2)/2, test_max(length(src.value)), count(src.value), sin(count(src.value)), count(sin(src.value)), unix_timestamp(), CAST(SUM(IF(value > 10, value, 1)) AS INT), if(src.key > 10,
	(src.key +5) % 2,
0)
from src group by src.key;
describe dest_grouped_new1;

create table dest_grouped_new2 as select distinct src.key from src;
describe dest_grouped_new2;

-- Drop the temporary function at the end till HIVE-3160 gets fixed
DROP TEMPORARY FUNCTION test_max;
