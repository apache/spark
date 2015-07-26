set hive.fetch.task.conversion=more;

drop table timestamp_udf;
drop table timestamp_udf_string;

create table timestamp_udf (t timestamp);
create table timestamp_udf_string (t string);
from (select * from src tablesample (1 rows)) s
  insert overwrite table timestamp_udf 
    select '2011-05-06 07:08:09.1234567'
  insert overwrite table timestamp_udf_string
    select '2011-05-06 07:08:09.1234567';

-- Test UDFs with Timestamp input
select unix_timestamp(t), year(t), month(t), day(t), dayofmonth(t),
    weekofyear(t), hour(t), minute(t), second(t), to_date(t)
  from timestamp_udf;

select date_add(t, 5), date_sub(t, 10)
  from timestamp_udf;

select datediff(t, t), datediff(t, '2002-03-21'), datediff('2002-03-21', t)
  from timestamp_udf;

select from_utc_timestamp(t, 'America/Chicago')
  from timestamp_udf;

select to_utc_timestamp(t, 'America/Chicago')
  from timestamp_udf;

select t, from_utc_timestamp(t, 'America/Chicago')
  from timestamp_udf;

select t, from_utc_timestamp(t, 'America/Chicago'), t, from_utc_timestamp(t, 'America/Chicago')
  from timestamp_udf;

select t, to_utc_timestamp(t, 'America/Chicago')
  from timestamp_udf;

select t, to_utc_timestamp(t, 'America/Chicago'), t, to_utc_timestamp(t, 'America/Chicago')
  from timestamp_udf;

-- Test UDFs with string input
select unix_timestamp(t), year(t), month(t), day(t), dayofmonth(t), 
    weekofyear(t), hour(t), minute(t), second(t), to_date(t)
  from timestamp_udf_string;

select date_add(t, 5), date_sub(t, 10)  from timestamp_udf_string;

select datediff(t, t), datediff(t, '2002-03-21'), datediff('2002-03-21', t)
  from timestamp_udf_string;

select from_utc_timestamp(t, 'America/Chicago')
  from timestamp_udf_string;

select to_utc_timestamp(t, 'America/Chicago')
  from timestamp_udf_string;

drop table timestamp_udf;
drop table timestamp_udf_string;
