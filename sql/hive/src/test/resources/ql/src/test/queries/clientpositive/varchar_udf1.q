drop table varchar_udf_1;

create table varchar_udf_1 (c1 string, c2 string, c3 varchar(10), c4 varchar(20));
insert overwrite table varchar_udf_1
  select key, value, key, value from src where key = '238' limit 1;

-- UDFs with varchar support
select 
  concat(c1, c2),
  concat(c3, c4),
  concat(c1, c2) = concat(c3, c4)
from varchar_udf_1 limit 1;

select
  upper(c2),
  upper(c4),
  upper(c2) = upper(c4)
from varchar_udf_1 limit 1;

select
  lower(c2),
  lower(c4),
  lower(c2) = lower(c4)
from varchar_udf_1 limit 1;

-- Scalar UDFs
select
  ascii(c2),
  ascii(c4),
  ascii(c2) = ascii(c4)
from varchar_udf_1 limit 1;

select 
  concat_ws('|', c1, c2),
  concat_ws('|', c3, c4),
  concat_ws('|', c1, c2) = concat_ws('|', c3, c4)
from varchar_udf_1 limit 1;

select
  decode(encode(c2, 'US-ASCII'), 'US-ASCII'),
  decode(encode(c4, 'US-ASCII'), 'US-ASCII'),
  decode(encode(c2, 'US-ASCII'), 'US-ASCII') = decode(encode(c4, 'US-ASCII'), 'US-ASCII')
from varchar_udf_1 limit 1;

select
  instr(c2, '_'),
  instr(c4, '_'),
  instr(c2, '_') = instr(c4, '_')
from varchar_udf_1 limit 1;

select
  length(c2),
  length(c4),
  length(c2) = length(c4)
from varchar_udf_1 limit 1;

select
  locate('a', 'abcdabcd', 3),
  locate(cast('a' as varchar(1)), cast('abcdabcd' as varchar(10)), 3),
  locate('a', 'abcdabcd', 3) = locate(cast('a' as varchar(1)), cast('abcdabcd' as varchar(10)), 3)
from varchar_udf_1 limit 1;

select
  lpad(c2, 15, ' '),
  lpad(c4, 15, ' '),
  lpad(c2, 15, ' ') = lpad(c4, 15, ' ')
from varchar_udf_1 limit 1;

select
  ltrim(c2),
  ltrim(c4),
  ltrim(c2) = ltrim(c4)
from varchar_udf_1 limit 1;

select
  regexp(c2, 'val'),
  regexp(c4, 'val'),
  regexp(c2, 'val') = regexp(c4, 'val')
from varchar_udf_1 limit 1;

select
  regexp_extract(c2, 'val_([0-9]+)', 1),
  regexp_extract(c4, 'val_([0-9]+)', 1),
  regexp_extract(c2, 'val_([0-9]+)', 1) = regexp_extract(c4, 'val_([0-9]+)', 1)
from varchar_udf_1 limit 1;

select
  regexp_replace(c2, 'val', 'replaced'),
  regexp_replace(c4, 'val', 'replaced'),
  regexp_replace(c2, 'val', 'replaced') = regexp_replace(c4, 'val', 'replaced')
from varchar_udf_1 limit 1;

select
  reverse(c2),
  reverse(c4),
  reverse(c2) = reverse(c4)
from varchar_udf_1 limit 1;

select
  rpad(c2, 15, ' '),
  rpad(c4, 15, ' '),
  rpad(c2, 15, ' ') = rpad(c4, 15, ' ')
from varchar_udf_1 limit 1;

select
  rtrim(c2),
  rtrim(c4),
  rtrim(c2) = rtrim(c4)
from varchar_udf_1 limit 1;

select
  sentences('See spot run.  See jane run.'),
  sentences(cast('See spot run.  See jane run.' as varchar(50)))
from varchar_udf_1 limit 1;

select
  split(c2, '_'),
  split(c4, '_')
from varchar_udf_1 limit 1;

select 
  str_to_map('a:1,b:2,c:3',',',':'),
  str_to_map(cast('a:1,b:2,c:3' as varchar(20)),',',':')
from varchar_udf_1 limit 1;

select
  substr(c2, 1, 3),
  substr(c4, 1, 3),
  substr(c2, 1, 3) = substr(c4, 1, 3)
from varchar_udf_1 limit 1;

select
  trim(c2),
  trim(c4),
  trim(c2) = trim(c4)
from varchar_udf_1 limit 1;


-- Aggregate Functions
select
  compute_stats(c2, 16),
  compute_stats(c4, 16)
from varchar_udf_1;

select
  min(c2),
  min(c4)
from varchar_udf_1;

select
  max(c2),
  max(c4)
from varchar_udf_1;


drop table varchar_udf_1;
