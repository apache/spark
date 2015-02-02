set hive.auto.convert.join=true;

-- empty tables
create table studenttab10k (name string, age int, gpa double);
create table votertab10k (name string, age int, registration string, contributions float);

explain select s.name, count(distinct registration)
from studenttab10k s join votertab10k v
on (s.name = v.name)
group by s.name;

select s.name, count(distinct registration)
from studenttab10k s join votertab10k v
on (s.name = v.name)
group by s.name;

set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;
set hive.auto.convert.sortmerge.join=true;

-- smb
create table studenttab10k_smb (name string, age int, gpa double) clustered by (name) sorted by (name) into 2 buckets;
create table votertab10k_smb (name string, age int, registration string, contributions float) clustered by (name) sorted by (name) into 2 buckets;

explain select s.name, count(distinct registration)
from studenttab10k_smb s join votertab10k_smb v
on (s.name = v.name)
group by s.name;

select s.name, count(distinct registration)
from studenttab10k_smb s join votertab10k_smb v
on (s.name = v.name)
group by s.name;

load data local inpath '../../data/files/empty1.txt' into table studenttab10k_smb;
load data local inpath '../../data/files/empty2.txt' into table studenttab10k_smb;
load data local inpath '../../data/files/empty1.txt' into table votertab10k_smb;
load data local inpath '../../data/files/empty2.txt' into table votertab10k_smb;

explain select s.name, count(distinct registration)
from studenttab10k_smb s join votertab10k_smb v
on (s.name = v.name)
group by s.name;

select s.name, count(distinct registration)
from studenttab10k_smb s join votertab10k_smb v
on (s.name = v.name)
group by s.name;

-- smb + partitions
create table studenttab10k_part (name string, age int, gpa double) partitioned by (p string) clustered by (name) sorted by (name) into 2 buckets;
create table votertab10k_part (name string, age int, registration string, contributions float) partitioned by (p string) clustered by (name) sorted by (name) into 2 buckets;

load data local inpath '../../data/files/empty1.txt' into table studenttab10k_part partition (p='foo');
load data local inpath '../../data/files/empty2.txt' into table studenttab10k_part partition (p='foo');
load data local inpath '../../data/files/empty1.txt' into table votertab10k_part partition (p='foo');
load data local inpath '../../data/files/empty2.txt' into table votertab10k_part partition (p='foo');

explain select s.name, count(distinct registration)
from studenttab10k_part s join votertab10k_part v
on (s.name = v.name)
where s.p = 'bar'
and v.p = 'bar'
group by s.name;

select s.name, count(distinct registration)
from studenttab10k_part s join votertab10k_part v
on (s.name = v.name)
where s.p = 'bar'
and v.p = 'bar'
group by s.name;

drop table studenttab10k;
drop table votertab10k;
drop table studenttab10k_smb;
drop table votertab10k_smb;
drop table studenttab10k_part;
drop table votertab10k_part;