-- scanning partitioned data

create table tmptable(key string, value string, hr string, ds string);

explain extended 
insert overwrite table tmptable
select a.* from srcpart a where rand(1) < 0.1 and a.ds = '2008-04-08';


insert overwrite table tmptable
select a.* from srcpart a where rand(1) < 0.1 and a.ds = '2008-04-08';

select * from tmptable x sort by x.key,x.value,x.ds,x.hr;

