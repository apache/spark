set hive.exec.dynamic.partition=true;

create table srcpart_dp like srcpart;

create table destpart_dp like srcpart;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcpart_dp partition(ds='2008-04-08', hr=11);

insert overwrite table destpart_dp partition (ds='2008-04-08', hr) if not exists select key, value, hr from srcpart_dp where ds='2008-04-08';