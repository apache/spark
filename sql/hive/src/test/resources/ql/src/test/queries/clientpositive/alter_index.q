drop index src_index_8 on src;

create index src_index_8 on table src(key) as 'compact' WITH DEFERRED REBUILD IDXPROPERTIES ("prop1"="val1", "prop2"="val2"); 
desc extended default__src_src_index_8__;

alter index src_index_8 on src set IDXPROPERTIES ("prop1"="val1_new", "prop3"="val3"); 
desc extended default__src_src_index_8__;

drop index src_index_8 on src;

show tables;
