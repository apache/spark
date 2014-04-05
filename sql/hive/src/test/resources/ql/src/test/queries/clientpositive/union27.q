create table jackson_sev_same as select * from src;
create table dim_pho as select * from src;
create table jackson_sev_add as select * from src;
select b.* from jackson_sev_same a join (select * from dim_pho union all select * from jackson_sev_add)b on a.key=b.key and b.key=97;
