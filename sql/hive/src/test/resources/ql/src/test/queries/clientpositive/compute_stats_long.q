create table tab_int(a int);

-- insert some data
LOAD DATA LOCAL INPATH "../data/files/int.txt" INTO TABLE tab_int;

select count(*) from tab_int;

-- compute statistical summary of data
select compute_stats(a, 16) from tab_int;
