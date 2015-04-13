create table tab_int(a int);

-- insert some data
LOAD DATA LOCAL INPATH "../../data/files/int.txt" INTO TABLE tab_int;

-- compute stats should raise an error since the number of bit vectors > 1024
select compute_stats(a, 10000) from tab_int;
