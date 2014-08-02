create table tab_double(a double);

-- insert some data
LOAD DATA LOCAL INPATH "../data/files/double.txt" INTO TABLE tab_double;

select count(*) from tab_double;

-- compute statistical summary of data
select compute_stats(a, 16) from tab_double;
