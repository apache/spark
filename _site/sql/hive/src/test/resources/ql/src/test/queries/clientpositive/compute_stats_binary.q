create table tab_binary(a binary);

-- insert some data
LOAD DATA LOCAL INPATH "../../data/files/binary.txt" INTO TABLE tab_binary;

select count(*) from tab_binary;

-- compute statistical summary of data
select compute_stats(a, 16) from tab_binary;
