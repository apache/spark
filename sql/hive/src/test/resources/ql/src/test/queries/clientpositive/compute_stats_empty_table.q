create table tab_empty(a boolean, b int, c double, d string, e binary);

select count(*) from tab_empty;

-- compute statistical summary of data
select compute_stats(a, 16) from tab_empty;
select compute_stats(b, 16) from tab_empty;
select compute_stats(c, 16) from tab_empty;
select compute_stats(d, 16) from tab_empty;
select compute_stats(e, 16) from tab_empty;


