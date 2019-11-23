create table tstnullinut(a string, b string);
select x.* from tstnullinut x;
select x.a, count(1) from tstnullinut x group by x.a;

