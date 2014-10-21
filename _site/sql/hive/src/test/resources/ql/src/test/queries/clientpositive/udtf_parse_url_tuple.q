create table url_t (key string, fullurl string);

insert overwrite table url_t
select * from (
  select '1', 'http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' from src limit 1
  union all
  select '2', 'https://www.socs.uts.edu.au:80/MosaicDocs-old/url-primer.html?k1=tps#chapter1' from src limit 1
  union all
  select '3', 'ftp://sites.google.com/a/example.com/site/page' from src limit 1
  union all
  select '4', cast(null as string) from src limit 1
  union all
  select '5', 'htttp://' from src limit 1
  union all
  select '6', '[invalid url string]' from src limit 1
) s;

describe function parse_url_tuple;
describe function extended parse_url_tuple;

explain 
select a.key, b.* from url_t a lateral view parse_url_tuple(a.fullurl, 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE', 'AUTHORITY', 'USERINFO', 'QUERY:k1') b as ho, pa, qu, re, pr, fi, au, us, qk1 order by a.key;

select a.key, b.* from url_t a lateral view parse_url_tuple(a.fullurl, 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE', 'AUTHORITY', 'USERINFO', 'QUERY:k1') b as ho, pa, qu, re, pr, fi, au, us, qk1 order by a.key;

explain 
select parse_url_tuple(a.fullurl, 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE', 'AUTHORITY', 'USERINFO', 'QUERY:k1') as (ho, pa, qu, re, pr, fi, au, us, qk1) from url_t a order by ho, pa, qu;

select parse_url_tuple(a.fullurl, 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE', 'AUTHORITY', 'USERINFO', 'QUERY:k1') as (ho, pa, qu, re, pr, fi, au, us, qk1) from url_t a order by ho, pa, qu;

-- should return null for 'host', 'query', 'QUERY:nonExistCol' 
explain
select a.key, b.ho, b.qu, b.qk1, b.err1, b.err2, b.err3 from url_t a lateral view parse_url_tuple(a.fullurl, 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE', 'AUTHORITY', 'USERINFO', 'QUERY:k1', 'host', 'query', 'QUERY:nonExistCol') b as ho, pa, qu, re, pr, fi, au, us, qk1, err1, err2, err3 order by a.key;

select a.key, b.ho, b.qu, b.qk1, b.err1, b.err2, b.err3 from url_t a lateral view parse_url_tuple(a.fullurl, 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE', 'AUTHORITY', 'USERINFO', 'QUERY:k1', 'host', 'query', 'QUERY:nonExistCol') b as ho, pa, qu, re, pr, fi, au, us, qk1, err1, err2, err3 order by a.key;


explain
select ho, count(*) from url_t a lateral view parse_url_tuple(a.fullurl, 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE', 'AUTHORITY', 'USERINFO', 'QUERY:k1') b as ho, pa, qu, re, pr, fi, au, us, qk1 where qk1 is not null group by ho;

select ho, count(*) from url_t a lateral view parse_url_tuple(a.fullurl, 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE', 'AUTHORITY', 'USERINFO', 'QUERY:k1') b as ho, pa, qu, re, pr, fi, au, us, qk1 where qk1 is not null group by ho;

