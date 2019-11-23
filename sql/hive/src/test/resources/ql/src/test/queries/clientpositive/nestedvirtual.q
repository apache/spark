CREATE TABLE pokes (foo INT, bar STRING);
create table pokes2(foo INT, bar STRING);

create table jssarma_nilzma_bad as select a.val, a.filename, a.offset from (select hash(foo) as val, INPUT__FILE__NAME as filename, BLOCK__OFFSET__INSIDE__FILE as  offset from pokes) a join pokes2 b on (a.val = b.foo);

drop table jssarma_nilzma_bad;

drop table pokes;
drop table pokes2;
CREATE TABLE pokes (foo INT, bar STRING);
create table pokes2(foo INT, bar STRING);

create table jssarma_nilzma_bad as select a.val, a.filename, a.offset from (select hash(foo) as val, INPUT__FILE__NAME as filename, BLOCK__OFFSET__INSIDE__FILE as  offset from pokes) a join pokes2 b on (a.val = b.foo);

drop table jssarma_nilzma_bad;

drop table pokes;
drop table pokes2;
CREATE TABLE pokes (foo INT, bar STRING);
create table pokes2(foo INT, bar STRING);

create table jssarma_nilzma_bad as select a.val, a.filename, a.offset from (select hash(foo) as val, INPUT__FILE__NAME as filename, BLOCK__OFFSET__INSIDE__FILE as  offset from pokes) a join pokes2 b on (a.val = b.foo);

drop table jssarma_nilzma_bad;

drop table pokes;
drop table pokes2;
