describe srcpart;
describe srcpart.key;
describe srcpart PARTITION(ds='2008-04-08', hr='12');

describe extended srcpart;
describe extended srcpart.key;
describe extended srcpart PARTITION(ds='2008-04-08', hr='12');

describe formatted srcpart;
describe formatted srcpart.key;
describe formatted srcpart PARTITION(ds='2008-04-08', hr='12');

create table srcpart_serdeprops like srcpart;
alter table srcpart_serdeprops set serdeproperties('xyz'='0');
alter table srcpart_serdeprops set serdeproperties('pqrs'='1');
alter table srcpart_serdeprops set serdeproperties('abcd'='2');
alter table srcpart_serdeprops set serdeproperties('A1234'='3');
describe formatted srcpart_serdeprops;
drop table srcpart_serdeprops;
