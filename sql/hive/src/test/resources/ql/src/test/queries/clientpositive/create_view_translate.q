drop view if exists v;
drop view if exists w;

create view v as select cast(key as string) from src;
describe formatted v;

create view w as select key, value from (
  select key, value from src
) a;
describe formatted w;

drop view v;
drop view w;


-- HIVE-4116 Can't use views using map datatype.

CREATE TABLE items (id INT, name STRING, info MAP<STRING,STRING>);

explain
CREATE VIEW priceview AS SELECT items.id, items.info['price'] FROM items;
CREATE VIEW priceview AS SELECT items.id, items.info['price'] FROM items;

select * from priceview;
