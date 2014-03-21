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
