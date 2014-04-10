drop table union_date_1;
drop table union_date_2;

create table union_date_1 (
  ORIGIN_CITY_NAME string,
  DEST_CITY_NAME string,
  FL_DATE date,
  ARR_DELAY float,
  FL_NUM int
);

create table union_date_2 (
  ORIGIN_CITY_NAME string,
  DEST_CITY_NAME string,
  FL_DATE date,
  ARR_DELAY float,
  FL_NUM int
);

LOAD DATA LOCAL INPATH '../data/files/flights_join.txt' OVERWRITE INTO TABLE union_date_1;
LOAD DATA LOCAL INPATH '../data/files/flights_join.txt' OVERWRITE INTO TABLE union_date_2;

select * from (
  select fl_num, fl_date from union_date_1
  union all
  select fl_num, fl_date from union_date_2
) union_result order by fl_date, fl_num;

drop table union_date_1;
drop table union_date_2;


