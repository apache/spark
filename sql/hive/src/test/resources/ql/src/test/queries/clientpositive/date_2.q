drop table if exists date_2;

create table date_2 (
  ORIGIN_CITY_NAME string,
  DEST_CITY_NAME string,
  FL_DATE date,
  ARR_DELAY float,
  FL_NUM int
);

LOAD DATA LOCAL INPATH '../../data/files/flights_tiny.txt.1' OVERWRITE INTO TABLE date_2;

select fl_date, fl_num from date_2 order by fl_date asc, fl_num desc;
select fl_date, fl_num from date_2 order by fl_date desc, fl_num asc;

select fl_date, count(*) from date_2 group by fl_date;

drop table date_2;
