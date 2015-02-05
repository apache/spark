drop table date_join1;

create table date_join1 (
  ORIGIN_CITY_NAME string,
  DEST_CITY_NAME string,
  FL_DATE date,
  ARR_DELAY float,
  FL_NUM int
);

LOAD DATA LOCAL INPATH '../../data/files/flights_join.txt' OVERWRITE INTO TABLE date_join1;

-- Note that there are 2 rows with date 2000-11-28, so we should expect 4 rows with that date in the join results
select t1.fl_num, t1.fl_date, t2.fl_num, t2.fl_date
  from date_join1 t1 
  join date_join1 t2 
  on (t1.fl_date = t2.fl_date);

drop table date_join1;
