-- date time functions

-- [SPARK-16836] current_date and current_timestamp literals
select current_date = current_date(), current_timestamp = current_timestamp();

select to_timestamp('2016-12-31 00:12:00'), to_timestamp('2016-12-31', 'yyyy-MM-dd');
