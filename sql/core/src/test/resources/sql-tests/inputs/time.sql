-- time literals, functions and operations

create temporary view time_view as select '11:53:26.038344' time_str, 'HH:mm:ss.SSSSSS' fmt_str;

select time '16:39:45\t';

select to_time(null), to_time('01:02:03'), to_time('23-59-59.999999', 'HH-mm-ss.SSSSSS');
select to_time(time_str, fmt_str) from time_view;

-- missing fields in `to_time`
select to_time("11", "HH");
-- invalid: there is no 13 hours
select to_time("13-60", "HH-mm");
