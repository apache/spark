-- time literals, functions and operations

create temporary view time_view as select '11:53:26.038344' time_str, 'HH:mm:ss.SSSSSS' fmt_str;

select time '16:39:45\t';

select to_time(null), to_time('01:02:03'), to_time('23-59-59.999999', 'HH-mm-ss.SSSSSS');
select to_time(time_str, fmt_str) from time_view;

-- missing fields in `to_time`
select to_time("11", "HH");
-- invalid: there is no 13 hours
select to_time("13-60", "HH-mm");

select try_to_time(null), try_to_time('00:12:00'), try_to_time('01:02:03', 'HH:mm:ss');
select try_to_time(1);
select try_to_time('12:48:31 abc');
select try_to_time('10:11:12.', 'HH:mm:ss.SSSSSS');
select try_to_time("02-69", "HH-mm");
select try_to_time('11:12:13', 'HH:mm:ss', 'SSSSSS');

select make_time(1, 18, 19.87);
-- invalid cases
select make_time(null, 18, 19.87);
select make_time(1, null, 19.87);
select make_time(1, 18, null);
select make_time(-1, 18, 19.87);
select make_time(1, 60, 19.87);
select make_time(1, 18, 60.0);
select make_time(1, 18, 9999999999.999999);
select make_time(1, 18, -999999999.999999);
-- Full seconds overflows to a valid seconds integer when converted from long to int
select make_time(1, 18, 4294967297.999999);

select secondoftime_with_fraction(to_time('23-59-59.987654', 'HH-mm-ss.SSSSSS'));
select second(to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select minute(to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select hour(to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));

select extract(HOUR from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(H from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(HOURS from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(HR from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(HRS from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));

select extract(MINUTE from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(M from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(MIN from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(MINS from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(MINUTES from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));

select extract(SECOND from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(S from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(SEC from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(SECONDS from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select extract(SECS from  to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));

-- test with precisions
select extract(SECOND FROM cast('09:08:01.987654' as time(0)));
select extract(SECOND FROM cast('09:08:01.987654' as time(1)));
select extract(SECOND FROM cast('09:08:01.987654' as time(2)));
select extract(SECOND FROM cast('09:08:01.987654' as time(3)));
select extract(SECOND FROM cast('09:08:01.987654' as time(4)));
select extract(SECOND FROM cast('09:08:01.987654' as time(5)));
select extract(SECOND FROM cast('09:08:01.987654' as time(6)));
