select try_to_timestamp(null), try_to_timestamp('2016-12-31 00:12:00'), try_to_timestamp('2016-12-31', 'yyyy-MM-dd');
select try_to_timestamp(1);
select try_to_timestamp('2016-12-31 abc');
select try_to_timestamp('2019-10-06 10:11:12.', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select try_to_timestamp("02-29", "MM-dd");
select try_to_timestamp('22 05 2020 Friday', 'dd MM yyyy EEEEEE');
