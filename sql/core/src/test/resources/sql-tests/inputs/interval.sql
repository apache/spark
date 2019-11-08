-- multiply and divide an interval by a number
select 3 * (timestamp'2019-10-15 10:11:12.001002' - date'2019-10-15');
select interval 4 month 2 weeks 3 microseconds * 1.5;
select (timestamp'2019-10-15' - timestamp'2019-10-14') / 1.5;

-- interval operation with null and zero case
select interval '2 seconds' / 0;
select interval '2 seconds' / null;
select interval '2 seconds' * null;
select null * interval '2 seconds';

-- interval with sign
select -interval '-1 month 1 day -1 second';
select +interval '-1 month 1 day -1 second';
select -interval -1 month 1 day -1 second;
select +interval -1 month 1 day -1 second;
