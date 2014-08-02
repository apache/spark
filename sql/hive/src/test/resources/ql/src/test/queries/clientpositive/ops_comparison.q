
select 1.0 < 2.0 from src limit 1;
select 2.0 < 2.0 from src limit 1;
select 2.0 > 1.0 from src limit 1;
select 2.0 > 2.0 from src limit 1;

select 'NaN' < 2.0 from src limit 1;
select 1.0 < 'NaN' from src limit 1;
select 1.0 > 'NaN' from src limit 1;
select 'NaN' > 2.0 from src limit 1;
select 'NaN' > 'NaN' from src limit 1;
select 'NaN' < 'NaN' from src limit 1;

select 'NaN' = 2.0 from src limit 1;
select 1.0 = 'NaN' from src limit 1;
select 'NaN' = 2.0 from src limit 1;
select 'NaN' = 'NaN' from src limit 1;

select 'NaN' <> 2.0 from src limit 1;
select 1.0 <> 'NaN' from src limit 1;
select 'NaN' <> 2.0 from src limit 1;
select 'NaN' <> 'NaN' from src limit 1;

