-- start query 43 in stream 0 using template query43.tpl
select
  s_store_name,
  s_store_id,
  sum(case when (d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
  sum(case when (d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
  sum(case when (d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
  sum(case when (d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
  sum(case when (d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
  sum(case when (d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
  sum(case when (d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
from
  date_dim,
  store_sales,
  store
where
  d_date_sk = ss_sold_date_sk
  and s_store_sk = ss_store_sk
  and s_gmt_offset = -5
  and d_year = 1998
  and ss_sold_date_sk between 2450816 and 2451179  -- partition key filter
group by
  s_store_name,
  s_store_id
order by
  s_store_name,
  s_store_id,
  sun_sales,
  mon_sales,
  tue_sales,
  wed_sales,
  thu_sales,
  fri_sales,
  sat_sales
limit 100
-- end query 43 in stream 0 using template query43.tpl
