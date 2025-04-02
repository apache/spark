-- start query 55 in stream 0 using template query55.tpl
select
  i_brand_id brand_id,
  i_brand brand,
  sum(ss_ext_sales_price) ext_price
from
  date_dim,
  store_sales,
  item
where
  d_date_sk = ss_sold_date_sk
  and ss_item_sk = i_item_sk
  and i_manager_id = 48
  and d_moy = 11
  and d_year = 2001
  and ss_sold_date_sk between 2452215 and 2452244
group by
  i_brand,
  i_brand_id
order by
  ext_price desc,
  i_brand_id
limit 100
-- end query 55 in stream 0 using template query55.tpl
