-- start query 7 in stream 0 using template query7.tpl
select
  i_item_id,
  avg(ss_quantity) agg1,
  avg(ss_list_price) agg2,
  avg(ss_coupon_amt) agg3,
  avg(ss_sales_price) agg4
from
  store_sales,
  customer_demographics,
  date_dim,
  item,
  promotion
where
  ss_sold_date_sk = d_date_sk
  and ss_item_sk = i_item_sk
  and ss_cdemo_sk = cd_demo_sk
  and ss_promo_sk = p_promo_sk
  and cd_gender = 'F'
  and cd_marital_status = 'W'
  and cd_education_status = 'Primary'
  and (p_channel_email = 'N'
    or p_channel_event = 'N')
  and d_year = 1998
  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
group by
  i_item_id
order by
  i_item_id
limit 100
-- end query 7 in stream 0 using template query7.tpl
