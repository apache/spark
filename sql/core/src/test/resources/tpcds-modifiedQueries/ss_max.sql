select 
  count(*) as total,
  count(ss_sold_date_sk) as not_null_total,
  count(distinct ss_sold_date_sk) as unique_days,
  max(ss_sold_date_sk) as max_ss_sold_date_sk,
  max(ss_sold_time_sk) as max_ss_sold_time_sk,
  max(ss_item_sk) as max_ss_item_sk,
  max(ss_customer_sk) as max_ss_customer_sk,
  max(ss_cdemo_sk) as max_ss_cdemo_sk,
  max(ss_hdemo_sk) as max_ss_hdemo_sk,
  max(ss_addr_sk) as max_ss_addr_sk,
  max(ss_store_sk) as max_ss_store_sk,
  max(ss_promo_sk) as max_ss_promo_sk
from store_sales
