SELECT
  count(*) AS total,
  count(ss_sold_date_sk) AS not_null_total,
  count(DISTINCT ss_sold_date_sk) AS unique_days,
  max(ss_sold_date_sk) AS max_ss_sold_date_sk,
  max(ss_sold_time_sk) AS max_ss_sold_time_sk,
  max(ss_item_sk) AS max_ss_item_sk,
  max(ss_customer_sk) AS max_ss_customer_sk,
  max(ss_cdemo_sk) AS max_ss_cdemo_sk,
  max(ss_hdemo_sk) AS max_ss_hdemo_sk,
  max(ss_addr_sk) AS max_ss_addr_sk,
  max(ss_store_sk) AS max_ss_store_sk,
  max(ss_promo_sk) AS max_ss_promo_sk
FROM store_sales
