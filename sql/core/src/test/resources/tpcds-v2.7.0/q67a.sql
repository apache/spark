-- This is a new query in TPCDS v2.7
with results as (
    select
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy,
        s_store_id,
        sum(coalesce(ss_sales_price * ss_quantity, 0)) sumsales
    from
      store_sales, date_dim, store, item
    where
      ss_sold_date_sk=d_date_sk
        and ss_item_sk=i_item_sk
        and ss_store_sk = s_store_sk
        and d_month_seq between 1212 and 1212 + 11
    group by
      i_category,
      i_class,
      i_brand,
      i_product_name,
      d_year,
      d_qoy,
      d_moy,
      s_store_id),
results_rollup as (
    select
      i_category,
      i_class,
      i_brand,
      i_product_name,
      d_year,
      d_qoy,
      d_moy,
      s_store_id,
      sumsales
    from
      results
    union all
    select
      i_category,
      i_class,
      i_brand,
      i_product_name,
      d_year,
      d_qoy,
      d_moy,
      null s_store_id,
      sum(sumsales) sumsales
    from
      results
    group by
      i_category,
      i_class,
      i_brand,
      i_product_name,
      d_year,
      d_qoy,
      d_moy
    union all
    select
      i_category,
      i_class,
      i_brand,
      i_product_name,
      d_year,
      d_qoy,
      null d_moy,
      null s_store_id,
      sum(sumsales) sumsales
    from
      results
    group by
      i_category,
      i_class,
      i_brand,
      i_product_name,
      d_year,
      d_qoy
    union all
    select
      i_category,
      i_class,
      i_brand,
      i_product_name,
      d_year,
      null d_qoy,
      null d_moy,
      null s_store_id,
      sum(sumsales) sumsales
    from
      results
    group by
      i_category,
      i_class,
      i_brand,
      i_product_name,
      d_year
    union all
    select
      i_category,
      i_class,
      i_brand,
      i_product_name,
      null d_year,
      null d_qoy,
      null d_moy,
      null s_store_id,
      sum(sumsales) sumsales
  from
    results
  group by
    i_category,
    i_class,
    i_brand,
    i_product_name
  union all
  select
    i_category,
    i_class,
    i_brand,
    null i_product_name,
    null d_year,
    null d_qoy,
    null d_moy,
    null s_store_id,
    sum(sumsales) sumsales
  from
    results
  group by
    i_category,
    i_class,
    i_brand
  union all
  select
    i_category,
    i_class,
    null i_brand,
    null i_product_name,
    null d_year,
    null d_qoy,
    null d_moy,
    null s_store_id,
    sum(sumsales) sumsales
  from
    results
  group by
    i_category,
    i_class
  union all
  select
    i_category,
    null i_class,
    null i_brand,
    null i_product_name,
    null d_year,
    null d_qoy,
    null d_moy,
    null s_store_id,
    sum(sumsales) sumsales
  from results
  group by
    i_category
  union all
  select
    null i_category,
    null i_class,
    null i_brand,
    null i_product_name,
    null d_year,
    null d_qoy,
    null d_moy,
    null s_store_id,
    sum(sumsales) sumsales
  from
    results)
select
  *
from (
    select
      i_category,
      i_class,
      i_brand,
      i_product_name,
      d_year,
      d_qoy,
      d_moy,
      s_store_id,
      sumsales,
      rank() over (partition by i_category order by sumsales desc) rk
    from results_rollup) dw2
where
  rk <= 100
order by
  i_category,
  i_class,
  i_brand,
  i_product_name,
  d_year,
  d_qoy,
  d_moy,
  s_store_id,
  sumsales,
  rk
limit 100
