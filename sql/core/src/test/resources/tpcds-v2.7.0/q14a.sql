-- This query is the alternative form of sql/core/src/test/resources/tpcds/q14b.sql
with cross_items as (
    select
      i_item_sk ss_item_sk
    from item, (
        select
          iss.i_brand_id brand_id,
          iss.i_class_id class_id,
          iss.i_category_id category_id
        from
          store_sales, item iss, date_dim d1
        where
          ss_item_sk = iss.i_item_sk
            and ss_sold_date_sk = d1.d_date_sk
            and d1.d_year between 1999 AND 1999 + 2
        intersect
        select
          ics.i_brand_id,
          ics.i_class_id,
          ics.i_category_id
        from
          catalog_sales, item ics, date_dim d2
        where
          cs_item_sk = ics.i_item_sk
            and cs_sold_date_sk = d2.d_date_sk
            and d2.d_year between 1999 AND 1999 + 2
        intersect
        select
          iws.i_brand_id,
          iws.i_class_id,
          iws.i_category_id
        from
          web_sales, item iws, date_dim d3
        where
          ws_item_sk = iws.i_item_sk
            and ws_sold_date_sk = d3.d_date_sk
            and d3.d_year between 1999 AND 1999 + 2) x
    where
      i_brand_id = brand_id
        and i_class_id = class_id
        and i_category_id = category_id),
avg_sales as (
    select
      avg(quantity*list_price) average_sales
    from (
        select
          ss_quantity quantity,
          ss_list_price list_price
         from
           store_sales, date_dim
         where
           ss_sold_date_sk = d_date_sk
             and d_year between 1999 and 2001
         union all
         select
           cs_quantity quantity,
           cs_list_price list_price
         from
           catalog_sales, date_dim
         where
           cs_sold_date_sk = d_date_sk
             and d_year between 1998 and 1998 + 2
         union all
         select
           ws_quantity quantity,
           ws_list_price list_price
         from
           web_sales, date_dim
         where
           ws_sold_date_sk = d_date_sk
             and d_year between 1998 and 1998 + 2) x),
results AS (
    select
      channel,
      i_brand_id,
      i_class_id,
      i_category_id,
      sum(sales) sum_sales,
      sum(number_sales) number_sales
    from (
        select
          'store' channel,
          i_brand_id,i_class_id,
          i_category_id,
          sum(ss_quantity*ss_list_price) sales,
          count(*) number_sales
       from
         store_sales, item, date_dim
       where
         ss_item_sk in (select ss_item_sk from cross_items)
           and ss_item_sk = i_item_sk
           and ss_sold_date_sk = d_date_sk
           and d_year = 1998 + 2
           and d_moy = 11
       group by
         i_brand_id,
         i_class_id,
         i_category_id
       having
         sum(ss_quantity * ss_list_price) > (select average_sales from avg_sales)
       union all
       select
         'catalog' channel,
         i_brand_id,
         i_class_id,
         i_category_id,
         sum(cs_quantity*cs_list_price) sales,
         count(*) number_sales
       from
         catalog_sales, item, date_dim
       where
         cs_item_sk in (select ss_item_sk from cross_items)
           and cs_item_sk = i_item_sk
           and cs_sold_date_sk = d_date_sk
           and d_year = 1998+2
           and d_moy = 11
       group by
         i_brand_id,i_class_id,i_category_id
       having
         sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
       union all
       select
         'web' channel,
         i_brand_id,
         i_class_id,
         i_category_id,
         sum(ws_quantity*ws_list_price) sales,
         count(*) number_sales
       from
         web_sales, item, date_dim
       where
         ws_item_sk in (select ss_item_sk from cross_items)
           and ws_item_sk = i_item_sk
           and ws_sold_date_sk = d_date_sk
           and d_year = 1998 + 2
           and d_moy = 11
       group by
         i_brand_id,
         i_class_id,
         i_category_id
       having
         sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)) y
    group by
      channel,
      i_brand_id,
      i_class_id,
      i_category_id)
select
  channel,
  i_brand_id,
  i_class_id,
  i_category_id,
  sum_sales,
  number_sales
from (
    select
      channel,
      i_brand_id,
      i_class_id,
      i_category_id,
      sum_sales,
      number_sales
    from
      results
    union
    select
      channel,
      i_brand_id,
      i_class_id,
      null as i_category_id,
      sum(sum_sales),
      sum(number_sales)
    from results
    group by
      channel,
      i_brand_id,
      i_class_id
    union
    select
      channel,
      i_brand_id,
      null as i_class_id,
      null as i_category_id,
      sum(sum_sales),
      sum(number_sales)
    from results
    group by
      channel,
      i_brand_id
    union
    select
      channel,
      null as i_brand_id,
      null as i_class_id,
      null as i_category_id,
      sum(sum_sales),
      sum(number_sales)
    from results
    group by
      channel
    union
    select
      null as channel,
      null as i_brand_id,
      null as i_class_id,
      null as i_category_id,
      sum(sum_sales),
      sum(number_sales)
    from results) z
order by
  channel,
  i_brand_id,
  i_class_id,
  i_category_id
limit 100
