-- This query is the alternative form of sql/core/src/test/resources/tpcds/q14a.sql
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
          and d1.d_year between 1998 AND 1998 + 2
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
          and d2.d_year between 1998 AND 1998 + 2
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
          and d3.d_year between 1998 AND 1998 + 2) x
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
          and d_year between 1998 and 1998 + 2
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
          and d_year between 1998 and 1998 + 2) x)
select
  *
from (
    select
      'store' channel,
      i_brand_id,
      i_class_id,
      i_category_id,
      sum(ss_quantity * ss_list_price) sales,
      count(*) number_sales
    from
      store_sales, item, date_dim
    where
      ss_item_sk in (select ss_item_sk from cross_items)
        and ss_item_sk = i_item_sk
        and ss_sold_date_sk = d_date_sk
        and d_week_seq = (
            select d_week_seq
            from date_dim
            where d_year = 1998 + 1
              and d_moy = 12
              and d_dom = 16)
    group by
      i_brand_id,
      i_class_id,
      i_category_id
    having
      sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)) this_year,
  (
    select
      'store' channel,
      i_brand_id,
      i_class_id,
      i_category_id,
      sum(ss_quantity * ss_list_price) sales,
      count(*) number_sales
    from
      store_sales, item, date_dim
    where
      ss_item_sk in (select ss_item_sk from cross_items)
        and ss_item_sk = i_item_sk
        and ss_sold_date_sk = d_date_sk
        and d_week_seq = (
            select d_week_seq
            from date_dim
            where d_year = 1998
              and d_moy = 12
              and d_dom = 16)
    group by
      i_brand_id,
      i_class_id,
      i_category_id
    having
      sum(ss_quantity * ss_list_price) > (select average_sales from avg_sales)) last_year
where
  this_year.i_brand_id = last_year.i_brand_id
    and this_year.i_class_id = last_year.i_class_id
    and this_year.i_category_id = last_year.i_category_id
order by
  this_year.channel,
  this_year.i_brand_id,
  this_year.i_class_id,
  this_year.i_category_id
limit 100
