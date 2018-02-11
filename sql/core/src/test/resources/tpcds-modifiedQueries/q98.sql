-- start query 98 in stream 0 using template query98.tpl
select
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  sum(ss_ext_sales_price) as itemrevenue,
  sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price)) over (partition by i_class) as revenueratio
from
  store_sales,
  item,
  date_dim
where
  ss_item_sk = i_item_sk
  and i_category in ('Jewelry', 'Sports', 'Books')
  and ss_sold_date_sk = d_date_sk
  and ss_sold_date_sk between 2451911 and 2451941  -- partition key filter (1 calendar month)
  and d_date between '2001-01-01' and '2001-01-31'
group by
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price
order by
  i_category,
  i_class,
  i_item_id,
  i_item_desc,
  revenueratio
--limit 1000; -- added limit
-- end query 98 in stream 0 using template query98.tpl
