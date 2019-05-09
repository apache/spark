-- This is a new query in TPCDS v2.7
with results as (
    select
      sum(ws_net_paid) as total_sum,
      i_category, i_class,
      0 as g_category,
      0 as g_class
    from
      web_sales, date_dim d1, item
    where
      d1.d_month_seq between 1212 and 1212 + 11
        and d1.d_date_sk = ws_sold_date_sk
        and i_item_sk = ws_item_sk
    group by
      i_category, i_class),
results_rollup as(
    select
      total_sum,
      i_category,
      i_class,
      g_category,
      g_class,
      0 as lochierarchy
    from
      results
    union
    select
      sum(total_sum) as total_sum,
      i_category,
      NULL as i_class,
      0 as g_category,
      1 as g_class,
      1 as lochierarchy
    from
      results
    group by
      i_category
    union
    select
      sum(total_sum) as total_sum,
      NULL as i_category,
      NULL as i_class,
      1 as g_category,
      1 as g_class,
      2 as lochierarchy
    from
      results)
select
  total_sum,
  i_category ,i_class, lochierarchy,
  rank() over (
      partition by lochierarchy,
        case when g_class = 0 then i_category end
      order by total_sum desc) as rank_within_parent
from
  results_rollup
order by
  lochierarchy desc,
  case when lochierarchy = 0 then i_category end,
  rank_within_parent
limit 100
