-- This is a new query in TPCDS v2.7
with results as (
    select
      sum(ss_net_profit) as total_sum,
      s_state ,s_county,
      0 as gstate,
      0 as g_county
    from
      store_sales, date_dim d1, store
    where
      d1.d_month_seq between 1212 and 1212 + 11
        and d1.d_date_sk = ss_sold_date_sk
        and s_store_sk  = ss_store_sk
        and s_state in (
            select s_state
            from (
                select
                  s_state as s_state,
                  rank() over (partition by s_state order by sum(ss_net_profit) desc) as ranking
                from store_sales, store, date_dim
                where d_month_seq between 1212 and 1212 + 11
                  and d_date_sk = ss_sold_date_sk
                  and s_store_sk  = ss_store_sk
                group by s_state) tmp1
              where ranking <= 5)
    group by
      s_state, s_county),
results_rollup as (
    select
      total_sum,
      s_state,
      s_county,
      0 as g_state,
      0 as g_county,
      0 as lochierarchy
    from results
    union
    select
      sum(total_sum) as total_sum,s_state,
      NULL as s_county,
      0 as g_state,
      1 as g_county,
      1 as lochierarchy
    from results
    group by s_state
    union
    select
      sum(total_sum) as total_sum,
      NULL as s_state,
      NULL as s_county,
      1 as g_state,
      1 as g_county,
      2 as lochierarchy
    from results)
select
  total_sum,
  s_state,
  s_county,
  lochierarchy,
  rank() over (
      partition by lochierarchy,
      case when g_county = 0 then s_state end
      order by total_sum desc) as rank_within_parent
from
  results_rollup
order by
  lochierarchy desc,
  case when lochierarchy = 0 then s_state end,
  rank_within_parent
limit 100
