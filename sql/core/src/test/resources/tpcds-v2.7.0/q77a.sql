-- This is a new query in TPCDS v2.7
with ss as (
    select
      s_store_sk,
      sum(ss_ext_sales_price) as sales,
      sum(ss_net_profit) as profit
    from
      store_sales, date_dim, store
    where
      ss_sold_date_sk = d_date_sk
        and d_date between cast('1998-08-04' as date)
        and (cast('1998-08-04' as date) + interval 30 days)
        and ss_store_sk = s_store_sk
    group by
      s_store_sk),
sr as (
    select
      s_store_sk,
      sum(sr_return_amt) as returns,
      sum(sr_net_loss) as profit_loss
    from
      store_returns, date_dim, store
    where
      sr_returned_date_sk = d_date_sk
        and d_date between cast('1998-08-04' as date)
        and (cast('1998-08-04' as date) + interval 30 days)
        and sr_store_sk = s_store_sk
     group by
       s_store_sk),
cs as (
    select
      cs_call_center_sk,
      sum(cs_ext_sales_price) as sales,
      sum(cs_net_profit) as profit
    from
      catalog_sales,
      date_dim
    where
      cs_sold_date_sk = d_date_sk
        and d_date between cast('1998-08-04' as date)
        and (cast('1998-08-04' as date) + interval 30 days)
    group by
      cs_call_center_sk),
 cr as (
     select
       sum(cr_return_amount) as returns,
       sum(cr_net_loss) as profit_loss
     from catalog_returns,
       date_dim
     where
       cr_returned_date_sk = d_date_sk
         and d_date between cast('1998-08-04' as date)
         and (cast('1998-08-04' as date) + interval 30 days)),
ws as ( select wp_web_page_sk,
        sum(ws_ext_sales_price) as sales,
        sum(ws_net_profit) as profit
 from web_sales,
      date_dim,
      web_page
 where ws_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-04' as date)
                  and (cast('1998-08-04' as date) +  interval 30 days)
       and ws_web_page_sk = wp_web_page_sk
 group by wp_web_page_sk), 
 wr as
 (select wp_web_page_sk,
        sum(wr_return_amt) as returns,
        sum(wr_net_loss) as profit_loss
 from web_returns,
      date_dim,
      web_page
 where wr_returned_date_sk = d_date_sk
       and d_date between cast('1998-08-04' as date)
                  and (cast('1998-08-04' as date) +  interval 30 days)
       and wr_web_page_sk = wp_web_page_sk
 group by wp_web_page_sk)
 ,
 results as
 (select channel
        , id
        , sum(sales) as sales
        , sum(returns) as returns
        , sum(profit) as profit
 from 
 (select 'store channel' as channel
        , ss.s_store_sk as id
        , sales
        , coalesce(returns, 0) as returns
        , (profit - coalesce(profit_loss,0)) as profit
 from   ss left join sr
        on  ss.s_store_sk = sr.s_store_sk
 union all
 select 'catalog channel' as channel
        , cs_call_center_sk as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from  cs
       , cr
 union all
 select 'web channel' as channel
        , ws.wp_web_page_sk as id
        , sales
        , coalesce(returns, 0) returns
        , (profit - coalesce(profit_loss,0)) as profit
 from   ws left join wr
        on  ws.wp_web_page_sk = wr.wp_web_page_sk
 ) x
 group by channel, id )

  select  *
 from (
 select channel, id, sales, returns, profit from  results
 union
 select channel, NULL AS id, sum(sales) as sales, sum(returns) as returns, sum(profit) as profit from  results group by channel
 union
 select NULL AS channel, NULL AS id, sum(sales) as sales, sum(returns) as returns, sum(profit) as profit from  results
) foo
order by
  channel, id
limit 100
