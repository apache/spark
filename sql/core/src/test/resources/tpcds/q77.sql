WITH ss AS
(SELECT
    s_store_sk,
    sum(ss_ext_sales_price) AS sales,
    sum(ss_net_profit) AS profit
  FROM store_sales, date_dim, store
  WHERE ss_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
    AND ss_store_sk = s_store_sk
  GROUP BY s_store_sk),
    sr AS
  (SELECT
    s_store_sk,
    sum(sr_return_amt) AS returns,
    sum(sr_net_loss) AS profit_loss
  FROM store_returns, date_dim, store
  WHERE sr_returned_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
    AND sr_store_sk = s_store_sk
  GROUP BY s_store_sk),
    cs AS
  (SELECT
    cs_call_center_sk,
    sum(cs_ext_sales_price) AS sales,
    sum(cs_net_profit) AS profit
  FROM catalog_sales, date_dim
  WHERE cs_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
  GROUP BY cs_call_center_sk),
    cr AS
  (SELECT
    sum(cr_return_amount) AS returns,
    sum(cr_net_loss) AS profit_loss
  FROM catalog_returns, date_dim
  WHERE cr_returned_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03]' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)),
    ws AS
  (SELECT
    wp_web_page_sk,
    sum(ws_ext_sales_price) AS sales,
    sum(ws_net_profit) AS profit
  FROM web_sales, date_dim, web_page
  WHERE ws_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
    AND ws_web_page_sk = wp_web_page_sk
  GROUP BY wp_web_page_sk),
    wr AS
  (SELECT
    wp_web_page_sk,
    sum(wr_return_amt) AS returns,
    sum(wr_net_loss) AS profit_loss
  FROM web_returns, date_dim, web_page
  WHERE wr_returned_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
    AND wr_web_page_sk = wp_web_page_sk
  GROUP BY wp_web_page_sk)
SELECT
  channel,
  id,
  sum(sales) AS sales,
  sum(returns) AS returns,
  sum(profit) AS profit
FROM
  (SELECT
     'store channel' AS channel,
     ss.s_store_sk AS id,
     sales,
     coalesce(returns, 0) AS returns,
     (profit - coalesce(profit_loss, 0)) AS profit
   FROM ss
     LEFT JOIN sr
       ON ss.s_store_sk = sr.s_store_sk
   UNION ALL
   SELECT
     'catalog channel' AS channel,
     cs_call_center_sk AS id,
     sales,
     returns,
     (profit - profit_loss) AS profit
   FROM cs, cr
   UNION ALL
   SELECT
     'web channel' AS channel,
     ws.wp_web_page_sk AS id,
     sales,
     coalesce(returns, 0) returns,
     (profit - coalesce(profit_loss, 0)) AS profit
   FROM ws
     LEFT JOIN wr
       ON ws.wp_web_page_sk = wr.wp_web_page_sk
  ) x
GROUP BY ROLLUP (channel, id)
ORDER BY channel, id
LIMIT 100
