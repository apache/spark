WITH ssr AS
( SELECT
    s_store_id,
    sum(sales_price) AS sales,
    sum(profit) AS profit,
    sum(return_amt) AS RETURNS,
    sum(net_loss) AS profit_loss
  FROM
    (SELECT
       ss_store_sk AS store_sk,
       ss_sold_date_sk AS date_sk,
       ss_ext_sales_price AS sales_price,
       ss_net_profit AS profit,
       cast(0 AS DECIMAL(7, 2)) AS return_amt,
       cast(0 AS DECIMAL(7, 2)) AS net_loss
     FROM store_sales
     UNION ALL
     SELECT
       sr_store_sk AS store_sk,
       sr_returned_date_sk AS date_sk,
       cast(0 AS DECIMAL(7, 2)) AS sales_price,
       cast(0 AS DECIMAL(7, 2)) AS profit,
       sr_return_amt AS return_amt,
       sr_net_loss AS net_loss
     FROM store_returns)
    salesreturns, date_dim, store
  WHERE date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-23' AS DATE)
  AND ((cast('2000-08-23' AS DATE) + INTERVAL 14 days))
    AND store_sk = s_store_sk
  GROUP BY s_store_id),
    csr AS
  ( SELECT
    cp_catalog_page_id,
    sum(sales_price) AS sales,
    sum(profit) AS profit,
    sum(return_amt) AS RETURNS,
    sum(net_loss) AS profit_loss
  FROM
    (SELECT
       cs_catalog_page_sk AS page_sk,
       cs_sold_date_sk AS date_sk,
       cs_ext_sales_price AS sales_price,
       cs_net_profit AS profit,
       cast(0 AS DECIMAL(7, 2)) AS return_amt,
       cast(0 AS DECIMAL(7, 2)) AS net_loss
     FROM catalog_sales
     UNION ALL
     SELECT
       cr_catalog_page_sk AS page_sk,
       cr_returned_date_sk AS date_sk,
       cast(0 AS DECIMAL(7, 2)) AS sales_price,
       cast(0 AS DECIMAL(7, 2)) AS profit,
       cr_return_amount AS return_amt,
       cr_net_loss AS net_loss
     FROM catalog_returns
    ) salesreturns, date_dim, catalog_page
  WHERE date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-23' AS DATE)
  AND ((cast('2000-08-23' AS DATE) + INTERVAL 14 days))
    AND page_sk = cp_catalog_page_sk
  GROUP BY cp_catalog_page_id)
  ,
    wsr AS
  ( SELECT
    web_site_id,
    sum(sales_price) AS sales,
    sum(profit) AS profit,
    sum(return_amt) AS RETURNS,
    sum(net_loss) AS profit_loss
  FROM
    (SELECT
       ws_web_site_sk AS wsr_web_site_sk,
       ws_sold_date_sk AS date_sk,
       ws_ext_sales_price AS sales_price,
       ws_net_profit AS profit,
       cast(0 AS DECIMAL(7, 2)) AS return_amt,
       cast(0 AS DECIMAL(7, 2)) AS net_loss
     FROM web_sales
     UNION ALL
     SELECT
       ws_web_site_sk AS wsr_web_site_sk,
       wr_returned_date_sk AS date_sk,
       cast(0 AS DECIMAL(7, 2)) AS sales_price,
       cast(0 AS DECIMAL(7, 2)) AS profit,
       wr_return_amt AS return_amt,
       wr_net_loss AS net_loss
     FROM web_returns
       LEFT OUTER JOIN web_sales ON
                                   (wr_item_sk = ws_item_sk
                                     AND wr_order_number = ws_order_number)
    ) salesreturns, date_dim, web_site
  WHERE date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-23' AS DATE)
  AND ((cast('2000-08-23' AS DATE) + INTERVAL 14 days))
    AND wsr_web_site_sk = web_site_sk
  GROUP BY web_site_id)
SELECT
  channel,
  id,
  sum(sales) AS sales,
  sum(returns) AS returns,
  sum(profit) AS profit
FROM
  (SELECT
     'store channel' AS channel,
     concat('store', s_store_id) AS id,
     sales,
     returns,
     (profit - profit_loss) AS profit
   FROM ssr
   UNION ALL
   SELECT
     'catalog channel' AS channel,
     concat('catalog_page', cp_catalog_page_id) AS id,
     sales,
     returns,
     (profit - profit_loss) AS profit
   FROM csr
   UNION ALL
   SELECT
     'web channel' AS channel,
     concat('web_site', web_site_id) AS id,
     sales,
     returns,
     (profit - profit_loss) AS profit
   FROM wsr
  ) x
GROUP BY ROLLUP (channel, id)
ORDER BY channel, id
LIMIT 100
