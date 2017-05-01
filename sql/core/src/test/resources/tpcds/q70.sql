SELECT
  sum(ss_net_profit) AS total_sum,
  s_state,
  s_county,
  grouping(s_state) + grouping(s_county) AS lochierarchy,
  rank()
  OVER (
    PARTITION BY grouping(s_state) + grouping(s_county),
      CASE WHEN grouping(s_county) = 0
        THEN s_state END
    ORDER BY sum(ss_net_profit) DESC) AS rank_within_parent
FROM
  store_sales, date_dim d1, store
WHERE
  d1.d_month_seq BETWEEN 1200 AND 1200 + 11
    AND d1.d_date_sk = ss_sold_date_sk
    AND s_store_sk = ss_store_sk
    AND s_state IN
    (SELECT s_state
    FROM
      (SELECT
        s_state AS s_state,
        rank()
        OVER (PARTITION BY s_state
          ORDER BY sum(ss_net_profit) DESC) AS ranking
      FROM store_sales, store, date_dim
      WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
        AND d_date_sk = ss_sold_date_sk
        AND s_store_sk = ss_store_sk
      GROUP BY s_state) tmp1
    WHERE ranking <= 5)
GROUP BY ROLLUP (s_state, s_county)
ORDER BY
  lochierarchy DESC
  , CASE WHEN lochierarchy = 0
  THEN s_state END
  , rank_within_parent
LIMIT 100
