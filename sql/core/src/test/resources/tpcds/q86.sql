SELECT
  sum(ws_net_paid) AS total_sum,
  i_category,
  i_class,
  grouping(i_category) + grouping(i_class) AS lochierarchy,
  rank()
  OVER (
    PARTITION BY grouping(i_category) + grouping(i_class),
      CASE WHEN grouping(i_class) = 0
        THEN i_category END
    ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
FROM
  web_sales, date_dim d1, item
WHERE
  d1.d_month_seq BETWEEN 1200 AND 1200 + 11
    AND d1.d_date_sk = ws_sold_date_sk
    AND i_item_sk = ws_item_sk
GROUP BY ROLLUP (i_category, i_class)
ORDER BY
  lochierarchy DESC,
  CASE WHEN lochierarchy = 0
    THEN i_category END,
  rank_within_parent
LIMIT 100
