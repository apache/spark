SELECT
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  sum(ss_ext_sales_price) AS itemrevenue,
  sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price))
  OVER
  (PARTITION BY i_class) AS revenueratio
FROM
  store_sales, item, date_dim
WHERE
  ss_item_sk = i_item_sk
    AND i_category IN ('Sports', 'Books', 'Home')
    AND ss_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('1999-02-22' AS DATE)
  AND (cast('1999-02-22' AS DATE) + INTERVAL 30 days)
GROUP BY
  i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY
  i_category, i_class, i_item_id, i_item_desc, revenueratio
