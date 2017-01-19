SELECT *
FROM (
       SELECT
         i_category,
         i_class,
         i_brand,
         s_store_name,
         s_company_name,
         d_moy,
         sum(ss_sales_price) sum_sales,
         avg(sum(ss_sales_price))
         OVER
         (PARTITION BY i_category, i_brand, s_store_name, s_company_name)
         avg_monthly_sales
       FROM item, store_sales, date_dim, store
       WHERE ss_item_sk = i_item_sk AND
         ss_sold_date_sk = d_date_sk AND
         ss_store_sk = s_store_sk AND
         d_year IN (1999) AND
         ((i_category IN ('Books', 'Electronics', 'Sports') AND
           i_class IN ('computers', 'stereo', 'football'))
           OR (i_category IN ('Men', 'Jewelry', 'Women') AND
           i_class IN ('shirts', 'birdal', 'dresses')))
       GROUP BY i_category, i_class, i_brand,
         s_store_name, s_company_name, d_moy) tmp1
WHERE CASE WHEN (avg_monthly_sales <> 0)
  THEN (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales)
      ELSE NULL END > 0.1
ORDER BY sum_sales - avg_monthly_sales, s_store_name
LIMIT 100
