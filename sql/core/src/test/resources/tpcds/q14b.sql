WITH cross_items AS
(SELECT i_item_sk ss_item_sk
  FROM item,
    (SELECT
      iss.i_brand_id brand_id,
      iss.i_class_id class_id,
      iss.i_category_id category_id
    FROM store_sales, item iss, date_dim d1
    WHERE ss_item_sk = iss.i_item_sk
      AND ss_sold_date_sk = d1.d_date_sk
      AND d1.d_year BETWEEN 1999 AND 1999 + 2
    INTERSECT
    SELECT
      ics.i_brand_id,
      ics.i_class_id,
      ics.i_category_id
    FROM catalog_sales, item ics, date_dim d2
    WHERE cs_item_sk = ics.i_item_sk
      AND cs_sold_date_sk = d2.d_date_sk
      AND d2.d_year BETWEEN 1999 AND 1999 + 2
    INTERSECT
    SELECT
      iws.i_brand_id,
      iws.i_class_id,
      iws.i_category_id
    FROM web_sales, item iws, date_dim d3
    WHERE ws_item_sk = iws.i_item_sk
      AND ws_sold_date_sk = d3.d_date_sk
      AND d3.d_year BETWEEN 1999 AND 1999 + 2) x
  WHERE i_brand_id = brand_id
    AND i_class_id = class_id
    AND i_category_id = category_id
),
    avg_sales AS
  (SELECT avg(quantity * list_price) average_sales
  FROM (SELECT
          ss_quantity quantity,
          ss_list_price list_price
        FROM store_sales, date_dim
        WHERE ss_sold_date_sk = d_date_sk AND d_year BETWEEN 1999 AND 1999 + 2
        UNION ALL
        SELECT
          cs_quantity quantity,
          cs_list_price list_price
        FROM catalog_sales, date_dim
        WHERE cs_sold_date_sk = d_date_sk AND d_year BETWEEN 1999 AND 1999 + 2
        UNION ALL
        SELECT
          ws_quantity quantity,
          ws_list_price list_price
        FROM web_sales, date_dim
        WHERE ws_sold_date_sk = d_date_sk AND d_year BETWEEN 1999 AND 1999 + 2) x)
SELECT *
FROM
  (SELECT
    'store' channel,
    i_brand_id,
    i_class_id,
    i_category_id,
    sum(ss_quantity * ss_list_price) sales,
    count(*) number_sales
  FROM store_sales, item, date_dim
  WHERE ss_item_sk IN (SELECT ss_item_sk
  FROM cross_items)
    AND ss_item_sk = i_item_sk
    AND ss_sold_date_sk = d_date_sk
    AND d_week_seq = (SELECT d_week_seq
  FROM date_dim
  WHERE d_year = 1999 + 1 AND d_moy = 12 AND d_dom = 11)
  GROUP BY i_brand_id, i_class_id, i_category_id
  HAVING sum(ss_quantity * ss_list_price) > (SELECT average_sales
  FROM avg_sales)) this_year,
  (SELECT
    'store' channel,
    i_brand_id,
    i_class_id,
    i_category_id,
    sum(ss_quantity * ss_list_price) sales,
    count(*) number_sales
  FROM store_sales, item, date_dim
  WHERE ss_item_sk IN (SELECT ss_item_sk
  FROM cross_items)
    AND ss_item_sk = i_item_sk
    AND ss_sold_date_sk = d_date_sk
    AND d_week_seq = (SELECT d_week_seq
  FROM date_dim
  WHERE d_year = 1999 AND d_moy = 12 AND d_dom = 11)
  GROUP BY i_brand_id, i_class_id, i_category_id
  HAVING sum(ss_quantity * ss_list_price) > (SELECT average_sales
  FROM avg_sales)) last_year
WHERE this_year.i_brand_id = last_year.i_brand_id
  AND this_year.i_class_id = last_year.i_class_id
  AND this_year.i_category_id = last_year.i_category_id
ORDER BY this_year.channel, this_year.i_brand_id, this_year.i_class_id, this_year.i_category_id
LIMIT 100
