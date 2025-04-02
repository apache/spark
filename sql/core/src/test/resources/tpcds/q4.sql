WITH year_total AS (
  SELECT
    c_customer_id customer_id,
    c_first_name customer_first_name,
    c_last_name customer_last_name,
    c_preferred_cust_flag customer_preferred_cust_flag,
    c_birth_country customer_birth_country,
    c_login customer_login,
    c_email_address customer_email_address,
    d_year dyear,
    sum(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) +
      ss_ext_sales_price) / 2) year_total,
    's' sale_type
  FROM customer, store_sales, date_dim
  WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
  GROUP BY c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address,
    d_year
  UNION ALL
  SELECT
    c_customer_id customer_id,
    c_first_name customer_first_name,
    c_last_name customer_last_name,
    c_preferred_cust_flag customer_preferred_cust_flag,
    c_birth_country customer_birth_country,
    c_login customer_login,
    c_email_address customer_email_address,
    d_year dyear,
    sum((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) +
      cs_ext_sales_price) / 2)) year_total,
    'c' sale_type
  FROM customer, catalog_sales, date_dim
  WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk
  GROUP BY c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address,
    d_year
  UNION ALL
  SELECT
    c_customer_id customer_id,
    c_first_name customer_first_name,
    c_last_name customer_last_name,
    c_preferred_cust_flag customer_preferred_cust_flag,
    c_birth_country customer_birth_country,
    c_login customer_login,
    c_email_address customer_email_address,
    d_year dyear,
    sum((((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) /
      2)) year_total,
    'w' sale_type
  FROM customer, web_sales, date_dim
  WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk
  GROUP BY c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address,
    d_year)
SELECT
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name,
  t_s_secyear.customer_preferred_cust_flag,
  t_s_secyear.customer_birth_country,
  t_s_secyear.customer_login,
  t_s_secyear.customer_email_address
FROM year_total t_s_firstyear, year_total t_s_secyear, year_total t_c_firstyear,
  year_total t_c_secyear, year_total t_w_firstyear, year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_c_secyear.customer_id
  AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_c_firstyear.sale_type = 'c'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_c_secyear.sale_type = 'c'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 2001
  AND t_s_secyear.dyear = 2001 + 1
  AND t_c_firstyear.dyear = 2001
  AND t_c_secyear.dyear = 2001 + 1
  AND t_w_firstyear.dyear = 2001
  AND t_w_secyear.dyear = 2001 + 1
  AND t_s_firstyear.year_total > 0
  AND t_c_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE WHEN t_c_firstyear.year_total > 0
  THEN t_c_secyear.year_total / t_c_firstyear.year_total
      ELSE NULL END
  > CASE WHEN t_s_firstyear.year_total > 0
  THEN t_s_secyear.year_total / t_s_firstyear.year_total
    ELSE NULL END
  AND CASE WHEN t_c_firstyear.year_total > 0
  THEN t_c_secyear.year_total / t_c_firstyear.year_total
      ELSE NULL END
  > CASE WHEN t_w_firstyear.year_total > 0
  THEN t_w_secyear.year_total / t_w_firstyear.year_total
    ELSE NULL END
ORDER BY
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name,
  t_s_secyear.customer_preferred_cust_flag,
  t_s_secyear.customer_birth_country,
  t_s_secyear.customer_login,
  t_s_secyear.customer_email_address
LIMIT 100
