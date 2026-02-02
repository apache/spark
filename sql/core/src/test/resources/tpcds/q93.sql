SELECT
  ss_customer_sk,
  sum(act_sales) sumsales
FROM (SELECT
  ss_item_sk,
  ss_ticket_number,
  ss_customer_sk,
  CASE WHEN sr_return_quantity IS NOT NULL
    THEN (ss_quantity - sr_return_quantity) * ss_sales_price
  ELSE (ss_quantity * ss_sales_price) END act_sales
FROM store_sales
  LEFT OUTER JOIN store_returns
    ON (sr_item_sk = ss_item_sk AND sr_ticket_number = ss_ticket_number)
  ,
  reason
WHERE sr_reason_sk = r_reason_sk AND r_reason_desc = 'reason 28') t
GROUP BY ss_customer_sk
ORDER BY sumsales, ss_customer_sk
LIMIT 100
