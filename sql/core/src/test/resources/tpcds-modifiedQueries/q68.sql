-- start query 68 in stream 0 using template query68.tpl
-- changed to match exact same partitions in original query
select
  c_last_name,
  c_first_name,
  ca_city,
  bought_city,
  ss_ticket_number,
  extended_price,
  extended_tax,
  list_price
from
  (select
    ss_ticket_number,
    ss_customer_sk,
    ca_city bought_city,
    sum(ss_ext_sales_price) extended_price,
    sum(ss_ext_list_price) list_price,
    sum(ss_ext_tax) extended_tax
  from
    store_sales,
    date_dim,
    store,
    household_demographics,
    customer_address
  where
    store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and date_dim.d_dom between 1 and 2
    and (household_demographics.hd_dep_count = 5
      or household_demographics.hd_vehicle_count = 3)
    and date_dim.d_year in (1999, 1999 + 1, 1999 + 2)
    and store.s_city in ('Midway', 'Fairview')
    -- partition key filter
    and ss_sold_date_sk in (2451180, 2451181, 2451211, 2451212, 2451239, 2451240, 2451270, 2451271, 2451300, 2451301, 2451331, 
                             2451332, 2451361, 2451362, 2451392, 2451393, 2451423, 2451424, 2451453, 2451454, 2451484, 2451485, 
                             2451514, 2451515, 2451545, 2451546, 2451576, 2451577, 2451605, 2451606, 2451636, 2451637, 2451666, 
                             2451667, 2451697, 2451698, 2451727, 2451728, 2451758, 2451759, 2451789, 2451790, 2451819, 2451820, 
                             2451850, 2451851, 2451880, 2451881, 2451911, 2451912, 2451942, 2451943, 2451970, 2451971, 2452001, 
                             2452002, 2452031, 2452032, 2452062, 2452063, 2452092, 2452093, 2452123, 2452124, 2452154, 2452155, 
                             2452184, 2452185, 2452215, 2452216, 2452245, 2452246) 
    --and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
    --and d_date between '1999-01-01' and '1999-03-31'
  group by
    ss_ticket_number,
    ss_customer_sk,
    ss_addr_sk,
    ca_city
  ) dn,
  customer,
  customer_address current_addr
where
  ss_customer_sk = c_customer_sk
  and customer.c_current_addr_sk = current_addr.ca_address_sk
  and current_addr.ca_city <> bought_city
order by
  c_last_name,
  ss_ticket_number
limit 100
-- end query 68 in stream 0 using template query68.tpl
