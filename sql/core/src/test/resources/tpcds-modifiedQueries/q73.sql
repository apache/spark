-- start query 73 in stream 0 using template query73.tpl
select
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  ss_ticket_number,
  cnt
from
  (select
    ss_ticket_number,
    ss_customer_sk,
    count(*) cnt
  from
    store_sales,
    date_dim,
    store,
    household_demographics
  where
    store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and date_dim.d_dom between 1 and 2
    and (household_demographics.hd_buy_potential = '>10000'
      or household_demographics.hd_buy_potential = 'Unknown')
    and household_demographics.hd_vehicle_count > 0
    and case when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count else null end > 1
    and date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
    and store.s_county in ('Fairfield County','Ziebach County','Bronx County','Barrow County')
    -- partition key filter
    and ss_sold_date_sk in (2450815, 2450816, 2450846, 2450847, 2450874, 2450875, 2450905, 2450906, 2450935, 2450936, 2450966, 2450967, 
                            2450996, 2450997, 2451027, 2451028, 2451058, 2451059, 2451088, 2451089, 2451119, 2451120, 2451149, 
                            2451150, 2451180, 2451181, 2451211, 2451212, 2451239, 2451240, 2451270, 2451271, 2451300, 2451301, 
                            2451331, 2451332, 2451361, 2451362, 2451392, 2451393, 2451423, 2451424, 2451453, 2451454, 2451484, 
                            2451485, 2451514, 2451515, 2451545, 2451546, 2451576, 2451577, 2451605, 2451606, 2451636, 2451637, 
                            2451666, 2451667, 2451697, 2451698, 2451727, 2451728, 2451758, 2451759, 2451789, 2451790, 2451819, 
                            2451820, 2451850, 2451851, 2451880, 2451881)    
    --and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
  group by
    ss_ticket_number,
    ss_customer_sk
  ) dj,
  customer
where
  ss_customer_sk = c_customer_sk
  and cnt between 1 and 5
order by
  cnt desc
-- end query 73 in stream 0 using template query73.tpl
