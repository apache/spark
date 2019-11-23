-- start query 79 in stream 0 using template query79.tpl
select
  c_last_name,
  c_first_name,
  substr(s_city, 1, 30),
  ss_ticket_number,
  amt,
  profit
from
  (select
    ss_ticket_number,
    ss_customer_sk,
    store.s_city,
    sum(ss_coupon_amt) amt,
    sum(ss_net_profit) profit
  from
    store_sales,
    date_dim,
    store,
    household_demographics
  where
    store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (household_demographics.hd_dep_count = 8
      or household_demographics.hd_vehicle_count > 0)
    and date_dim.d_dow = 1
     and date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
    and store.s_number_employees between 200 and 295
    and ss_sold_date_sk between 2450819 and 2451904
    -- partition key filter
    --and ss_sold_date_sk in (2450819, 2450826, 2450833, 2450840, 2450847, 2450854, 2450861, 2450868, 2450875, 2450882, 2450889,
    -- 2450896, 2450903, 2450910, 2450917, 2450924, 2450931, 2450938, 2450945, 2450952, 2450959, 2450966, 2450973, 2450980, 2450987,
    -- 2450994, 2451001, 2451008, 2451015, 2451022, 2451029, 2451036, 2451043, 2451050, 2451057, 2451064, 2451071, 2451078, 2451085,
    -- 2451092, 2451099, 2451106, 2451113, 2451120, 2451127, 2451134, 2451141, 2451148, 2451155, 2451162, 2451169, 2451176, 2451183,
    -- 2451190, 2451197, 2451204, 2451211, 2451218, 2451225, 2451232, 2451239, 2451246, 2451253, 2451260, 2451267, 2451274, 2451281,
    -- 2451288, 2451295, 2451302, 2451309, 2451316, 2451323, 2451330, 2451337, 2451344, 2451351, 2451358, 2451365, 2451372, 2451379,
    -- 2451386, 2451393, 2451400, 2451407, 2451414, 2451421, 2451428, 2451435, 2451442, 2451449, 2451456, 2451463, 2451470, 2451477,
    -- 2451484, 2451491, 2451498, 2451505, 2451512, 2451519, 2451526, 2451533, 2451540, 2451547, 2451554, 2451561, 2451568, 2451575,
    -- 2451582, 2451589, 2451596, 2451603, 2451610, 2451617, 2451624, 2451631, 2451638, 2451645, 2451652, 2451659, 2451666, 2451673,
    -- 2451680, 2451687, 2451694, 2451701, 2451708, 2451715, 2451722, 2451729, 2451736, 2451743, 2451750, 2451757, 2451764, 2451771,
    -- 2451778, 2451785, 2451792, 2451799, 2451806, 2451813, 2451820, 2451827, 2451834, 2451841, 2451848, 2451855, 2451862, 2451869,
    -- 2451876, 2451883, 2451890, 2451897, 2451904)    
  group by
    ss_ticket_number,
    ss_customer_sk,
    ss_addr_sk,
    store.s_city
  ) ms,
  customer
where
  ss_customer_sk = c_customer_sk
order by
  c_last_name,
  c_first_name,
  substr(s_city, 1, 30),
  profit 
  limit 100
-- end query 79 in stream 0 using template query79.tpl
