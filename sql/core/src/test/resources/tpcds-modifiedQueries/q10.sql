-- start query 10 in stream 0 using template query10.tpl
with 
v1 as (
  select 
     ws_bill_customer_sk as customer_sk
  from web_sales,
       date_dim
  where ws_sold_date_sk = d_date_sk
  and d_year = 2002
  and d_moy between 4 and 4+3
  union all
  select 
    cs_ship_customer_sk as customer_sk
  from catalog_sales,
       date_dim 
  where cs_sold_date_sk = d_date_sk
  and d_year = 2002
  and d_moy between 4 and 4+3
),
v2 as (
  select 
    ss_customer_sk as customer_sk
  from store_sales,
       date_dim
  where ss_sold_date_sk = d_date_sk
  and d_year = 2002
  and d_moy between 4 and 4+3 
)
select
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3,
  cd_dep_count,
  count(*) cnt4,
  cd_dep_employed_count,
  count(*) cnt5,
  cd_dep_college_count,
  count(*) cnt6
from customer c
join customer_address ca on (c.c_current_addr_sk = ca.ca_address_sk)
join customer_demographics on (cd_demo_sk = c.c_current_cdemo_sk) 
left semi join v1 on (v1.customer_sk = c.c_customer_sk) 
left semi join v2 on (v2.customer_sk = c.c_customer_sk)
where 
  ca_county in ('Walker County','Richland County','Gaines County','Douglas County','Dona Ana County')
group by 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
order by 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
limit 100
-- end query 10 in stream 0 using template query10.tpl
