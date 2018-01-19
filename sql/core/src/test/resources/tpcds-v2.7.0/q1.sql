with customer_total_return as
 (select sr_customer_sk as ctr_customer_sk
        ,sr_store_sk as ctr_store_sk
        ,sum(SR_FEE) as ctr_total_return
  from store_returns
      ,date_dim
  where sr_returned_date_sk = d_date_sk and
        d_year =2000
  group by sr_customer_sk
          ,sr_store_sk)
select  c_customer_id
from customer_total_return ctr1
    ,store
    ,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk) and
      s_store_sk = ctr1.ctr_store_sk and
      s_state = 'TN' and
      ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100
