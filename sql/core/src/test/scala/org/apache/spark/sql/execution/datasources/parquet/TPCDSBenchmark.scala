/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object TPCDSBenchmark {
  val conf = new SparkConf()
  conf.set("spark.sql.parquet.compression.codec", "snappy")
  conf.set("spark.sql.shuffle.partitions", "4")
  conf.set("spark.driver.memory", "3g")
  conf.set("spark.executor.memory", "3g")
  conf.set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)

  val sc = new SparkContext("local[1]", "test-sql-context", conf)
  val sqlContext = new SQLContext(sc)

  // These queries a subset of the TPCDS benchmark queries and are taken from
  // https://github.com/databricks/spark-sql-perf/blob/master/src/main/scala/com/databricks/spark/
  // sql/perf/tpcds/ImpalaKitQueries.scala
  val tpcds = Seq(
    ("q19", """
              |select
              |  i_brand_id,
              |  i_brand,
              |  i_manufact_id,
              |  i_manufact,
              |  sum(ss_ext_sales_price) ext_price
              |from
              |  store_sales
              |  join item on (store_sales.ss_item_sk = item.i_item_sk)
              |  join store on (store_sales.ss_store_sk = store.s_store_sk)
              |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |  join customer on (store_sales.ss_customer_sk = customer.c_customer_sk)
              |  join customer_address on
              |    (customer.c_current_addr_sk = customer_address.ca_address_sk)
              |where
              |  ss_sold_date_sk between 2451484 and 2451513
              |  and d_moy = 11
              |  and d_year = 1999
              |  and i_manager_id = 7
              |  and substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
              |group by
              |  i_brand,
              |  i_brand_id,
              |  i_manufact_id,
              |  i_manufact
              |order by
              |  ext_price desc,
              |  i_brand,
              |  i_brand_id,
              |  i_manufact_id,
              |  i_manufact
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q19                                      1710 / 1858          8.7         114.5       1.0X
     */

    ("q27", """
              |select
              |  i_item_id,
              |  s_state,
              |  avg(ss_quantity) agg1,
              |  avg(ss_list_price) agg2,
              |  avg(ss_coupon_amt) agg3,
              |  avg(ss_sales_price) agg4
              |from
              |  store_sales
              |  join store on (store_sales.ss_store_sk = store.s_store_sk)
              |  join customer_demographics on
              |    (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
              |  join item on (store_sales.ss_item_sk = item.i_item_sk)
              |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |where
              |  ss_sold_date_sk between 2450815 and 2451179  -- partition key filter
              |  and d_year = 1998
              |  and cd_gender = 'F'
              |  and cd_marital_status = 'W'
              |  and cd_education_status = 'Primary'
              |  and s_state in ('WI', 'CA', 'TX', 'FL', 'WA', 'TN')
              |group by
              |  i_item_id,
              |  s_state
              |order by
              |  i_item_id,
              |  s_state
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q27                                      2016 / 2180          8.2         122.6       1.0X
     */

    ("q3", """
             |select
             |  dt.d_year,
             |  item.i_brand_id brand_id,
             |  item.i_brand brand,
             |  sum(ss_ext_sales_price) sum_agg
             |from
             |  store_sales
             |  join item on (store_sales.ss_item_sk = item.i_item_sk)
             |  join date_dim dt on (dt.d_date_sk = store_sales.ss_sold_date_sk)
             |where
             |  item.i_manufact_id = 436
             |  and dt.d_moy = 12
             |  and (ss_sold_date_sk between 2451149 and 2451179
             |    or ss_sold_date_sk between 2451514 and 2451544
             |    or ss_sold_date_sk between 2451880 and 2451910
             |    or ss_sold_date_sk between 2452245 and 2452275
             |    or ss_sold_date_sk between 2452610 and 2452640)
             |group by
             |  d_year,
             |  item.i_brand,
             |  item.i_brand_id
             |order by
             |  d_year,
             |  sum_agg desc,
             |  brand_id
             |limit 100
           """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q3                                       1073 / 1140         13.5          73.9       1.0X
     */

    ("q34", """
              |select
              |  c_last_name,
              |  c_first_name,
              |  c_salutation,
              |  c_preferred_cust_flag,
              |  ss_ticket_number,
              |  cnt
              |from
              |  (select
              |    ss_ticket_number,
              |    ss_customer_sk,
              |    count(*) cnt
              |  from
              |    store_sales
              |    join household_demographics on
              |      (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |  where
              |    date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
              |    and (date_dim.d_dom between 1 and 3
              |      or date_dim.d_dom between 25 and 28)
              |    and (household_demographics.hd_buy_potential = '>10000'
              |      or household_demographics.hd_buy_potential = 'unknown')
              |    and household_demographics.hd_vehicle_count > 0
              |    and (case when household_demographics.hd_vehicle_count > 0 then
              |        household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
              |      else null end) > 1.2
              |    and ss_sold_date_sk between 2450816 and 2451910 -- partition key filter
              |  group by
              |    ss_ticket_number,
              |    ss_customer_sk
              |  ) dn
              |join customer on (dn.ss_customer_sk = customer.c_customer_sk)
              |where
              |  cnt between 15 and 20
              |order by
              |  c_last_name,
              |  c_first_name,
              |  c_salutation,
              |  c_preferred_cust_flag desc,
              |  ss_ticket_number,
              |  cnt
              |limit 1000
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q34                                      1482 / 1734         10.0         100.4       1.0X
     */

    ("q42", """
              |select
              |  d_year,
              |  i_category_id,
              |  i_category,
              |  sum(ss_ext_sales_price) as total_price
              |from
              |  store_sales
              |  join item on (store_sales.ss_item_sk = item.i_item_sk)
              |  join date_dim dt on (dt.d_date_sk = store_sales.ss_sold_date_sk)
              |where
              |  item.i_manager_id = 1
              |  and dt.d_moy = 12
              |  and dt.d_year = 1998
              |  and ss_sold_date_sk between 2451149 and 2451179  -- partition key filter
              |group by
              |  d_year,
              |  i_category_id,
              |  i_category
              |order by
              |  total_price desc,
              |  d_year,
              |  i_category_id,
              |  i_category
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q42                                      1125 / 1357         12.9          77.4       1.0X
     */

    ("q43", """
              |select
              |  s_store_name,
              |  s_store_id,
              |  sum(case when (d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
              |  sum(case when (d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
              |  sum(case when (d_day_name = 'Tuesday') then
              |    ss_sales_price else null end) tue_sales,
              |  sum(case when (d_day_name = 'Wednesday') then
              |    ss_sales_price else null end) wed_sales,
              |  sum(case when (d_day_name = 'Thursday') then
              |    ss_sales_price else null end) thu_sales,
              |  sum(case when (d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
              |  sum(case when (d_day_name = 'Saturday') then
              |    ss_sales_price else null end) sat_sales
              |from
              |  store_sales
              |  join store on (store_sales.ss_store_sk = store.s_store_sk)
              |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |where
              |  s_gmt_offset = -5
              |  and d_year = 1998
              |  and ss_sold_date_sk between 2450816 and 2451179  -- partition key filter
              |group by
              |  s_store_name,
              |  s_store_id
              |order by
              |  s_store_name,
              |  s_store_id,
              |  sun_sales,
              |  mon_sales,
              |  tue_sales,
              |  wed_sales,
              |  thu_sales,
              |  fri_sales,
              |  sat_sales
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q43                                      1681 / 1985          8.6         116.1       1.0X
     */

    ("q46", """
              |select
              |  c_last_name,
              |  c_first_name,
              |  ca_city,
              |  bought_city,
              |  ss_ticket_number,
              |  amt,
              |  profit
              |from
              |  (select
              |    ss_ticket_number,
              |    ss_customer_sk,
              |    ca_city bought_city,
              |    sum(ss_coupon_amt) amt,
              |    sum(ss_net_profit) profit
              |  from
              |    store_sales
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |    join household_demographics on
              |      (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |    join customer_address on
              |      (store_sales.ss_addr_sk = customer_address.ca_address_sk)
              |  where
              |    store.s_city in ('Midway', 'Concord', 'Spring Hill', 'Brownsville', 'Greenville')
              |    and (household_demographics.hd_dep_count = 5
              |      or household_demographics.hd_vehicle_count = 3)
              |    and date_dim.d_dow in (6, 0)
              |    and date_dim.d_year in (1999, 1999 + 1, 1999 + 2)
              |      group by
              |    ss_ticket_number,
              |    ss_customer_sk,
              |    ss_addr_sk,
              |    ca_city
              |  ) dn
              |  join customer on (dn.ss_customer_sk = customer.c_customer_sk)
              |  join customer_address current_addr on
              |    (customer.c_current_addr_sk = current_addr.ca_address_sk)
              |where
              |  current_addr.ca_city <> bought_city
              |order by
              |  c_last_name,
              |  c_first_name,
              |  ca_city,
              |  bought_city,
              |  ss_ticket_number
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q46                                      2948 / 3218          5.1         196.1       1.0X
     */

    ("q52", """
              |select
              |  d_year,
              |  i_brand_id,
              |  i_brand,
              |  sum(ss_ext_sales_price) ext_price
              |from
              |  store_sales
              |  join item on (store_sales.ss_item_sk = item.i_item_sk)
              |  join date_dim dt on (store_sales.ss_sold_date_sk = dt.d_date_sk)
              |where
              |  i_manager_id = 1
              |  and d_moy = 12
              |  and d_year = 1998
              |  and ss_sold_date_sk between 2451149 and 2451179 -- partition key filter
              |group by
              |  d_year,
              |  i_brand,
              |  i_brand_id
              |order by
              |  d_year,
              |  ext_price desc,
              |  i_brand_id
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q52                                      1099 / 1228         13.2          75.7       1.0X
     */

    ("q53", """
              |select
              |  *
              |from
              |  (select
              |    i_manufact_id,
              |    sum(ss_sales_price) sum_sales
              |  from
              |    store_sales
              |    join item on (store_sales.ss_item_sk = item.i_item_sk)
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |  where
              |    ss_sold_date_sk between 2451911 and 2452275 -- partition key filter
              |    and d_month_seq in(1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5,
              |      1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
              |    and (
              |         (i_category in('Books', 'Children', 'Electronics')
              |           and i_class in('personal', 'portable', 'reference', 'self-help')
              |           and i_brand in('scholaramalgamalg #14', 'scholaramalgamalg #7',
              |             'exportiunivamalg #9', 'scholaramalgamalg #9')
              |         )
              |         or
              |         (i_category in('Women', 'Music', 'Men')
              |           and i_class in('accessories', 'classical', 'fragrances', 'pants')
              |           and i_brand in('amalgimporto #1', 'edu packscholar #1',
              |             'exportiimporto #1', 'importoamalg #1')
              |         )
              |       )
              |  group by
              |    i_manufact_id,
              |    d_qoy
              |  ) tmp1
              |order by
              |  sum_sales,
              |  i_manufact_id
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q53                                       968 / 1020         15.0          66.6       1.0X
     */

    ("q55", """
              |select
              |  i_brand_id,
              |  i_brand,
              |  sum(ss_ext_sales_price) ext_price
              |from
              |  store_sales
              |  join item on (store_sales.ss_item_sk = item.i_item_sk)
              |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |where
              |  i_manager_id = 36
              |  and d_moy = 12
              |  and d_year = 2001
              |  and ss_sold_date_sk between 2452245 and 2452275 -- partition key filter
              |group by
              |  i_brand,
              |  i_brand_id
              |order by
              |  ext_price desc,
              |  i_brand_id
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q55                                      1002 / 1020         14.5          69.0       1.0X
     */

    ("q59", """
              |select
              |  s_store_name1,
              |  s_store_id1,
              |  d_week_seq1,
              |  sun_sales1 / sun_sales2,
              |  mon_sales1 / mon_sales2,
              |  tue_sales1 / tue_sales2,
              |  wed_sales1 / wed_sales2,
              |  thu_sales1 / thu_sales2,
              |  fri_sales1 / fri_sales2,
              |  sat_sales1 / sat_sales2
              |from
              |  (select
              |    s_store_name s_store_name1,
              |    wss.d_week_seq d_week_seq1,
              |    s_store_id s_store_id1,
              |    sun_sales sun_sales1,
              |    mon_sales mon_sales1,
              |    tue_sales tue_sales1,
              |    wed_sales wed_sales1,
              |    thu_sales thu_sales1,
              |    fri_sales fri_sales1,
              |    sat_sales sat_sales1
              |  from
              |    (select
              |      d_week_seq,
              |      ss_store_sk,
              |      sum(case when(d_day_name = 'Sunday') then
              |        ss_sales_price else null end) sun_sales,
              |      sum(case when(d_day_name = 'Monday') then
              |        ss_sales_price else null end) mon_sales,
              |      sum(case when(d_day_name = 'Tuesday') then
              |        ss_sales_price else null end) tue_sales,
              |      sum(case when(d_day_name = 'Wednesday') then
              |        ss_sales_price else null end) wed_sales,
              |      sum(case when(d_day_name = 'Thursday') then
              |        ss_sales_price else null end) thu_sales,
              |      sum(case when(d_day_name = 'Friday') then
              |        ss_sales_price else null end) fri_sales,
              |      sum(case when(d_day_name = 'Saturday') then
              |        ss_sales_price else null end) sat_sales
              |    from
              |      store_sales
              |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |    where
              |      ss_sold_date_sk between 2451088 and 2451452
              |    group by
              |      d_week_seq,
              |      ss_store_sk
              |    ) wss
              |    join store on (wss.ss_store_sk = store.s_store_sk)
              |    join date_dim d on (wss.d_week_seq = d.d_week_seq)
              |  where
              |    d_month_seq between 1185 and 1185 + 11
              |  ) y
              |  join
              |  (select
              |    s_store_name s_store_name2,
              |    wss.d_week_seq d_week_seq2,
              |    s_store_id s_store_id2,
              |    sun_sales sun_sales2,
              |    mon_sales mon_sales2,
              |    tue_sales tue_sales2,
              |    wed_sales wed_sales2,
              |    thu_sales thu_sales2,
              |    fri_sales fri_sales2,
              |    sat_sales sat_sales2
              |  from
              |    (select
              |      d_week_seq,
              |      ss_store_sk,
              |      sum(case when(d_day_name = 'Sunday') then
              |        ss_sales_price else null end) sun_sales,
              |      sum(case when(d_day_name = 'Monday') then
              |        ss_sales_price else null end) mon_sales,
              |      sum(case when(d_day_name = 'Tuesday') then
              |        ss_sales_price else null end) tue_sales,
              |      sum(case when(d_day_name = 'Wednesday') then
              |        ss_sales_price else null end) wed_sales,
              |      sum(case when(d_day_name = 'Thursday') then
              |        ss_sales_price else null end) thu_sales,
              |      sum(case when(d_day_name = 'Friday') then
              |        ss_sales_price else null end) fri_sales,
              |      sum(case when(d_day_name = 'Saturday') then
              |        ss_sales_price else null end) sat_sales
              |    from
              |      store_sales
              |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |    where
              |      ss_sold_date_sk between 2451088 and 2451452
              |    group by
              |      d_week_seq,
              |      ss_store_sk
              |    ) wss
              |    join store on (wss.ss_store_sk = store.s_store_sk)
              |    join date_dim d on (wss.d_week_seq = d.d_week_seq)
              |  where
              |    d_month_seq between 1185 + 12 and 1185 + 23
              |  ) x
              |  on (y.s_store_id1 = x.s_store_id2)
              |where
              |  d_week_seq1 = d_week_seq2 - 52
              |order by
              |  s_store_name1,
              |  s_store_id1,
              |  d_week_seq1
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q59                                      1624 / 1663         17.9          55.8       1.0X
     */

    ("q63", """
              |select
              |  *
              |from
              |  (select
              |    i_manager_id,
              |    sum(ss_sales_price) sum_sales
              |  from
              |    store_sales
              |    join item on (store_sales.ss_item_sk = item.i_item_sk)
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |  where
              |    ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
              |    and d_month_seq in (1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5,
              |      1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
              |    and (
              |          (i_category in('Books', 'Children', 'Electronics')
              |            and i_class in('personal', 'portable', 'refernece', 'self-help')
              |            and i_brand in('scholaramalgamalg #14', 'scholaramalgamalg #7',
              |              'exportiunivamalg #9', 'scholaramalgamalg #9')
              |          )
              |          or
              |          (i_category in('Women', 'Music', 'Men')
              |            and i_class in('accessories', 'classical', 'fragrances', 'pants')
              |            and i_brand in('amalgimporto #1', 'edu packscholar #1',
              |              'exportiimporto #1', 'importoamalg #1')
              |          )
              |        )
              |  group by
              |    i_manager_id,
              |    d_moy
              |  ) tmp1
              |order by
              |  i_manager_id,
              |  sum_sales
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q63                                       979 / 1006         14.8          67.4       1.0X
     */

    ("q65", """
              |select
              |  s_store_name,
              |  i_item_desc,
              |  sc.revenue,
              |  i_current_price,
              |  i_wholesale_cost,
              |  i_brand
              |from
              |  (select
              |    ss_store_sk,
              |    ss_item_sk,
              |    sum(ss_sales_price) as revenue
              |  from
              |    store_sales
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |  where
              |    ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
              |    and d_month_seq between 1212 and 1212 + 11
              |  group by
              |    ss_store_sk,
              |    ss_item_sk
              |  ) sc
              |  join item on (sc.ss_item_sk = item.i_item_sk)
              |  join store on (sc.ss_store_sk = store.s_store_sk)
              |  join
              |  (select
              |    ss_store_sk,
              |    avg(revenue) as ave
              |  from
              |    (select
              |      ss_store_sk,
              |      ss_item_sk,
              |      sum(ss_sales_price) as revenue
              |    from
              |      store_sales
              |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |    where
              |      ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
              |      and d_month_seq between 1212 and 1212 + 11
              |    group by
              |      ss_store_sk,
              |      ss_item_sk
              |    ) sa
              |  group by
              |    ss_store_sk
              |  ) sb on (sc.ss_store_sk = sb.ss_store_sk) -- 676 rows
              |where
              |  sc.revenue <= 0.1 * sb.ave
              |order by
              |  s_store_name,
              |  i_item_desc
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q65                                      7770 / 8097          3.7         267.9       1.0X
     */

    ("q68", """
              |select
              |  c_last_name,
              |  c_first_name,
              |  ca_city,
              |  bought_city,
              |  ss_ticket_number,
              |  extended_price,
              |  extended_tax,
              |  list_price
              |from
              |  (select
              |    ss_ticket_number,
              |    ss_customer_sk,
              |    ca_city bought_city,
              |    sum(ss_ext_sales_price) extended_price,
              |    sum(ss_ext_list_price) list_price,
              |    sum(ss_ext_tax) extended_tax
              |  from
              |    store_sales
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |    join household_demographics on
              |      (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |    join customer_address on
              |      (store_sales.ss_addr_sk = customer_address.ca_address_sk)
              |  where
              |    store.s_city in('Midway', 'Fairview')
              |    --and date_dim.d_dom between 1 and 2
              |    --and date_dim.d_year in(1999, 1999 + 1, 1999 + 2)
              |    -- and ss_date between '1999-01-01' and '2001-12-31'
              |    -- and dayofmonth(ss_date) in (1,2)
              |        and (household_demographics.hd_dep_count = 5
              |      or household_demographics.hd_vehicle_count = 3)
              |    and d_date between '1999-01-01' and '1999-03-31'
              |    and ss_sold_date_sk between 2451180 and 2451269
              |    -- partition key filter (3 months)
              |  group by
              |    ss_ticket_number,
              |    ss_customer_sk,
              |    ss_addr_sk,
              |    ca_city
              |  ) dn
              |  join customer on (dn.ss_customer_sk = customer.c_customer_sk)
              |  join customer_address current_addr on
              |    (customer.c_current_addr_sk = current_addr.ca_address_sk)
              |where
              |  current_addr.ca_city <> bought_city
              |order by
              |  c_last_name,
              |  ss_ticket_number
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q68                                      3105 / 3405          4.8         206.5       1.0X
     */

    ("q7", """
             |select
             |  i_item_id,
             |  avg(ss_quantity) agg1,
             |  avg(ss_list_price) agg2,
             |  avg(ss_coupon_amt) agg3,
             |  avg(ss_sales_price) agg4
             |from
             |  store_sales
             |  join customer_demographics on
             |    (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
             |  join item on (store_sales.ss_item_sk = item.i_item_sk)
             |  join promotion on (store_sales.ss_promo_sk = promotion.p_promo_sk)
             |  join date_dim on (ss_sold_date_sk = d_date_sk)
             |where
             |  cd_gender = 'F'
             |  and cd_marital_status = 'W'
             |  and cd_education_status = 'Primary'
             |  and (p_channel_email = 'N'
             |    or p_channel_event = 'N')
             |  and d_year = 1998
             |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
             |group by
             |  i_item_id
             |order by
             |  i_item_id
             |limit 100
           """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q7                                       2042 / 2333          8.1         124.2       1.0X
     */

    ("q73", """
              |select
              |  c_last_name,
              |  c_first_name,
              |  c_salutation,
              |  c_preferred_cust_flag,
              |  ss_ticket_number,
              |  cnt
              |from
              |  (select
              |    ss_ticket_number,
              |    ss_customer_sk,
              |    count(*) cnt
              |  from
              |    store_sales
              |    join household_demographics on
              |      (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |    -- join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |  where
              |    store.s_county in
              |      ('Williamson County','Franklin Parish','Bronx County','Orange County')
              |    -- and date_dim.d_dom between 1 and 2
              |    -- and date_dim.d_year in(1998, 1998 + 1, 1998 + 2)
              |    -- and ss_date between '1999-01-01' and '2001-12-02'
              |    -- and dayofmonth(ss_date) in (1,2)
              |    -- partition key filter
              |    -- and ss_sold_date_sk in (2450816, 2450846, 2450847, 2450874, 2450875, 2450905,
              |    --                         2450906, 2450935, 2450936, 2450966, 2450967,
              |    --                         2450996, 2450997, 2451027, 2451028, 2451058, 2451059,
              |    --                         2451088, 2451089, 2451119, 2451120, 2451149,
              |    --                         2451150, 2451180, 2451181, 2451211, 2451212, 2451239,
              |    --                         2451240, 2451270, 2451271, 2451300, 2451301,
              |    --                         2451331, 2451332, 2451361, 2451362, 2451392, 2451393,
              |    --                         2451423, 2451424, 2451453, 2451454, 2451484,
              |    --                         2451485, 2451514, 2451515, 2451545, 2451546, 2451576,
              |    --                         2451577, 2451605, 2451606, 2451636, 2451637,
              |    --                         2451666, 2451667, 2451697, 2451698, 2451727, 2451728,
              |    --                         2451758, 2451759, 2451789, 2451790, 2451819,
              |    --                         2451820, 2451850, 2451851, 2451880, 2451881)
              |    and (household_demographics.hd_buy_potential = '>10000'
              |      or household_demographics.hd_buy_potential = 'unknown')
              |    and household_demographics.hd_vehicle_count > 0
              |    and case when household_demographics.hd_vehicle_count > 0 then
              |        household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
              |      else null end > 1
              |    and ss_sold_date_sk between 2451180 and 2451269
              |    -- partition key filter (3 months)
              |  group by
              |    ss_ticket_number,
              |    ss_customer_sk
              |  ) dj
              |  join customer on (dj.ss_customer_sk = customer.c_customer_sk)
              |where
              |  cnt between 1 and 5
              |order by
              |  cnt desc
              |limit 1000
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q73                                      1124 / 1221         13.1          76.5       1.0X
     */

    ("q79", """
              |select
              |  c_last_name,
              |  c_first_name,
              |  substr(s_city, 1, 30) as city,
              |  ss_ticket_number,
              |  amt,
              |  profit
              |from
              |  (select
              |    ss_ticket_number,
              |    ss_customer_sk,
              |    s_city,
              |    sum(ss_coupon_amt) amt,
              |    sum(ss_net_profit) profit
              |  from
              |    store_sales
              |    join household_demographics on
              |      (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |  where
              |    store.s_number_employees between 200 and 295
              |    and (household_demographics.hd_dep_count = 8
              |      or household_demographics.hd_vehicle_count > 0)
              |    and date_dim.d_dow = 1
              |    and date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
              |    -- and ss_date between '1998-01-01' and '2000-12-25'
              |    -- 156 days
              |  and d_date between '1999-01-01' and '1999-03-31'
              |  and ss_sold_date_sk between 2451180 and 2451269  -- partition key filter
              |  group by
              |    ss_ticket_number,
              |    ss_customer_sk,
              |    ss_addr_sk,
              |    s_city
              |  ) ms
              |  join customer on (ms.ss_customer_sk = customer.c_customer_sk)
              |order by
              |  c_last_name,
              |  c_first_name,
              |  city,
              |  profit
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q79                                      2029 / 2488          7.3         137.5       1.0X
     */

      ("q8",
        """
          |select  s_store_name
          |      ,sum(ss_net_profit)
          | from store_sales
          |     ,date_dim
          |     ,store,
          |     (select distinct a01.ca_zip
          |     from
          |     (SELECT substr(ca_zip,1,5) ca_zip
          |      FROM customer_address
          |      WHERE substr(ca_zip,1,5) IN ('89436', '30868', '65085', '22977', '83927', '77557',
          |      '58429', '40697', '80614', '10502', '32779',
          |      '91137', '61265', '98294', '17921', '18427', '21203', '59362', '87291', '84093',
          |      '21505', '17184', '10866', '67898', '25797',
          |      '28055', '18377', '80332', '74535', '21757', '29742', '90885', '29898', '17819',
          |      '40811', '25990', '47513', '89531', '91068',
          |      '10391', '18846', '99223', '82637', '41368', '83658', '86199', '81625', '26696',
          |      '89338', '88425', '32200', '81427', '19053',
          |      '77471', '36610', '99823', '43276', '41249', '48584', '83550', '82276', '18842',
          |      '78890', '14090', '38123', '40936', '34425',
          |      '19850', '43286', '80072', '79188', '54191', '11395', '50497', '84861', '90733',
          |      '21068', '57666', '37119', '25004', '57835',
          |      '70067', '62878', '95806', '19303', '18840', '19124', '29785', '16737', '16022',
          |      '49613', '89977', '68310', '60069', '98360',
          |      '48649', '39050', '41793', '25002', '27413', '39736', '47208', '16515', '94808',
          |      '57648', '15009', '80015', '42961', '63982',
          |      '21744', '71853', '81087', '67468', '34175', '64008', '20261', '11201', '51799',
          |      '48043', '45645', '61163', '48375', '36447',
          |      '57042', '21218', '41100', '89951', '22745', '35851', '83326', '61125', '78298',
          |      '80752', '49858', '52940', '96976', '63792',
          |      '11376', '53582', '18717', '90226', '50530', '94203', '99447', '27670', '96577',
          |      '57856', '56372', '16165', '23427', '54561',
          |      '28806', '44439', '22926', '30123', '61451', '92397', '56979', '92309', '70873',
          |      '13355', '21801', '46346', '37562', '56458',
          |      '28286', '47306', '99555', '69399', '26234', '47546', '49661', '88601', '35943',
          |      '39936', '25632', '24611', '44166', '56648',
          |      '30379', '59785', '11110', '14329', '93815', '52226', '71381', '13842', '25612',
          |      '63294', '14664', '21077', '82626', '18799',
          |      '60915', '81020', '56447', '76619', '11433', '13414', '42548', '92713', '70467',
          |      '30884', '47484', '16072', '38936', '13036',
          |      '88376', '45539', '35901', '19506', '65690', '73957', '71850', '49231', '14276',
          |      '20005', '18384', '76615', '11635', '38177',
          |      '55607', '41369', '95447', '58581', '58149', '91946', '33790', '76232', '75692',
          |      '95464', '22246', '51061', '56692', '53121',
          |      '77209', '15482', '10688', '14868', '45907', '73520', '72666', '25734', '17959',
          |      '24677', '66446', '94627', '53535', '15560',
          |      '41967', '69297', '11929', '59403', '33283', '52232', '57350', '43933', '40921',
          |      '36635', '10827', '71286', '19736', '80619',
          |      '25251', '95042', '15526', '36496', '55854', '49124', '81980', '35375', '49157',
          |      '63512', '28944', '14946', '36503', '54010',
          |      '18767', '23969', '43905', '66979', '33113', '21286', '58471', '59080', '13395',
          |      '79144', '70373', '67031', '38360', '26705',
          |      '50906', '52406', '26066', '73146', '15884', '31897', '30045', '61068', '45550',
          |      '92454', '13376', '14354', '19770', '22928',
          |      '97790', '50723', '46081', '30202', '14410', '20223', '88500', '67298', '13261',
          |      '14172', '81410', '93578', '83583', '46047',
          |      '94167', '82564', '21156', '15799', '86709', '37931', '74703', '83103', '23054',
          |      '70470', '72008', '35709', '91911', '69998',
          |      '20961', '70070', '63197', '54853', '88191', '91830', '49521', '19454', '81450',
          |      '89091', '62378', '31904', '61869', '51744',
          |      '36580', '85778', '36871', '48121', '28810', '83712', '45486', '67393', '26935',
          |      '42393', '20132', '55349', '86057', '21309',
          |      '80218', '10094', '11357', '48819', '39734', '40758', '30432', '21204', '29467',
          |      '30214', '61024', '55307', '74621', '11622',
          |      '68908', '33032', '52868', '99194', '99900', '84936', '69036', '99149', '45013',
          |      '32895', '59004', '32322', '14933', '32936',
          |      '33562', '72550', '27385', '58049', '58200', '16808', '21360', '32961', '18586',
          |      '79307', '15492')) a01
          |     inner join
          |     (select ca_zip
          |      from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt
          |            FROM customer_address, customer
          |            WHERE ca_address_sk = c_current_addr_sk and
          |                  c_preferred_cust_flag='Y'
          |            group by ca_zip
          |            having count(*) > 10)A1
          |      ) b11
          |      on (a01.ca_zip = b11.ca_zip )) A2
          | where ss_store_sk = s_store_sk
          |  and ss_sold_date_sk = d_date_sk
          |  and ss_sold_date_sk between 2451271 and 2451361
          |  and d_qoy = 2 and d_year = 1999
          |  and (substr(s_zip,1,2) = substr(a2.ca_zip,1,2))
          | group by s_store_name
          | order by s_store_name
          |limit 100
        """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q8                                       1737 / 2197          8.7         115.6       1.0X
     */

      ("q82", """
                |select
                |  i_item_id,
                |  i_item_desc,
                |  i_current_price
                |from
                |  store_sales
                |  join item on (store_sales.ss_item_sk = item.i_item_sk)
                |  join inventory on (item.i_item_sk = inventory.inv_item_sk)
                |  join date_dim on (inventory.inv_date_sk = date_dim.d_date_sk)
                |where
                |  i_current_price between 30 and 30 + 30
                |  and i_manufact_id in (437, 129, 727, 663)
                |  and inv_quantity_on_hand between 100 and 500
                |group by
                |  i_item_id,
                |  i_item_desc,
                |  i_current_price
                |order by
                |  i_item_id
                |limit 100
              """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q82                                     9399 / 10245          6.8         147.2       1.0X
     */

    ("q89", """
              |select
              |  *
              |from
              |  (select
              |    i_category,
              |    i_class,
              |    i_brand,
              |    s_store_name,
              |    s_company_name,
              |    d_moy,
              |    sum(ss_sales_price) sum_sales
              |  from
              |    store_sales
              |    join item on (store_sales.ss_item_sk = item.i_item_sk)
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |  where
              |    ss_sold_date_sk between 2451545 and 2451910  -- partition key filter
              |    and d_year in (2000)
              |    and ((i_category in('Home', 'Books', 'Electronics')
              |          and i_class in('wallpaper', 'parenting', 'musical'))
              |        or (i_category in('Shoes', 'Jewelry', 'Men')
              |            and i_class in('womens', 'birdal', 'pants'))
              |        )
              |  group by
              |    i_category,
              |    i_class,
              |    i_brand,
              |    s_store_name,
              |    s_company_name,
              |    d_moy
              |  ) tmp1
              |order by
              |  sum_sales,
              |  s_store_name
              |limit 100
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q89                                      1122 / 1274         12.9          77.2       1.0X
     */

    ("q98", """
              |select
              |  i_item_desc,
              |  i_category,
              |  i_class,
              |  i_current_price,
              |  sum(ss_ext_sales_price) as itemrevenue
              |from
              |  store_sales
              |  join item on (store_sales.ss_item_sk = item.i_item_sk)
              |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |where
              |  ss_sold_date_sk between 2451911 and 2451941
              |  -- partition key filter (1 calendar month)
              |  and d_date between '2001-01-01' and '2001-01-31'
              |  and i_category in('Jewelry', 'Sports', 'Books')
              |group by
              |  i_item_id,
              |  i_item_desc,
              |  i_category,
              |  i_class,
              |  i_current_price
              |order by
              |  i_category,
              |  i_class,
              |  i_item_id,
              |  i_item_desc
              |  -- revenueratio
              |limit 1000
            """.stripMargin),

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    q98                                      1235 / 1542         11.8          85.0       1.0X
     */

    ("ss_max", """
                 |select
                 |  count(*) as total,
                 |  max(ss_sold_date_sk) as max_ss_sold_date_sk,
                 |  max(ss_sold_time_sk) as max_ss_sold_time_sk,
                 |  max(ss_item_sk) as max_ss_item_sk,
                 |  max(ss_customer_sk) as max_ss_customer_sk,
                 |  max(ss_cdemo_sk) as max_ss_cdemo_sk,
                 |  max(ss_hdemo_sk) as max_ss_hdemo_sk,
                 |  max(ss_addr_sk) as max_ss_addr_sk,
                 |  max(ss_store_sk) as max_ss_store_sk,
                 |  max(ss_promo_sk) as max_ss_promo_sk
                 |from store_sales
               """.stripMargin)

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    TPCDS Snappy (scale = 5):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    ss_max                                   2305 / 2731          6.2         160.0       1.0X
     */

  ).toArray

  val tables = Seq("customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "inventory", "item", "promotion", "store", "catalog_sales",
    "web_sales", "store_sales")

  def setupTables(dataLocation: String): Map[String, Long] = {
    tables.map { tableName =>
      sqlContext.read.parquet(s"$dataLocation/$tableName").registerTempTable(tableName)
      tableName -> sqlContext.table(tableName).count()
    }.toMap
  }

  def tpcdsAll(dataLocation: String): Unit = {
    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")
    val tableSizes = setupTables(dataLocation)
    sqlContext.conf.setConfString(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sqlContext.conf.setConfString(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    tpcds.filter(q => q._1 != "").foreach {
      case (name: String, query: String) =>
        val numRows = sqlContext.sql(query).queryExecution.logical.map {
          case ur@UnresolvedRelation(t: TableIdentifier, _) =>
            tableSizes.getOrElse(t.table, throw new RuntimeException(s"${t.table} not found."))
          case _ => 0L
        }.sum
        val benchmark = new Benchmark("TPCDS Snappy (scale = 5)", numRows, 5)
        benchmark.addCase(name) { i =>
          sqlContext.sql(query).collect()
        }
        benchmark.run()
    }
  }

  def main(args: Array[String]): Unit = {

    // In order to run this benchmark, please follow the instructions at
    // https://github.com/databricks/spark-sql-perf/blob/master/README.md to generate the TPCDS data
    // locally (preferably with a scale factor of 5 for benchmarking). Thereafter, the value of
    // dataLocation below needs to be set to the location where the generated data is stored.
    val dataLocation = ""

    tpcdsAll(dataLocation)
  }
}
