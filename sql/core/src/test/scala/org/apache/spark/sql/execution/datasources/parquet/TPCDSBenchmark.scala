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

import java.io.File

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.util.{Benchmark, Utils}

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
  conf.set("spark.sql.autoBroadcastJoinThreshold", (100 * 1024 * 1024).toString)

  val sc = new SparkContext("local[1]", "test-sql-context", conf)
  val sqlContext = new SQLContext(sc)

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(sqlContext.dropTempTable)
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(sqlContext.conf.getConfString(key)).toOption)
    (keys, values).zipped.foreach(sqlContext.conf.setConfString)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => sqlContext.conf.setConfString(key, value)
        case (key, None) => sqlContext.conf.unsetConf(key)
      }
    }
  }

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
              |  join customer_address on (customer.c_current_addr_sk = customer_address.ca_address_sk)
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
              |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
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
              |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |  where
              |    date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
              |    and (date_dim.d_dom between 1 and 3
              |      or date_dim.d_dom between 25 and 28)
              |    and (household_demographics.hd_buy_potential = '>10000'
              |      or household_demographics.hd_buy_potential = 'unknown')
              |    and household_demographics.hd_vehicle_count > 0
              |    and (case when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count else null end) > 1.2
              |     and ss_sold_date_sk between 2450816 and 2451910 -- partition key filter
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

    ("q43", """
              |select
              |  s_store_name,
              |  s_store_id,
              |  sum(case when (d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
              |  sum(case when (d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
              |  sum(case when (d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
              |  sum(case when (d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
              |  sum(case when (d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
              |  sum(case when (d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
              |  sum(case when (d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
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
              |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |    join customer_address on (store_sales.ss_addr_sk = customer_address.ca_address_sk)
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
              |  join customer_address current_addr on (customer.c_current_addr_sk = current_addr.ca_address_sk)
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
              |    and d_month_seq in(1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
              |    and (
              |  	    	(i_category in('Books', 'Children', 'Electronics')
              |    		    and i_class in('personal', 'portable', 'reference', 'self-help')
              |    		    and i_brand in('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
              |  		    )
              |  		    or
              |  		    (i_category in('Women', 'Music', 'Men')
              |    		    and i_class in('accessories', 'classical', 'fragrances', 'pants')
              |    		    and i_brand in('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
              |  		    )
              |  	    )
              |  group by
              |    i_manufact_id,
              |    d_qoy
              |  ) tmp1
              |order by
              |  sum_sales,
              |  i_manufact_id
              |limit 100
            """.stripMargin),

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
              |      sum(case when(d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
              |      sum(case when(d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
              |      sum(case when(d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
              |      sum(case when(d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
              |      sum(case when(d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
              |      sum(case when(d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
              |      sum(case when(d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
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
              |      sum(case when(d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
              |      sum(case when(d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
              |      sum(case when(d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
              |      sum(case when(d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
              |      sum(case when(d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
              |      sum(case when(d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
              |      sum(case when(d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
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
              |    and d_month_seq in (1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
              |    and (
              |          (i_category in('Books', 'Children', 'Electronics')
              |            and i_class in('personal', 'portable', 'refernece', 'self-help')
              |            and i_brand in('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
              |          )
              |          or
              |          (i_category in('Women', 'Music', 'Men')
              |            and i_class in('accessories', 'classical', 'fragrances', 'pants')
              |            and i_brand in('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
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
              |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
              |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |    join customer_address on (store_sales.ss_addr_sk = customer_address.ca_address_sk)
              |  where
              |    store.s_city in('Midway', 'Fairview')
              |    --and date_dim.d_dom between 1 and 2
              |    --and date_dim.d_year in(1999, 1999 + 1, 1999 + 2)
              |    -- and ss_date between '1999-01-01' and '2001-12-31'
              |    -- and dayofmonth(ss_date) in (1,2)
              |        and (household_demographics.hd_dep_count = 5
              |      or household_demographics.hd_vehicle_count = 3)
              |    and d_date between '1999-01-01' and '1999-03-31'
              |    and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
              |  group by
              |    ss_ticket_number,
              |    ss_customer_sk,
              |    ss_addr_sk,
              |    ca_city
              |  ) dn
              |  join customer on (dn.ss_customer_sk = customer.c_customer_sk)
              |  join customer_address current_addr on (customer.c_current_addr_sk = current_addr.ca_address_sk)
              |where
              |  current_addr.ca_city <> bought_city
              |order by
              |  c_last_name,
              |  ss_ticket_number
              |limit 100
            """.stripMargin),

    ("q7", """
             |select
             |  i_item_id,
             |  avg(ss_quantity) agg1,
             |  avg(ss_list_price) agg2,
             |  avg(ss_coupon_amt) agg3,
             |  avg(ss_sales_price) agg4
             |from
             |  store_sales
             |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
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
              |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
              |    join store on (store_sales.ss_store_sk = store.s_store_sk)
              |    -- join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |  where
              |    store.s_county in ('Williamson County','Franklin Parish','Bronx County','Orange County')
              |    -- and date_dim.d_dom between 1 and 2
              |    -- and date_dim.d_year in(1998, 1998 + 1, 1998 + 2)
              |    -- and ss_date between '1999-01-01' and '2001-12-02'
              |    -- and dayofmonth(ss_date) in (1,2)
              |    -- partition key filter
              |    -- and ss_sold_date_sk in (2450816, 2450846, 2450847, 2450874, 2450875, 2450905, 2450906, 2450935, 2450936, 2450966, 2450967,
              |    --                         2450996, 2450997, 2451027, 2451028, 2451058, 2451059, 2451088, 2451089, 2451119, 2451120, 2451149,
              |    --                         2451150, 2451180, 2451181, 2451211, 2451212, 2451239, 2451240, 2451270, 2451271, 2451300, 2451301,
              |    --                         2451331, 2451332, 2451361, 2451362, 2451392, 2451393, 2451423, 2451424, 2451453, 2451454, 2451484,
              |    --                         2451485, 2451514, 2451515, 2451545, 2451546, 2451576, 2451577, 2451605, 2451606, 2451636, 2451637,
              |    --                         2451666, 2451667, 2451697, 2451698, 2451727, 2451728, 2451758, 2451759, 2451789, 2451790, 2451819,
              |    --                         2451820, 2451850, 2451851, 2451880, 2451881)
              |    and (household_demographics.hd_buy_potential = '>10000'
              |      or household_demographics.hd_buy_potential = 'unknown')
              |    and household_demographics.hd_vehicle_count > 0
              |    and case when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count else null end > 1
              |    and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
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
              |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
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

    ("q14a", """
               |with cross_items as
               | (select i_item_sk ss_item_sk
               | from item,
               |    (select iss.i_brand_id brand_id, iss.i_class_id class_id, iss.i_category_id category_id
               |     from store_sales, item iss, date_dim d1
               |     where ss_item_sk = iss.i_item_sk
                    and ss_sold_date_sk = d1.d_date_sk
               |       and d1.d_year between 1999 AND 1999 + 2
               |   intersect
               |     select ics.i_brand_id, ics.i_class_id, ics.i_category_id
               |     from catalog_sales, item ics, date_dim d2
               |     where cs_item_sk = ics.i_item_sk
               |       and cs_sold_date_sk = d2.d_date_sk
               |       and d2.d_year between 1999 AND 1999 + 2
               |   intersect
               |     select iws.i_brand_id, iws.i_class_id, iws.i_category_id
               |     from web_sales, item iws, date_dim d3
               |     where ws_item_sk = iws.i_item_sk
               |       and ws_sold_date_sk = d3.d_date_sk
               |       and d3.d_year between 1999 AND 1999 + 2) x
               | where i_brand_id = brand_id
               |   and i_class_id = class_id
               |   and i_category_id = category_id
               |),
               | avg_sales as
               | (select avg(quantity*list_price) average_sales
               |  from (
               |     select ss_quantity quantity, ss_list_price list_price
               |     from store_sales, date_dim
               |     where ss_sold_date_sk = d_date_sk
               |       and d_year between 1999 and 2001
               |   union all
               |     select cs_quantity quantity, cs_list_price list_price
               |     from catalog_sales, date_dim
               |     where cs_sold_date_sk = d_date_sk
               |       and d_year between 1999 and 1999 + 2
               |   union all
               |     select ws_quantity quantity, ws_list_price list_price
               |     from web_sales, date_dim
               |     where ws_sold_date_sk = d_date_sk
               |       and d_year between 1999 and 1999 + 2) x)
               | select channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
               | from(
               |     select 'store' channel, i_brand_id,i_class_id
               |             ,i_category_id,sum(ss_quantity*ss_list_price) sales
               |             , count(*) number_sales
               |     from store_sales, item, date_dim
               |     where ss_item_sk in (select ss_item_sk from cross_items)
               |       and ss_item_sk = i_item_sk
               |       and ss_sold_date_sk = d_date_sk
               |       and d_year = 1999+2
               |       and d_moy = 11
               |     group by i_brand_id,i_class_id,i_category_id
               |     having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
               |   union all
               |     select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
               |     from catalog_sales, item, date_dim
               |     where cs_item_sk in (select ss_item_sk from cross_items)
               |       and cs_item_sk = i_item_sk
               |       and cs_sold_date_sk = d_date_sk
               |       and d_year = 1999+2
               |       and d_moy = 11
               |     group by i_brand_id,i_class_id,i_category_id
               |     having sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
               |   union all
               |     select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
               |     from web_sales, item, date_dim
               |     where ws_item_sk in (select ss_item_sk from cross_items)
               |       and ws_item_sk = i_item_sk
               |       and ws_sold_date_sk = d_date_sk
               |       and d_year = 1999+2
               |       and d_moy = 11
               |     group by i_brand_id,i_class_id,i_category_id
               |     having sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)
               | ) y
               | group by rollup (channel, i_brand_id,i_class_id,i_category_id)
               | order by channel,i_brand_id,i_class_id,i_category_id
               | limit 100
             """.stripMargin),

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
              |  ss_sold_date_sk between 2451911 and 2451941  -- partition key filter (1 calendar month)
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
               """.stripMargin),
    ("filter", """
                 | select count(*) from store_sales where ss_store_sk = 1
               """.stripMargin),
    ("join", """
               | select count(i_current_price) from store_sales join item
               |   on (store_sales.ss_item_sk = item.i_item_sk)
               | where
               |   i_category = 'Sports'
               |   and ss_sold_date_sk between 2451911 and 2451941
             """.stripMargin),
    ("agg", """
              | select count(ss_promo_sk) from store_sales
              | where ss_sold_date_sk > 2451911
              | group by ss_sold_date_sk
            """.stripMargin),
    ("join3", """
                | select count(i_category), count(s_county) from store_sales
                |   join item on (store_sales.ss_item_sk = item.i_item_sk)
                |   join store on (store_sales.ss_store_sk = store.s_store_sk)
              """.stripMargin)).toArray

  def tpcdsSetup(): String = {
    val HOME = "/Users/sameer/tpcds/"
    val dir = HOME + "store_sales"
    sqlContext.read.parquet(HOME + "customer").registerTempTable("customer")
    sqlContext.read.parquet(HOME + "customer_address").registerTempTable("customer_address")
    sqlContext.read.parquet(HOME + "customer_demographics")
      .registerTempTable("customer_demographics")
    sqlContext.read.parquet(HOME + "date_dim").registerTempTable("date_dim")
    sqlContext.read.parquet(HOME + "household_demographics")
      .registerTempTable("household_demographics")
    sqlContext.read.parquet(HOME + "inventory").registerTempTable("inventory")
    sqlContext.read.parquet(HOME + "item").registerTempTable("item")
    sqlContext.read.parquet(HOME + "promotion").registerTempTable("promotion")
    sqlContext.read.parquet(HOME + "store").registerTempTable("store")
    sqlContext.read.parquet(HOME + "catalog_sales").registerTempTable("catalog_sales")
    sqlContext.read.parquet(HOME + "web_sales").registerTempTable("web_sales")
    sqlContext.read.parquet(dir).registerTempTable("store_sales")
    dir
  }

  def tpcdsAll(): Unit = {
    tpcdsSetup()
    sqlContext.conf.setConfString(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sqlContext.conf.setConfString(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    val benchmark = new Benchmark("TPCDS Snappy", 28800501 * 4, 1)
    tpcds.filter(q => q._1 == "q14a").foreach(query => {
      benchmark.addCase(query._1) { i =>
        sqlContext.sql(query._2).show(2)
      }
    })
    benchmark.run
  }

  def tpcdsBenchmark(): Unit = {
    val dir = tpcdsSetup()

    sqlContext.conf.setConfString(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sqlContext.conf.setConfString(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    val benchmark = new Benchmark("TPCDS", 28800501)
    val query = sqlContext.sql(tpcds(0)._2)

    val files = SpecificParquetRecordReaderBase.listDirectory(new File(dir)).toArray
    // Driving the parquet reader directly without Spark.
    benchmark.addCase("ParquetReader") { num =>
      var sum = 0L
      files.map(_.asInstanceOf[String]).foreach { p =>
        val reader = new VectorizedParquetRecordReader
        reader.initialize(p, ("ss_store_sk" :: "ss_sold_date_sk" :: "ss_ext_sales_price"
          :: "ss_customer_sk" :: "ss_item_sk" :: Nil).asJava)
        val batch = reader.resultBatch()
        while (reader.nextBatch()) {
          val it = batch.rowIterator()
          while (it.hasNext) {
            val record = it.next()
            if (!record.isNullAt(0)) sum += 1
            if (!record.isNullAt(1)) sum += 1
            if (!record.isNullAt(2)) sum += 1
            if (!record.isNullAt(3)) sum += 1
            if (!record.isNullAt(4)) sum += 1
          }
        }
        println(sum)
        reader.close()
      }
    }

    benchmark.addCase("counts") { i =>
      sqlContext.sql(
        s"""
           | select count(ss_store_sk), count(ss_sold_date_sk), count(ss_ext_sales_price),
           | count(ss_customer_sk), count(ss_item_sk) from store_sales
         """.stripMargin).show
    }

    benchmark.addCase("Q19") { i =>
      query.show(5)
    }

    /**
     * Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
     * TPCDS Snappy:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     * -------------------------------------------------------------------------------------------
     * ParquetReader                            2052 / 2119         14.0          71.3       1.0X
     * counts                                   2580 / 2633         11.2          89.6       0.8X
     * Q19                                      3607 / 3720          8.0         125.2       0.6X
     **
     *counts (master)                          5608 / 5732          5.1         194.7       0.3X
     *Q19 (master)                             5418 / 5682          5.3         188.1       0.4X
     **
     *TPCDS Uncompressed:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *ParquetReader                            1929 / 2031         14.9          67.0       1.0X
     *counts                                   2427 / 2460         11.9          84.3       0.8X
     *Q19                                      3421 / 3598          8.4         118.8       0.6X
     **
     *TPCDS GZIP:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *ParquetReader                            3475 / 3626          8.3         120.6       1.0X
     *counts                                   3559 / 3727          8.1         123.6       1.0X
     *Q19                                      4876 / 5139          5.9         169.3       0.7X
     */
    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    // tpcdsAll()
    tpcdsBenchmark()
  }
}