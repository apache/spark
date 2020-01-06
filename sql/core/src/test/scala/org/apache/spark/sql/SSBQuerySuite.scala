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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.resourceToString

/**
 * This test suite ensures all the Star Schema Benchmark queries can be successfully analyzed,
 * optimized and compiled without hitting the max iteration threshold.
 */
class SSBQuerySuite extends BenchmarkQueryTest {

  override def beforeAll: Unit = {
    super.beforeAll

    sql(
      """
        |CREATE TABLE `part` (`p_partkey` INT, `p_name` STRING, `p_mfgr` STRING,
        |`p_category` STRING, `p_brand1` STRING, `p_color` STRING, `p_type` STRING, `p_size` INT,
        |`p_container` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `supplier` (`s_suppkey` INT, `s_name` STRING, `s_address` STRING,
        |`s_city` STRING, `s_nation` STRING, `s_region` STRING, `s_phone` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `customer` (`c_custkey` INT, `c_name` STRING, `c_address` STRING,
        |`c_city` STRING, `c_nation` STRING, `c_region` STRING, `c_phone` STRING,
        |`c_mktsegment` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `date` (`d_datekey` INT, `d_date` STRING, `d_dayofweek` STRING,
        |`d_month` STRING, `d_year` INT, `d_yearmonthnum` INT, `d_yearmonth` STRING,
        |`d_daynuminweek` INT, `d_daynuminmonth` INT, `d_daynuminyear` INT, `d_monthnuminyear` INT,
        |`d_weeknuminyear` INT, `d_sellingseason` STRING, `d_lastdayinweekfl` STRING,
        |`d_lastdayinmonthfl` STRING, `d_holidayfl` STRING, `d_weekdayfl` STRING)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |CREATE TABLE `lineorder` (`lo_orderkey` INT, `lo_linenumber` INT, `lo_custkey` INT,
        |`lo_partkey` INT, `lo_suppkey` INT, `lo_orderdate` INT, `lo_orderpriority` STRING,
        |`lo_shippriority` STRING, `lo_quantity` INT, `lo_extendedprice` INT,
        |`lo_ordertotalprice` INT, `lo_discount` INT, `lo_revenue` INT, `lo_supplycost` INT,
        |`lo_tax` INT, `lo_commitdate` INT, `lo_shipmode` STRING)
        |USING parquet
      """.stripMargin)
  }

  val ssbQueries = Seq(
    "1.1", "1.2", "1.3", "2.1", "2.2", "2.3", "3.1", "3.2", "3.3", "3.4", "4.1", "4.2", "4.3")

  ssbQueries.foreach { name =>
    val queryString = resourceToString(s"ssb/$name.sql",
      classLoader = Thread.currentThread.getContextClassLoader)
    test(name) {
      // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan)
    }
  }
}
