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

package org.apache.spark.sql.hive.execution

import java.io.File
import java.util.{Locale, TimeZone}

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.util.Utils

/**
 * The test suite for window functions. To actually compare results with Hive,
 * every test should be created by `createQueryTest`. Because we are reusing tables
 * for different tests and there are a few properties needed to let Hive generate golden
 * files, every `createQueryTest` calls should explicitly set `reset` to `false`.
 */
abstract class HiveWindowFunctionQueryBaseSuite extends HiveComparisonTest with BeforeAndAfter {
  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault
  private val testTempDir = Utils.createTempDir()

  override def beforeAll() {
    TestHive.cacheTables = true
    // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    // Add Locale setting
    Locale.setDefault(Locale.US)

    // Create the table used in windowing.q
    sql("DROP TABLE IF EXISTS part")
    sql(
      """
        |CREATE TABLE part(
        |  p_partkey INT,
        |  p_name STRING,
        |  p_mfgr STRING,
        |  p_brand STRING,
        |  p_type STRING,
        |  p_size INT,
        |  p_container STRING,
        |  p_retailprice DOUBLE,
        |  p_comment STRING)
      """.stripMargin)
    val testData1 = TestHive.getHiveFile("data/files/part_tiny.txt").getCanonicalPath
    sql(
      s"""
        |LOAD DATA LOCAL INPATH '$testData1' overwrite into table part
      """.stripMargin)

    sql("DROP TABLE IF EXISTS over1k")
    sql(
      """
        |create table over1k(
        |  t tinyint,
        |  si smallint,
        |  i int,
        |  b bigint,
        |  f float,
        |  d double,
        |  bo boolean,
        |  s string,
        |  ts timestamp,
        |  dec decimal(4,2),
        |  bin binary)
        |row format delimited
        |fields terminated by '|'
      """.stripMargin)
    val testData2 = TestHive.getHiveFile("data/files/over1k").getCanonicalPath
    sql(
      s"""
        |LOAD DATA LOCAL INPATH '$testData2' overwrite into table over1k
      """.stripMargin)

    // The following settings are used for generating golden files with Hive.
    // We have to use kryo to correctly let Hive serialize plans with window functions.
    // This is used to generate golden files.
    sql("set hive.plan.serialization.format=kryo")
    // Explicitly set fs to local fs.
    sql(s"set fs.default.name=file://$testTempDir/")
    // Ask Hive to run jobs in-process as a single map and reduce task.
    sql("set mapred.job.tracker=local")
  }

  override def afterAll() {
    TestHive.cacheTables = false
    TimeZone.setDefault(originalTimeZone)
    Locale.setDefault(originalLocale)
    TestHive.reset()
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests based on windowing_multipartitioning.q
  // Results of the original query file are not deterministic.
  /////////////////////////////////////////////////////////////////////////////
  createQueryTest("windowing_multipartitioning.q (deterministic) 1",
    s"""
      |select s,
      |rank() over (partition by s order by si) r,
      |sum(b) over (partition by s order by si) sum
      |from over1k
      |order by s, r, sum;
    """.stripMargin, reset = false)

  /* timestamp comparison issue with Hive?
  createQueryTest("windowing_multipartitioning.q (deterministic) 2",
    s"""
      |select s,
      |rank() over (partition by s order by dec desc) r,
      |sum(b) over (partition by s order by ts desc) as sum
      |from over1k
      |where s = 'tom allen' or s = 'bob steinbeck'
      |order by s, r, sum;
     """.stripMargin, reset = false)
  */

  createQueryTest("windowing_multipartitioning.q (deterministic) 3",
    s"""
      |select s, sum(i) over (partition by s), sum(f) over (partition by si)
      |from over1k where s = 'tom allen' or s = 'bob steinbeck';
     """.stripMargin, reset = false)

  createQueryTest("windowing_multipartitioning.q (deterministic) 4",
    s"""
      |select s, rank() over (partition by s order by bo),
      |rank() over (partition by si order by bin desc) from over1k
      |where s = 'tom allen' or s = 'bob steinbeck';
     """.stripMargin, reset = false)

  createQueryTest("windowing_multipartitioning.q (deterministic) 5",
    s"""
      |select s, sum(f) over (partition by i), row_number() over (order by f)
      |from over1k where s = 'tom allen' or s = 'bob steinbeck';
     """.stripMargin, reset = false)

  createQueryTest("windowing_multipartitioning.q (deterministic) 6",
    s"""
      |select s, rank() over w1,
      |rank() over w2
      |from over1k
      |where s = 'tom allen' or s = 'bob steinbeck'
      |window
      |w1 as (partition by s order by dec),
      |w2 as (partition by si order by f) ;
     """.stripMargin, reset = false)

  /////////////////////////////////////////////////////////////////////////////
  // Tests based on windowing_navfn.q
  // Results of the original query file are not deterministic.
  // Also, the original query of
  // select i, lead(s) over (partition by bin order by d,i desc) from over1k ;
  /////////////////////////////////////////////////////////////////////////////
  createQueryTest("windowing_navfn.q (deterministic)",
    s"""
      |select s, row_number() over (partition by d order by dec) rn from over1k
      |order by s, rn desc;
      |select i, lead(s) over (partition by cast(bin as string) order by d,i desc) as l
      |from over1k
      |order by i desc, l;
      |select i, lag(dec) over (partition by i order by s,i,dec) l from over1k
      |order by i, l;
      |select s, last_value(t) over (partition by d order by f) l from over1k
      |order by s, l;
      |select s, first_value(s) over (partition by bo order by s) f from over1k
      |order by s, f;
      |select t, s, i, last_value(i) over (partition by t order by s)
      |from over1k where (s = 'oscar allen' or s = 'oscar carson') and t = 10;
     """.stripMargin, reset = false)

  /////////////////////////////////////////////////////////////////////////////
  // Tests based on windowing_ntile.q
  // Results of the original query file are not deterministic.
  /////////////////////////////////////////////////////////////////////////////
  createQueryTest("windowing_ntile.q (deterministic)",
    s"""
      |select i, ntile(10) over (partition by s order by i) n from over1k
      |order by i, n;
      |select s, ntile(100) over (partition by i order by s) n from over1k
      |order by s, n;
      |select f, ntile(4) over (partition by d order by f) n from over1k
      |order by f, n;
      |select d, ntile(1000) over (partition by dec order by d) n from over1k
      |order by d, n;
     """.stripMargin, reset = false)

  /////////////////////////////////////////////////////////////////////////////
  // Tests based on windowing_udaf.q
  // Results of the original query file are not deterministic.
  /////////////////////////////////////////////////////////////////////////////
  createQueryTest("windowing_udaf.q (deterministic)",
    s"""
      |select s, min(i) over (partition by s) m from over1k
      |order by s, m;
      |select s, avg(f) over (partition by si order by s) a from over1k
      |order by s, a;
      |select s, avg(i) over (partition by t, b order by s) a from over1k
      |order by s, a;
      |select max(i) over w m from over1k
      |order by m window w as (partition by f) ;
      |select s, avg(d) over (partition by t order by f) a from over1k
      |order by s, a;
     """.stripMargin, reset = false)

  /////////////////////////////////////////////////////////////////////////////
  // Tests based on windowing_windowspec.q
  // Results of the original query file are not deterministic.
  /////////////////////////////////////////////////////////////////////////////
  createQueryTest("windowing_windowspec.q (deterministic)",
    s"""
      |select s, sum(b) over (partition by i order by s,b rows unbounded preceding) as sum
      |from over1k order by s, sum;
      |select s, sum(f) over (partition by d order by s,f rows unbounded preceding) as sum
      |from over1k order by s, sum;
      |select s, sum(f) over
      |(partition by ts order by f range between current row and unbounded following) as sum
      |from over1k order by s, sum;
      |select s, avg(f)
      |over (partition by ts order by s,f rows between current row and 5 following) avg
      |from over1k order by s, avg;
      |select s, avg(d) over
      |(partition by t order by s,d desc rows between 5 preceding and 5 following) avg
      |from over1k order by s, avg;
      |select s, sum(i) over(partition by ts order by s) sum from over1k
      |order by s, sum;
      |select f, sum(f) over
      |(partition by ts order by f range between unbounded preceding and current row) sum
      |from over1k order by f, sum;
      |select s, i, round(avg(d) over (partition by s order by i) / 10.0 , 2) avg
      |from over1k order by s, i, avg;
      |select s, i, round((avg(d) over  w1 + 10.0) - (avg(d) over w1 - 10.0),2) avg
      |from over1k
      |order by s, i, avg window w1 as (partition by s order by i);
     """.stripMargin, reset = false)

  /////////////////////////////////////////////////////////////////////////////
  // Tests based on windowing_rank.q
  // Results of the original query file are not deterministic.
  /////////////////////////////////////////////////////////////////////////////
  createQueryTest("windowing_rank.q (deterministic) 1",
    s"""
      |select s, rank() over (partition by f order by t) r from over1k order by s, r;
      |select s, dense_rank() over (partition by ts order by i,s desc) as r from over1k
      |order by s desc, r desc;
      |select s, cume_dist() over (partition by bo order by b,s) cd from over1k
      |order by s, cd;
      |select s, percent_rank() over (partition by dec order by f) r from over1k
      |order by s desc, r desc;
     """.stripMargin, reset = false)

  createQueryTest("windowing_rank.q (deterministic) 2",
    s"""
      |select ts, dec, rnk
      |from
      |  (select ts, dec,
      |          rank() over (partition by ts order by dec)  as rnk
      |          from
      |            (select other.ts, other.dec
      |             from over1k other
      |             join over1k on (other.b = over1k.b)
      |            ) joined
      |  ) ranked
      |where rnk =  1
      |order by ts, dec, rnk;
     """.stripMargin, reset = false)

  createQueryTest("windowing_rank.q (deterministic) 3",
    s"""
      |select ts, dec, rnk
      |from
      |  (select ts, dec,
      |          rank() over (partition by ts order by dec)  as rnk
      |          from
      |            (select other.ts, other.dec
      |             from over1k other
      |             join over1k on (other.b = over1k.b)
      |            ) joined
      |  ) ranked
      |where dec = 89.5
      |order by ts, dec, rnk;
     """.stripMargin, reset = false)

  createQueryTest("windowing_rank.q (deterministic) 4",
    s"""
      |select ts, dec, rnk
      |from
      |  (select ts, dec,
      |          rank() over (partition by ts order by dec)  as rnk
      |          from
      |            (select other.ts, other.dec
      |             from over1k other
      |             join over1k on (other.b = over1k.b)
      |             where other.t < 10
      |            ) joined
      |  ) ranked
      |where rnk = 1
      |order by ts, dec, rnk;
     """.stripMargin, reset = false)

  /////////////////////////////////////////////////////////////////////////////
  // Tests from windowing.q
  // We port tests in windowing.q to here because this query file contains too
  // many tests and the syntax of test "-- 7. testJoinWithWindowingAndPTF"
  // is not supported right now.
  /////////////////////////////////////////////////////////////////////////////
  createQueryTest("windowing.q -- 1. testWindowing",
    s"""
      |select p_mfgr, p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |sum(p_retailprice) over
      |(distribute by p_mfgr sort by p_name rows between unbounded preceding and current row) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 2. testGroupByWithPartitioning",
    s"""
      |select p_mfgr, p_name, p_size,
      |min(p_retailprice),
      |rank() over(distribute by p_mfgr sort by p_name)as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |group by p_mfgr, p_name, p_size
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 3. testGroupByHavingWithSWQ",
    s"""
      |select p_mfgr, p_name, p_size, min(p_retailprice),
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |group by p_mfgr, p_name, p_size
      |having p_size > 0
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 4. testCount",
    s"""
      |select p_mfgr, p_name,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as cd
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 5. testCountWithWindowingUDAF",
    s"""
      |select p_mfgr, p_name,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as cd,
      |p_retailprice, sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |                                  rows between unbounded preceding and current row) as s1,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 6. testCountInSubQ",
    s"""
      |select sub1.r, sub1.dr, sub1.cd, sub1.s1, sub1.deltaSz
      |from (select p_mfgr, p_name,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as cd,
      |p_retailprice, sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |                                  rows between unbounded preceding and current row) as s1,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |) sub1
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 8. testMixedCaseAlias",
    s"""
      |select p_mfgr, p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name, p_size desc) as R
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 9. testHavingWithWindowingNoGBY",
    s"""
      |select p_mfgr, p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |                        rows between unbounded preceding and current row)  as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 10. testHavingWithWindowingCondRankNoGBY",
    s"""
      |select p_mfgr, p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |                        rows between unbounded preceding and current row) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 11. testFirstLast",
    s"""
      |select  p_mfgr,p_name, p_size,
      |sum(p_size) over (distribute by p_mfgr sort by p_name
      |rows between current row and current row) as s2,
      |first_value(p_size) over w1  as f,
      |last_value(p_size, false) over w1  as l
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 12. testFirstLastWithWhere",
    s"""
      |select  p_mfgr,p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |sum(p_size) over (distribute by p_mfgr sort by p_name
      |rows between current row and current row) as s2,
      |first_value(p_size) over w1 as f,
      |last_value(p_size, false) over w1 as l
      |from part
      |where p_mfgr = 'Manufacturer#3'
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 13. testSumWindow",
    s"""
      |select  p_mfgr,p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over (distribute by p_mfgr  sort by p_name
      |rows between current row and current row)  as s2
      |from part
      |window w1 as (distribute by p_mfgr  sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 14. testNoSortClause",
    s"""
      |select  p_mfgr,p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 15. testExpressions",
    s"""
      |select  p_mfgr,p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |cume_dist() over(distribute by p_mfgr sort by p_name) as cud,
      |percent_rank() over(distribute by p_mfgr sort by p_name) as pr,
      |ntile(3) over(distribute by p_mfgr sort by p_name) as nt,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as ca,
      |avg(p_size) over(distribute by p_mfgr sort by p_name) as avg,
      |stddev(p_size) over(distribute by p_mfgr sort by p_name) as st,
      |first_value(p_size % 5) over(distribute by p_mfgr sort by p_name) as fv,
      |last_value(p_size) over(distribute by p_mfgr sort by p_name) as lv,
      |first_value(p_size) over w1  as fvW1
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 16. testMultipleWindows",
    s"""
      |select  p_mfgr,p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |cume_dist() over(distribute by p_mfgr sort by p_name) as cud,
      |sum(p_size) over (distribute by p_mfgr sort by p_name
      |range between unbounded preceding and current row) as s1,
      |sum(p_size) over (distribute by p_mfgr sort by p_size
      |range between 5 preceding and current row) as s2,
      |first_value(p_size) over w1  as fv1
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)


  createQueryTest("windowing.q -- 17. testCountStar",
    s"""
      |select  p_mfgr,p_name, p_size,
      |count(*) over(distribute by p_mfgr sort by p_name ) as c,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as ca,
      |first_value(p_size) over w1  as fvW1
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 18. testUDAFs",
    s"""
      |select  p_mfgr,p_name, p_size,
      |sum(p_retailprice) over w1 as s,
      |min(p_retailprice) over w1 as mi,
      |max(p_retailprice) over w1 as ma,
      |avg(p_retailprice) over w1 as ag
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 19. testUDAFsWithGBY",
    """
      |select  p_mfgr,p_name, p_size, p_retailprice,
      |sum(p_retailprice) over w1 as s,
      |min(p_retailprice) as mi ,
      |max(p_retailprice) as ma ,
      |avg(p_retailprice) over w1 as ag
      |from part
      |group by p_mfgr,p_name, p_size, p_retailprice
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following);
    """.stripMargin, reset = false)

  // collect_set() output array in an arbitrary order, hence causes different result
  // when running this test suite under Java 7 and 8.
  // We change the original sql query a little bit for making the test suite passed
  // under different JDK
  createQueryTest("windowing.q -- 20. testSTATs",
    """
      |select p_mfgr,p_name, p_size, sdev, sdev_pop, uniq_data, var, cor, covarp
      |from (
      |select  p_mfgr,p_name, p_size,
      |stddev(p_retailprice) over w1 as sdev,
      |stddev_pop(p_retailprice) over w1 as sdev_pop,
      |collect_set(p_size) over w1 as uniq_size,
      |variance(p_retailprice) over w1 as var,
      |corr(p_size, p_retailprice) over w1 as cor,
      |covar_pop(p_size, p_retailprice) over w1 as covarp
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
      |) t lateral view explode(uniq_size) d as uniq_data
      |order by p_mfgr,p_name, p_size, sdev, sdev_pop, uniq_data, var, cor, covarp
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 21. testDISTs",
    """
      |select  p_mfgr,p_name, p_size,
      |histogram_numeric(p_retailprice, 5) over w1 as hist,
      |percentile(p_partkey, 0.5) over w1 as per,
      |row_number() over(distribute by p_mfgr sort by p_name) as rn
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 24. testLateralViews",
    """
      |select p_mfgr, p_name,
      |lv_col, p_size, sum(p_size) over w1   as s
      |from (select p_mfgr, p_name, p_size, array(1,2,3) arr from part) p
      |lateral view explode(arr) part_lv as lv_col
      |window w1 as (distribute by p_mfgr sort by p_size, lv_col
      |             rows between 2 preceding and current row)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 26. testGroupByHavingWithSWQAndAlias",
    """
      |select p_mfgr, p_name, p_size, min(p_retailprice) as mi,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |group by p_mfgr, p_name, p_size
      |having p_size > 0
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 27. testMultipleRangeWindows",
    """
      |select  p_mfgr,p_name, p_size,
      |sum(p_size) over (distribute by p_mfgr sort by p_size
      |range between 10 preceding and current row) as s2,
      |sum(p_size) over (distribute by p_mfgr sort by p_size
      |range between current row and 10 following )  as s1
      |from part
      |window w1 as (rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 28. testPartOrderInUDAFInvoke",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over (partition by p_mfgr  order by p_name
      |rows between 2 preceding and 2 following) as s
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 29. testPartOrderInWdwDef",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s
      |from part
      |window w1 as (partition by p_mfgr  order by p_name
      |             rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 30. testDefaultPartitioningSpecRules",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s,
      |sum(p_size) over w2 as s2
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following),
      |       w2 as (partition by p_mfgr order by p_name)
    """.stripMargin, reset = false)

  /* p_name is not a numeric column. What is Hive's semantic?
  createQueryTest("windowing.q -- 31. testWindowCrossReference",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over w2 as s2
      |from part
      |window w1 as (partition by p_mfgr order by p_name
      |             range between 2 preceding and 2 following),
      |       w2 as w1
    """.stripMargin, reset = false)
  */
  /*
  createQueryTest("windowing.q -- 32. testWindowInheritance",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over w2 as s2
      |from part
      |window w1 as (partition by p_mfgr order by p_name
      |             range between 2 preceding and 2 following),
      |       w2 as (w1 rows between unbounded preceding and current row)
    """.stripMargin, reset = false)
  */

  /* p_name is not a numeric column. What is Hive's semantic?
  createQueryTest("windowing.q -- 33. testWindowForwardReference",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over w2 as s2,
      |sum(p_size) over w3 as s3
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name
      |             range between 2 preceding and 2 following),
      |       w2 as w3,
      |       w3 as (distribute by p_mfgr sort by p_name
      |             range between unbounded preceding and current row)
    """.stripMargin, reset = false)
  */
  /*
  createQueryTest("windowing.q -- 34. testWindowDefinitionPropagation",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s1,
      |sum(p_size) over w2 as s2,
      |sum(p_size) over (w3 rows between 2 preceding and 2 following)  as s3
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name
      |             range between 2 preceding and 2 following),
      |       w2 as w3,
      |       w3 as (distribute by p_mfgr sort by p_name
      |             range between unbounded preceding and current row)
    """.stripMargin, reset = false)
  */

  /* Seems Hive evaluate SELECT DISTINCT before window functions?
  createQueryTest("windowing.q -- 35. testDistinctWithWindowing",
    """
      |select DISTINCT p_mfgr, p_name, p_size,
      |sum(p_size) over w1 as s
      |from part
      |window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following)
    """.stripMargin, reset = false)
  */

  createQueryTest("windowing.q -- 36. testRankWithPartitioning",
    """
      |select p_mfgr, p_name, p_size,
      |rank() over (partition by p_mfgr order by p_name )  as r
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 37. testPartitioningVariousForms",
    """
      |select p_mfgr,
      |round(sum(p_retailprice) over (partition by p_mfgr order by p_mfgr),2) as s1,
      |min(p_retailprice) over (partition by p_mfgr) as s2,
      |max(p_retailprice) over (distribute by p_mfgr sort by p_mfgr) as s3,
      |round(avg(p_retailprice) over (distribute by p_mfgr),2) as s4,
      |count(p_retailprice) over (cluster by p_mfgr ) as s5
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 38. testPartitioningVariousForms2",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (partition by p_mfgr, p_name order by p_mfgr, p_name
      |rows between unbounded preceding and current row) as s1,
      |min(p_retailprice) over (distribute by p_mfgr, p_name sort by p_mfgr, p_name
      |rows between unbounded preceding and current row) as s2,
      |max(p_retailprice) over (partition by p_mfgr, p_name order by p_name) as s3
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 39. testUDFOnOrderCols",
    """
      |select p_mfgr, p_type, substr(p_type, 2) as short_ptype,
      |rank() over (partition by p_mfgr order by substr(p_type, 2))  as r
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 40. testNoBetweenForRows",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_name rows unbounded preceding) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 41. testNoBetweenForRange",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_size range unbounded preceding) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 42. testUnboundedFollowingForRows",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_name
      |rows between current row and unbounded following) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 43. testUnboundedFollowingForRange",
    """
      |select p_mfgr, p_name, p_size,
      |sum(p_retailprice) over (distribute by p_mfgr sort by p_size
      |range between current row and unbounded following) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 44. testOverNoPartitionSingleAggregate",
    """
      |select p_name, p_retailprice,
      |round(avg(p_retailprice) over(),2)
      |from part
      |order by p_name
    """.stripMargin, reset = false)
}

class HiveWindowFunctionQueryWithoutCodeGenSuite extends HiveWindowFunctionQueryBaseSuite {
  var originalCodegenEnabled: Boolean = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    originalCodegenEnabled = conf.codegenEnabled
    sql("set spark.sql.codegen=false")
  }

  override def afterAll(): Unit = {
    sql(s"set spark.sql.codegen=$originalCodegenEnabled")
    super.afterAll()
  }
}

abstract class HiveWindowFunctionQueryFileBaseSuite
  extends HiveCompatibilitySuite with BeforeAndAfter {
  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault
  private val testTempDir = Utils.createTempDir()

  override def beforeAll() {
    TestHive.cacheTables = true
    // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    // Add Locale setting
    Locale.setDefault(Locale.US)

    // The following settings are used for generating golden files with Hive.
    // We have to use kryo to correctly let Hive serialize plans with window functions.
    // This is used to generate golden files.
    sql("set hive.plan.serialization.format=kryo")
    // Explicitly set fs to local fs.
    sql(s"set fs.default.name=file://$testTempDir/")
    // Ask Hive to run jobs in-process as a single map and reduce task.
    sql("set mapred.job.tracker=local")
  }

  override def afterAll() {
    TestHive.cacheTables = false
    TimeZone.setDefault(originalTimeZone)
    Locale.setDefault(originalLocale)
    TestHive.reset()
  }

  override def blackList: Seq[String] = Seq(
    // Partitioned table functions are not supported.
    "ptf*",
    // tests of windowing.q are in HiveWindowFunctionQueryBaseSuite
    "windowing.q",

    // This one failed on the expression of
    // sum(lag(p_retailprice,1,0.0)) over w1
    // lag(p_retailprice,1,0.0) is a GenericUDF and the argument inspector of
    // p_retailprice created by HiveInspectors is
    // PrimitiveObjectInspectorFactory.javaDoubleObjectInspector.
    // However, seems Hive assumes it is
    // PrimitiveObjectInspectorFactory.writableDoubleObjectInspector, which introduces an error.
    "windowing_expressions",

    // Hive's results are not deterministic
    "windowing_multipartitioning",
    "windowing_navfn",
    "windowing_ntile",
    "windowing_udaf",
    "windowing_windowspec",
    "windowing_rank"
  )

  override def whiteList: Seq[String] = Seq(
    "windowing_udaf2",
    "windowing_columnPruning",
    "windowing_adjust_rowcontainer_sz"
  )

  override def testCases: Seq[(String, File)] = super.testCases.filter {
    case (name, _) => realWhiteList.contains(name)
  }
}

class HiveWindowFunctionQueryFileWithoutCodeGenSuite extends HiveWindowFunctionQueryFileBaseSuite {
  var originalCodegenEnabled: Boolean = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    originalCodegenEnabled = conf.codegenEnabled
    sql("set spark.sql.codegen=false")
  }

  override def afterAll(): Unit = {
    sql(s"set spark.sql.codegen=$originalCodegenEnabled")
    super.afterAll()
  }
}
