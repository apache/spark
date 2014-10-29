/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.{Row, SchemaRDD}

class HiveWindowFunctionSuite extends HiveComparisonTest {

  override def beforeAll() {
    sql("DROP TABLE IF EXISTS part").collect()

    sql("""
        |CREATE TABLE part(
        |    p_partkey INT,
        |    p_name STRING,
        |    p_mfgr STRING,
        |    p_brand STRING,
        |    p_type STRING,
        |    p_size INT,
        |    p_container STRING,
        |    p_retailprice DOUBLE,
        |    p_comment STRING
        |)
      """.stripMargin).collect()

    //remove duplicate data in part_tiny.txt for hive bug
    // https://issues.apache.org/jira/browse/HIVE-8569
    sql(s"""
       |LOAD DATA LOCAL INPATH '${getHiveFile("data/files/part_tiny_without_duplicate.txt")}'
       |OVERWRITE INTO TABLE part
      """.stripMargin).collect()
  }

  createQueryTest("1.testWindowing",
    """
      |SELECT p_mfgr, p_name, p_size,
      |row_number() over(DISTRIBUTE BY p_mfgr SORT BY p_name) AS r,
      |sum(p_retailprice) OVER (DISTRIBUTE BY p_mfgr SORT BY p_name rows BETWEEN
      |unbounded preceding AND current row) AS s1
      |FROM part
    """.stripMargin, false)

  createQueryTest("4.testCount",
    """
      |SELECT p_mfgr, p_name,
      |count(p_size) over(DISTRIBUTE BY p_mfgr SORT BY p_name) AS cd
      |FROM part
    """.stripMargin, false)

  createQueryTest("5.testCountWithWindowingUDAF",
    """
      |SELECT p_mfgr, p_name,
      |row_number() over(DISTRIBUTE BY p_mfgr SORT BY p_name) AS r,
      |count(p_size) over(DISTRIBUTE BY p_mfgr SORT BY p_name) AS cd,
      |p_retailprice, sum(p_retailprice) OVER (DISTRIBUTE BY p_mfgr SORT BY p_name rows
      |BETWEEN unbounded preceding AND current row) AS s1,
      |p_size
      |FROM part
    """.stripMargin, false)

  createQueryTest("6.testCountInSubQ",
    """
      |SELECT sub1.r, sub1.cd, sub1.s1
      |FROM (SELECT p_mfgr, p_name,
      |row_number() over(DISTRIBUTE BY p_mfgr SORT BY p_name) AS r,
      |count(p_size) over(DISTRIBUTE BY p_mfgr SORT BY p_name) AS cd,
      |p_retailprice, sum(p_retailprice) OVER (DISTRIBUTE BY p_mfgr SORT BY p_name rows
      |BETWEEN unbounded preceding AND current row) AS s1,
      |p_size
      |FROM part
      |) sub1
    """.stripMargin, false)
  createQueryTest("8.testMixedCaseAlias",
    """
      |SELECT p_mfgr, p_name, p_size,
      |row_number() over(DISTRIBUTE BY p_mfgr SORT BY p_name, p_size desc) AS R
      |FROM part
    """.stripMargin, false)

  createQueryTest("9.testHavingWithWindowingNoGBY",
    """
      |SELECT p_mfgr, p_name, p_size,
      |row_number() over(DISTRIBUTE BY p_mfgr SORT BY p_name) AS r,
      |sum(p_retailprice) OVER (DISTRIBUTE BY p_mfgr SORT BY p_name rows BETWEEN
      |unbounded preceding AND current row)  AS s1
      |FROM part
    """.stripMargin, false)

  createQueryTest("11.testFirstLast",
    """
      |SELECT  p_mfgr,p_name, p_size,
      |sum(p_size) OVER (DISTRIBUTE BY p_mfgr SORT BY p_name rows BETWEEN
      |current row AND current row) AS s2,
      |first_value(p_size) OVER w1  AS f,
      |last_value(p_size) OVER w1  AS l
      |FROM part
      |window w1 AS (DISTRIBUTE BY p_mfgr SORT BY p_name rows BETWEEN
      |2 preceding AND 2 following)
    """.stripMargin, false)

  createQueryTest("12.testFirstLastWithWhere",
    """
      |SELECT  p_mfgr,p_name, p_size,
      |row_number() over(DISTRIBUTE BY p_mfgr SORT BY p_name) AS r,
      |sum(p_size) OVER (DISTRIBUTE BY p_mfgr SORT BY p_name rows BETWEEN
      |current row AND current row) AS s2,
      |first_value(p_size) OVER w1 AS f,
      |last_value(p_size) OVER w1 AS l
      |FROM part
      |where p_mfgr = 'Manufacturer#3'
      |window w1 AS (DISTRIBUTE BY p_mfgr SORT BY p_name rows BETWEEN 2 preceding AND 2 following)
    """.stripMargin, false)

  createQueryTest("13.testSumWindow",
    """
      |SELECT  p_mfgr,p_name, p_size,
      |sum(p_size) OVER w1 AS s1,
      |sum(p_size) OVER (DISTRIBUTE BY p_mfgr  SORT BY p_name rows BETWEEN
      |current row AND current row)  AS s2
      |FROM part
      |window w1 AS (DISTRIBUTE BY p_mfgr  SORT BY p_name rows BETWEEN
      |2 preceding AND 2 following)
    """.stripMargin, false)

  createQueryTest("14.testNoSortClause",
    """
      |SELECT  p_mfgr,p_name, p_size,
      |row_number() over(DISTRIBUTE BY p_mfgr SORT BY p_name) AS r
      |FROM part
      |window w1 AS (DISTRIBUTE BY p_mfgr SORT BY p_name rows BETWEEN 2 preceding AND 2 following)
    """.stripMargin, false)

  createQueryTest("18.testUDAFs",
    """
      |SELECT  p_mfgr,p_name, p_size,
      |sum(p_retailprice) OVER w1 AS s,
      |min(p_retailprice) OVER w1 AS mi,
      |max(p_retailprice) OVER w1 AS ma,
      |avg(p_retailprice) OVER w1 AS ag
      |FROM part
      |window w1 AS (DISTRIBUTE BY p_mfgr SORT BY p_mfgr, p_name rows BETWEEN
      |2 preceding AND 2 following)
    """.stripMargin, false)

  createQueryTest("20.testSTATs",
    """
      |SELECT  p_mfgr,p_name, p_size,
      |stddev(p_retailprice) OVER w1 AS sdev,
      |stddev_pop(p_retailprice) OVER w1 AS sdev_pop,
      |collect_set(p_size) OVER w1 AS uniq_size,
      |variance(p_retailprice) OVER w1 AS var,
      |corr(p_size, p_retailprice) OVER w1 AS cor,
      |covar_pop(p_size, p_retailprice) OVER w1 AS covarp
      |FROM part
      |window w1 AS (DISTRIBUTE BY p_mfgr SORT BY p_mfgr, p_name rows BETWEEN
      |2 preceding AND 2 following)
    """.stripMargin, false)

  createQueryTest("27.testMultipleRangeWindows",
    """
      |SELECT  p_mfgr,p_name, p_size,
      |sum(p_size) OVER (DISTRIBUTE BY p_mfgr SORT BY p_size range BETWEEN
      |10 preceding AND current row) AS s2,
      |sum(p_size) OVER (DISTRIBUTE BY p_mfgr SORT BY p_size range BETWEEN
      |current row AND 10 following )  AS s1
      |FROM part
      |window w1 AS (rows BETWEEN 2 preceding AND 2 following)
    """.stripMargin, false)

  createQueryTest("28.testPartOrderInUDAFInvoke",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_size) OVER (PARTITION BY p_mfgr  ORDER BY p_name  rows BETWEEN
      |2 preceding AND 2 following) AS s
      |FROM part
    """.stripMargin, false)

  createQueryTest("29.testPartOrderInWdwDef",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_size) OVER w1 AS s
      |FROM part
      |window w1 AS (PARTITION BY p_mfgr  ORDER BY p_name  rows BETWEEN
      |2 preceding AND 2 following)
    """.stripMargin, false)

  createQueryTest("31.testWindowCrossReference",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_size) OVER w1 AS s1,
      |sum(p_size) OVER w2 AS s2
      |FROM part
      |window w1 AS (PARTITION BY p_mfgr ORDER BY p_size range BETWEEN 2 preceding AND 2 following),
      |       w2 AS w1
    """.stripMargin, false)

  createQueryTest("32.testWindowInheritance",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_size) OVER w1 AS s1,
      |sum(p_size) OVER w2 AS s2
      |FROM part
      |window w1 AS (PARTITION BY p_mfgr ORDER BY p_size range BETWEEN 2 preceding AND 2 following),
      |       w2 AS (w1 rows BETWEEN unbounded preceding AND current row)
    """.stripMargin, false)

  createQueryTest("33.testWindowForwardReference",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_size) OVER w1 AS s1,
      |sum(p_size) OVER w2 AS s2,
      |sum(p_size) OVER w3 AS s3
      |FROM part
      |window w1 AS (DISTRIBUTE BY p_mfgr SORT BY p_size range BETWEEN
      |2 preceding AND 2 following),
      |w2 AS w3,
      |w3 AS (DISTRIBUTE BY p_mfgr SORT BY p_size range BETWEEN
      |unbounded preceding AND current row)
    """.stripMargin, false)

  createQueryTest("34.testWindowDefinitionPropagation",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_size) OVER w1 AS s1,
      |sum(p_size) OVER w2 AS s2,
      |sum(p_size) OVER (w3 rows BETWEEN 2 preceding AND 2 following)  AS s3
      |FROM part
      |window w1 AS (DISTRIBUTE BY p_mfgr SORT BY p_size range BETWEEN
      |2 preceding AND 2 following),
      |w2 AS w3,
      |w3 AS (DISTRIBUTE BY p_mfgr SORT BY p_size range BETWEEN
      |unbounded preceding AND current row)
    """.stripMargin, false)

  createQueryTest("35.testDistinctWithWindowing",
    """
      |SELECT DISTINCT p_mfgr, p_name, p_size,
      |sum(p_size) OVER w1 AS s
      |FROM part
      |window w1 AS (DISTRIBUTE BY p_mfgr SORT BY p_name rows BETWEEN
      |2 preceding AND 2 following)
    """.stripMargin, false)

  createQueryTest("39.testUDFOnOrderCols",
    """
      |SELECT p_mfgr, p_type, substr(p_type, 2) AS short_ptype,
      |row_number() OVER (PARTITION BY p_mfgr ORDER BY substr(p_type, 2))  AS r
      |FROM part
    """.stripMargin, false)

  createQueryTest("40.testNoBetweenForRows",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_retailprice) OVER (DISTRIBUTE BY p_mfgr SORT BY p_name rows unbounded preceding) AS s1
      |FROM part
    """.stripMargin, false)

  createQueryTest("41.testNoBetweenForRange",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_retailprice) OVER (DISTRIBUTE BY p_mfgr SORT BY p_size range
      |unbounded preceding) AS s1
      |FROM part
    """.stripMargin, false)

  createQueryTest("42.testUnboundedFollowingForRows",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_retailprice) OVER (DISTRIBUTE BY p_mfgr SORT BY p_name rows BETWEEN
      |current row AND unbounded following) AS s1
      |FROM part
    """.stripMargin, false)

  createQueryTest("43.testUnboundedFollowingForRange",
    """
      |SELECT p_mfgr, p_name, p_size,
      |sum(p_retailprice) OVER (DISTRIBUTE BY p_mfgr SORT BY p_size range BETWEEN
      |current row AND unbounded following) AS s1
      |FROM part
    """.stripMargin, false)

  createQueryTest("44.testOverNoPartitionSingleAggregate",
    """
      |SELECT p_name, p_retailprice,
      |round(avg(p_retailprice) over(),2)
      |FROM part
      |ORDER BY p_name
    """.stripMargin, false)

  createQueryTest("ntile",
    """
      |SELECT p_name, ntile(4) OVER (PARTITION BY p_mfgr ORDER BY p_size) FROM part
    """.stripMargin, false)


}
