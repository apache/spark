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

package org.apache.spark.sql.hive

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, Not}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.QueryTest
import org.apache.spark.util.Utils

class QueryPartitionSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import spark.implicits._

  test("SPARK-5068: query data when path doesn't exist") {
    withSQLConf((SQLConf.HIVE_VERIFY_PARTITION_PATH.key, "true")) {
      val testData = sparkContext.parallelize(
        (1 to 10).map(i => TestData(i, i.toString))).toDF()
      testData.createOrReplaceTempView("testData")

      val tmpDir = Files.createTempDir()
      // create the table for test
      sql(s"CREATE TABLE table_with_partition(key int,value string) " +
        s"PARTITIONED by (ds string) location '${tmpDir.toURI}' ")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='1') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='2') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='3') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='4') " +
        "SELECT key,value FROM testData")

      // test for the exist path
      checkAnswer(sql("select key,value from table_with_partition"),
        testData.toDF.collect ++ testData.toDF.collect
          ++ testData.toDF.collect ++ testData.toDF.collect)

      // delete the path of one partition
      tmpDir.listFiles
        .find { f => f.isDirectory && f.getName().startsWith("ds=") }
        .foreach { f => Utils.deleteRecursively(f) }

      // test for after delete the path
      checkAnswer(sql("select key,value from table_with_partition"),
        testData.toDF.collect ++ testData.toDF.collect ++ testData.toDF.collect)

      sql("DROP TABLE IF EXISTS table_with_partition")
      sql("DROP TABLE IF EXISTS createAndInsertTest")
    }
  }

  test("partition pruning in disjunction") {
    withSQLConf((SQLConf.HIVE_VERIFY_PARTITION_PATH.key, "true")) {
      val testData = sparkContext.parallelize(
        (1 to 10).map(i => TestData(i, i.toString))).toDF()
      testData.createOrReplaceTempView("testData")

      val testData2 = sparkContext.parallelize(
        (11 to 20).map(i => TestData(i, i.toString))).toDF()
      testData2.createOrReplaceTempView("testData2")

      val testData3 = sparkContext.parallelize(
        (21 to 30).map(i => TestData(i, i.toString))).toDF()
      testData3.createOrReplaceTempView("testData3")

      val testData4 = sparkContext.parallelize(
        (31 to 40).map(i => TestData(i, i.toString))).toDF()
      testData4.createOrReplaceTempView("testData4")

      val tmpDir = Files.createTempDir()
      // create the table for test
      sql(s"CREATE TABLE table_with_partition(key int,value string) " +
        s"PARTITIONED by (ds string, ds2 string) location '${tmpDir.toURI.toString}' ")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='1', ds2='d1') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='2', ds2='d1') " +
        "SELECT key,value FROM testData2")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='3', ds2='d3') " +
        "SELECT key,value FROM testData3")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='4', ds2='d4') " +
        "SELECT key,value FROM testData4")

      checkAnswer(sql("select key,value from table_with_partition"),
        testData.collect ++ testData2.collect ++ testData3.collect ++ testData4.collect)

      sql("CREATE TABLE createAndInsertTest AS SELECT * FROM table_with_partition")
      checkAnswer(sql("select key,value from createAndInsertTest"),
        testData.collect ++ testData2.collect ++ testData3.collect ++ testData4.collect)

      checkAnswer(
        sql(
          """select key,value from table_with_partition
            | where (ds='4' and key=38) or (ds='3' and key=22)""".stripMargin),
        sql(
          """select key,value from createAndInsertTest
            | where (ds='4' and key=38) or (ds='3' and key=22)""".stripMargin).collect())

      checkAnswer(
        sql(
          """select key,value from table_with_partition
            | where (key<40 and key>38) or (ds='3' and key=22)""".stripMargin),
        sql(
          """select key,value from createAndInsertTest
            | where (key<40 and key>38) or (ds='3' and key=22)""".stripMargin).collect())

      checkAnswer(
        sql(
          """select key,value from table_with_partition
            | where !(key = 4 and ds > 35)""".stripMargin),
        sql(
          """select key,value from createAndInsertTest
            | where !(key = 4 and ds > 35)""".stripMargin).collect())

      sql("DROP TABLE table_with_partition")
      sql("DROP TABLE IF EXISTS createAndInsertTest")
    }
  }

  test("extract partition expression from disjunction") {
    def check(partitionKeys: AttributeSet, predicate: Expression, expected: Option[Expression])
    : Unit = {
      val answer = PhysicalOperation.extractPartitionKeyExpression(predicate, partitionKeys)
      assert(expected === answer, s"Expected: ${expected} but got ${answer} for ${predicate}")
    }

    val part1 = AttributeReference("part1", IntegerType, true)()
    val part2 = AttributeReference("part2", IntegerType, true)()
    val a = AttributeReference("a", IntegerType, true)()
    val b = AttributeReference("b", IntegerType, true)()
    val c = AttributeReference("c", IntegerType, true)()

    val partitionKeys = AttributeSet(part1 :: part2 :: Nil)

    val p1 = ((part1 === 1) && (a > 3)) || ((part2 === 2) && (a < 5))
    check(partitionKeys, p1, Some((part1 === 1) || (part2 === 2)))

    val p2 = ((part1 === 1) && (a > 3)) || (a < 100)
    check(partitionKeys, p2, None)

    val p3 = ((a > 100) && b < 100) || (part1 === 10)
    check(partitionKeys, p3, None)

    val p4 = ((a > 100) && b < 100) && (part1 === 10)
    check(partitionKeys, p4, Some(part1 === 10))

    val p5 = ((a > 100) && (b < 100) && (part1 === 10)) || (part1 === 2)
    check(partitionKeys, p5, Some((part1 === 10) || (part1 === 2)))

    val p6 = ((a > 100) && b < 100) && (part1 === 10 || part2 === 5)
    check(partitionKeys, p6, Some(part1 === 10 || part2 === 5))

    val p7 = !(part1 === 100 && a > 3)
    check(partitionKeys, p7, Some(Not(part1 === 100)))
  }
}
