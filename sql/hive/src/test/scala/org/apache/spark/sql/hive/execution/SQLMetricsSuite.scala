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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

class SQLMetricsSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import spark.implicits._
  import org.apache.spark.sql.execution.metric.SQLMetricsSuite._

  test("writing data out metrics") {
    withTable("writeToHiveTable") {
      // Verifies the metrics of CreateHiveTableAsSelectCommand
      val executionId1 = getLatestExecutionId(spark) { () =>
        Seq((1, 2)).toDF("i", "j").write.format("hive").saveAsTable("writeToHiveTable")
      }
      // written 1 file, 1 row, 0 dynamic partition.
      val verifyFuncs1: Seq[Int => Boolean] = Seq(_ == 1, _ == 0, _ > 0, _ == 1, _ > 0)
      verifyWriteDataMetrics(spark, executionId1, verifyFuncs1)

      // Verifies the metrics of InsertIntoHiveTable
      val executionId2 = getLatestExecutionId(spark) { () =>
        Seq((3, 4), (5, 6), (7, 8)).toDF("i", "j").repartition(1)
          .write.format("hive").insertInto("writeToHiveTable")
      }
      // written 1 file, 3 rows, 0 dynamic partition.
      val verifyFuncs2: Seq[Int => Boolean] = Seq(_ == 1, _ == 0, _ > 0, _ == 3, _ > 0)
      verifyWriteDataMetrics(spark, executionId2, verifyFuncs2)

      val executionId3 = getLatestExecutionId(spark) { () =>
        Seq((9, 10), (11, 12)).toDF("i", "j").repartition(2)
          .write.format("hive").insertInto("writeToHiveTable")
      }
      // written 2 files, 2 rows, 0 dynamic partition.
      val verifyFuncs3: Seq[Int => Boolean] = Seq(_ == 2, _ == 0, _ > 0, _ == 2, _ > 0)
      verifyWriteDataMetrics(spark, executionId3, verifyFuncs3)
    }
  }

  test("writing data out metrics: dynamic partition") {
    withTempPath { dir =>
      spark.sql(
        s"""
           |CREATE TABLE t1(a int, b int)
           |USING hive
           |PARTITIONED BY(a)
           |LOCATION '${dir.toURI}'
         """.stripMargin)

      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
      assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

      val df = spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .selectExpr("id a", "id b")
      sql("SET hive.exec.dynamic.partition.mode=nonstrict")

      // Verifies the metrics of InsertIntoHiveTable
      val executionId1 = getLatestExecutionId(spark) { () =>
        df
          .write
          .format("hive")
          .option("fileFormat", "parquet")
          .option("maxRecordsPerFile", 1)
          .mode("overwrite")
          .insertInto("t1")
      }
      assert(Utils.recursiveList(dir).count(_.getName.startsWith("part-")) == 4)
      // written 4 files, 4 rows, 4 dynamic partitions.
      val verifyFuncs1: Seq[Int => Boolean] = Seq(_ == 4, _ == 4, _ > 0, _ == 4, _ > 0)
      verifyWriteDataMetrics(spark, executionId1, verifyFuncs1)

      val executionId2 = getLatestExecutionId(spark) { () =>
        df.union(df).repartition(2, $"a")
          .write
          .format("hive")
          .option("fileFormat", "parquet")
          .option("maxRecordsPerFile", 2)
          .mode("overwrite")
          .insertInto("t1")
      }
      assert(Utils.recursiveList(dir).count(_.getName.startsWith("part-")) == 4)
      // written 4 files, 8 rows, 4 dynamic partitions.
      val verifyFuncs2: Seq[Int => Boolean] = Seq(_ == 4, _ == 4, _ > 0, _ == 8, _ > 0)
      verifyWriteDataMetrics(spark, executionId2, verifyFuncs2)
    }
  }
}
