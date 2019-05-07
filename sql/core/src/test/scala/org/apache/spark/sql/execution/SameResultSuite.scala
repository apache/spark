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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.IntegerType

/**
 * Tests for the sameResult function for [[SparkPlan]]s.
 */
class SameResultSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("FileSourceScanExec: different orders of data filters and partition filters") {
    withTempPath { path =>
      val tmpDir = path.getCanonicalPath
      spark.range(10)
        .selectExpr("id as a", "id + 1 as b", "id + 2 as c", "id + 3 as d")
        .write
        .partitionBy("a", "b")
        .parquet(tmpDir)
      val df = spark.read.parquet(tmpDir)
      // partition filters: a > 1 AND b < 9
      // data filters: c > 1 AND d < 9
      val plan1 = getFileSourceScanExec(df.where("a > 1 AND b < 9 AND c > 1 AND d < 9"))
      val plan2 = getFileSourceScanExec(df.where("b < 9 AND a > 1 AND d < 9 AND c > 1"))
      assert(plan1.sameResult(plan2))
    }
  }

  test("FileScan: different orders of data filters and partition filters") {
    withSQLConf(SQLConf.USE_V1_SOURCE_READER_LIST.key -> "") {
      Seq("orc", "json", "csv").foreach { format =>
        withTempPath { path =>
          val tmpDir = path.getCanonicalPath
          spark.range(10)
            .selectExpr("id as a", "id + 1 as b", "id + 2 as c", "id + 3 as d")
            .write
            .partitionBy("a", "b")
            .format(format)
            .option("header", true)
            .save(tmpDir)
          val df = spark.read.format(format).option("header", true).load(tmpDir)
          // partition filters: a > 1 AND b < 9
          // data filters: c > 1 AND d < 9
          val plan1 = df.where("a > 1 AND b < 9 AND c > 1 AND d < 9").queryExecution.sparkPlan
          val plan2 = df.where("b < 9 AND a > 1 AND d < 9 AND c > 1").queryExecution.sparkPlan
          assert(plan1.sameResult(plan2))
          val scan1 = getBatchScanExec(plan1)
          val scan2 = getBatchScanExec(plan2)
          assert(scan1.sameResult(scan2))
          val plan3 = df.where("b < 9 AND a > 1 AND d < 8 AND c > 1").queryExecution.sparkPlan
          assert(!plan1.sameResult(plan3))
          // The [[FileScan]]s should have different results if they support filter pushdown.
          if (format == "orc") {
            val scan3 = getBatchScanExec(plan3)
            assert(!scan1.sameResult(scan3))
          }
        }
      }
    }
  }

  test("TextScan") {
    withSQLConf(SQLConf.USE_V1_SOURCE_READER_LIST.key -> "") {
      withTempPath { path =>
        val tmpDir = path.getCanonicalPath
        spark.range(10)
          .selectExpr("id as a", "id + 1 as b", "cast(id as string) value")
          .write
          .partitionBy("a", "b")
          .text(tmpDir)
        val df = spark.read.text(tmpDir)
        // partition filters: a > 1 AND b < 9
        // data filters: c > 1 AND d < 9
        val plan1 = df.where("a > 1 AND b < 9 AND value == '3'").queryExecution.sparkPlan
        val plan2 = df.where("value == '3' AND a > 1 AND b < 9").queryExecution.sparkPlan
        assert(plan1.sameResult(plan2))
        val scan1 = getBatchScanExec(plan1)
        val scan2 = getBatchScanExec(plan2)
        assert(scan1.sameResult(scan2))
      }
    }
  }

  private def getBatchScanExec(plan: SparkPlan): BatchScanExec = {
    plan.find(_.isInstanceOf[BatchScanExec]).get.asInstanceOf[BatchScanExec]
  }

  private def getFileSourceScanExec(df: DataFrame): FileSourceScanExec = {
    df.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
      .asInstanceOf[FileSourceScanExec]
  }

  test("SPARK-20725: partial aggregate should behave correctly for sameResult") {
    val df1 = spark.range(10).agg(sum($"id"))
    val df2 = spark.range(10).agg(sum($"id"))
    assert(df1.queryExecution.executedPlan.sameResult(df2.queryExecution.executedPlan))

    val df3 = spark.range(10).agg(sumDistinct($"id"))
    val df4 = spark.range(10).agg(sumDistinct($"id"))
    assert(df3.queryExecution.executedPlan.sameResult(df4.queryExecution.executedPlan))
  }

  test("Canonicalized result is case-insensitive") {
    val a = AttributeReference("A", IntegerType)()
    val b = AttributeReference("B", IntegerType)()
    val planUppercase = Project(Seq(a), LocalRelation(a, b))

    val c = AttributeReference("a", IntegerType)()
    val d = AttributeReference("b", IntegerType)()
    val planLowercase = Project(Seq(c), LocalRelation(c, d))

    assert(planUppercase.sameResult(planLowercase))
  }
}
