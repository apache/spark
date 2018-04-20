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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

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
}
