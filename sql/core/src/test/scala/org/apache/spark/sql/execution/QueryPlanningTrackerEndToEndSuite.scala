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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SharedSQLContext

class QueryPlanningTrackerEndToEndSuite extends SharedSQLContext {

  private def checkTrackerWithPhaseKeySet(df: DataFrame, keySet: Set[String]) = {
    val tracker = df.queryExecution.tracker
    assert(tracker.phases.keySet == keySet)
    assert(tracker.rules.nonEmpty)
  }

  test("programmatic API") {
    val df = spark.range(1000).selectExpr("count(*)")
    df.collect()
    checkTrackerWithPhaseKeySet(df, Set("analysis", "optimization", "planning"))
  }

  test("sql") {
    val df = spark.sql("select * from range(1)")
    df.collect()
    checkTrackerWithPhaseKeySet(df, Set("parsing", "analysis", "optimization", "planning"))
  }

  test("file listing time in schema resolving") {
    withTempPath { path =>
      val pathStr = path.getAbsolutePath
      spark.range(0, 10).write.parquet(pathStr)
      val df = spark.read.parquet(pathStr)
      checkTrackerWithPhaseKeySet(
        df,
        Set("analysis", "fileListing"))
    }
  }

  test("file listing time in execution") {
    withTable("src") {
      sql("CREATE TABLE src (key INT, value STRING) using parquet")
      val df = spark.read.table("src")
      df.collect()
      val tracker = df.queryExecution.tracker
      assert(tracker.phases.keySet ==
        Set("planning", "optimization", "analysis", "fileListing"))
      assert(tracker.rules.nonEmpty)
    }
  }

  test("partition pruning time and file listing time") {
    withTable("tbl") {
      spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("tbl")
      val df = sql("SELECT * FROM tbl WHERE p = 1")
      df.collect()
      checkTrackerWithPhaseKeySet(
        df,
        Set("parsing", "analysis", "optimization", "planning", "fileListing", "partitionPruning"))
    }
  }

}
