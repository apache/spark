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
import org.apache.spark.sql.internal.SQLConf
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

  test("file listing time - analysis rules") {
    withTable("src") {
      sql("CREATE TABLE src (key INT, value STRING) using parquet")
      // File listing will be triggered in FindDataSourceTable rule.
      val df = sql("SELECT * FROM src")
      val tracker = df.queryExecution.tracker
      assert(tracker.phases.keySet ==
        Set("parsing", "fileListing", "analysis"))
      assert(tracker.rules.nonEmpty)
    }
  }

  test("file listing time - optimization rules") {
    withTable("tbl") {
      spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("tbl")
      // File listing will be triggered in PruneFileSourcePartitions rule.
      val df = sql("SELECT * FROM tbl WHERE p = 1")
      df.collect()
      checkTrackerWithPhaseKeySet(
        df,
        Set("parsing", "analysis", "optimization", "planning", "fileListing"))

      // File listing will be triggered in OptimizeMetadataOnlyQuery rule.
      withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "true") {
        val df = sql("SELECT p FROM tbl GROUP BY p")
        df.collect()
        checkTrackerWithPhaseKeySet(
          df,
          Set("parsing", "analysis", "optimization", "planning", "fileListing"))
      }

      withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "false") {
        val df = sql("SELECT p FROM tbl GROUP BY p")
        df.collect()
        checkTrackerWithPhaseKeySet(
          df,
          Set("parsing", "analysis", "optimization", "planning"))
      }
    }
  }

  test("file listing time - DataFrameWriter") {
    withTable("tbl") {
      val df = spark.range(10).selectExpr("id")
      // File listing will be triggered by DataSource.resolveRelation
      df.write.saveAsTable("tbl")
      checkTrackerWithPhaseKeySet(df, Set("analysis", "fileListing"))
    }
  }

  test("file listing time - DataFrameReader") {
    withTempPath { path =>
      val pathStr = path.getAbsolutePath
      spark.range(0, 10).write.parquet(pathStr)
      // File listing will be triggered by DataSource.resolveRelation
      val df = spark.read.parquet(pathStr)
      checkTrackerWithPhaseKeySet(
        df,
        Set("analysis", "fileListing"))
    }
  }

}
