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

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Suite that tests the redaction of DataSourceScanExec
 */
class DataSourceScanExecRedactionSuite extends QueryTest with SharedSQLContext {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.redaction.string.regex", "file:/[\\w_]+")

  test("treeString is redacted") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
      val df = spark.read.parquet(basePath)

      val rootPath = df.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
        .asInstanceOf[FileSourceScanExec].relation.location.rootPaths.head
      assert(rootPath.toString.contains(dir.toURI.getPath.stripSuffix("/")))

      assert(!df.queryExecution.sparkPlan.treeString(verbose = true).contains(rootPath.getName))
      assert(!df.queryExecution.executedPlan.treeString(verbose = true).contains(rootPath.getName))
      assert(!df.queryExecution.toString.contains(rootPath.getName))
      assert(!df.queryExecution.simpleString.contains(rootPath.getName))

      val replacement = "*********"
      assert(df.queryExecution.sparkPlan.treeString(verbose = true).contains(replacement))
      assert(df.queryExecution.executedPlan.treeString(verbose = true).contains(replacement))
      assert(df.queryExecution.toString.contains(replacement))
      assert(df.queryExecution.simpleString.contains(replacement))
    }
  }

  private def isIncluded(queryExecution: QueryExecution, msg: String): Boolean = {
    queryExecution.toString.contains(msg) ||
    queryExecution.simpleString.contains(msg) ||
    queryExecution.stringWithStats.contains(msg)
  }

  test("explain is redacted using SQLConf") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
      val df = spark.read.parquet(basePath)
      val replacement = "*********"

      // Respect SparkConf and replace file:/
      assert(isIncluded(df.queryExecution, replacement))

      assert(isIncluded(df.queryExecution, "FileScan"))
      assert(!isIncluded(df.queryExecution, "file:/"))

      withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> "(?i)FileScan") {
        // Respect SQLConf and replace FileScan
        assert(isIncluded(df.queryExecution, replacement))

        assert(!isIncluded(df.queryExecution, "FileScan"))
        assert(isIncluded(df.queryExecution, "file:/"))
      }
    }
  }

  test("FileSourceScanExec metadata") {
    withTempPath { path =>
      val dir = path.getCanonicalPath
      spark.range(0, 10).write.parquet(dir)
      val df = spark.read.parquet(dir)

      assert(isIncluded(df.queryExecution, "Format"))
      assert(isIncluded(df.queryExecution, "ReadSchema"))
      assert(isIncluded(df.queryExecution, "Batched"))
      assert(isIncluded(df.queryExecution, "PartitionFilters"))
      assert(isIncluded(df.queryExecution, "PushedFilters"))
      assert(isIncluded(df.queryExecution, "DataFilters"))
      assert(isIncluded(df.queryExecution, "Location"))
    }
  }

}
