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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SparkPlanSuite extends QueryTest with SharedSparkSession {

  test("SPARK-21619 execution of a canonicalized plan should fail") {
    val plan = spark.range(10).queryExecution.executedPlan.canonicalized

    intercept[IllegalStateException] { plan.execute() }
    intercept[IllegalStateException] { plan.executeCollect() }
    intercept[IllegalStateException] { plan.executeCollectPublic() }
    intercept[IllegalStateException] { plan.executeToIterator() }
    intercept[IllegalStateException] { plan.executeBroadcast() }
    intercept[IllegalStateException] { plan.executeTake(1) }
    intercept[IllegalStateException] { plan.executeTail(1) }
  }

  test("SPARK-23731 plans should be canonicalizable after being (de)serialized") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(1).write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        val fileSourceScanExec =
          df.queryExecution.sparkPlan.collectFirst { case p: FileSourceScanExec => p }.get
        val serializer = SparkEnv.get.serializer.newInstance()
        val readback =
          serializer.deserialize[FileSourceScanExec](serializer.serialize(fileSourceScanExec))
        try {
          readback.canonicalized
        } catch {
          case e: Throwable => fail("FileSourceScanExec was not canonicalizable", e)
        }
      }
    }
  }

  test("SPARK-27418 BatchScanExec should be canonicalizable after being (de)serialized") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        spark.range(1).write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        val batchScanExec =
          df.queryExecution.sparkPlan.collectFirst { case p: BatchScanExec => p }.get
        val serializer = SparkEnv.get.serializer.newInstance()
        val readback =
          serializer.deserialize[BatchScanExec](serializer.serialize(batchScanExec))
        try {
          readback.canonicalized
        } catch {
          case e: Throwable => fail("BatchScanExec was not canonicalizable", e)
        }
      }
    }
  }

  test("SPARK-25357 SparkPlanInfo of FileScan contains nonEmpty metadata") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(5).write.parquet(path.getAbsolutePath)
        val f = spark.read.parquet(path.getAbsolutePath)
        assert(SparkPlanInfo.fromSparkPlan(f.queryExecution.sparkPlan).metadata.nonEmpty)
      }
    }
  }

  test("SPARK-30780 empty LocalTableScan should use RDD without partitions") {
    assert(LocalTableScanExec(Nil, Nil).execute().getNumPartitions == 0)
  }
}
