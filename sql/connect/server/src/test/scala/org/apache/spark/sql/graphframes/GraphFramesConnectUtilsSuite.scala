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

package org.apache.spark.sql.graphframes

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.StorageLevel

class GraphFramesConnectUtilsSuite extends QueryTest with SharedSparkSession {
  private def withCheckpointCleanup(f: => Unit): Unit = {
    val before = spark.sparkContext.getPersistentRDDs.keySet
    val checkpointDir = spark.sparkContext.checkpointDir
    try {
      f
    } finally {
      spark.sparkContext.checkpointDir = checkpointDir
      // Production uses Spark's normal checkpoint cleanup; tests release their blocks eagerly.
      spark.sparkContext.getPersistentRDDs.foreach { case (id, rdd) =>
        if (!before.contains(id)) {
          rdd.unpersist(blocking = true)
        }
      }
    }
  }

  test("without a directory the result uses a disk-only local checkpoint") {
    withCheckpointCleanup {
      spark.sparkContext.checkpointDir = None
      val output = spark.range(3).toDF().cache()
      val result = GraphFramesConnectUtils.materializeResult(spark) { output }
      val rdds = result.queryExecution.analyzed.collect { case logical: LogicalRDD =>
        logical.rdd
      }
      assert(rdds.nonEmpty)
      assert(rdds.forall(_.isCheckpointedAndMaterialized))
      assert(rdds.forall(_.getCheckpointFile.isEmpty))
      assert(rdds.forall(_.getStorageLevel == StorageLevel.DISK_ONLY))
      assert(spark.sharedState.cacheManager.lookupCachedData(output).isEmpty)
      assert(result.storageLevel == StorageLevel.NONE)
      assert(result.cache().count() == 3)
      result.unpersist(blocking = true)
      assert(result.count() == 3)
    }
  }

  test("a configured directory uses a reliable checkpoint") {
    withTempDir { dir =>
      withCheckpointCleanup {
        spark.sparkContext.setCheckpointDir(dir.getAbsolutePath)
        val result = GraphFramesConnectUtils.materializeResult(spark) { spark.range(3).toDF() }
        val rdds = result.queryExecution.analyzed.collect { case logical: LogicalRDD =>
          logical.rdd
        }
        assert(rdds.nonEmpty)
        assert(rdds.forall(_.getCheckpointFile.nonEmpty))
        assert(result.storageLevel == StorageLevel.NONE)
        assert(result.count() == 3)
      }
    }
  }

  test("checkpoint failure releases the algorithm output cache") {
    withCheckpointCleanup {
      spark.sparkContext.checkpointDir = None
      val output = spark.range(1).selectExpr("raise_error('checkpoint failed') AS id").cache()
      intercept[Exception] {
        GraphFramesConnectUtils.materializeResult(spark) { output }
      }
      assert(spark.sharedState.cacheManager.lookupCachedData(output).isEmpty)
    }
  }
}
