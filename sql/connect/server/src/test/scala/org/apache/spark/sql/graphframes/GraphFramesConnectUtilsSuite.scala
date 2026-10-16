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

import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.connect.proto.{graphframes => proto, GraphFramesCommand}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.StorageLevel

class GraphFramesConnectUtilsSuite extends QueryTest with SharedSparkSession {
  private def command(local: Boolean): GraphFramesCommand = {
    GraphFramesCommand
      .newBuilder()
      .setViewName("_graphframes_cc_" + UUID.randomUUID().toString.replace("-", ""))
      .setApi(
        proto.GraphFramesAPI
          .newBuilder()
          .setConnectedComponents(
            proto.ConnectedComponents.newBuilder().setUseLocalCheckpoints(local)))
      .build()
  }

  private def withResult(request: GraphFramesCommand)(f: => Unit): Unit = {
    val before = spark.sparkContext.getPersistentRDDs.keySet
    val checkpointDir = spark.sparkContext.checkpointDir
    try {
      f
    } finally {
      spark.sparkContext.checkpointDir = checkpointDir
      spark.catalog.dropTempView(request.getViewName)
      // Production uses Spark's normal checkpoint cleanup; tests release their blocks eagerly.
      spark.sparkContext.getPersistentRDDs.foreach { case (id, rdd) =>
        if (!before.contains(id)) {
          rdd.unpersist(blocking = true)
        }
      }
    }
  }

  test("without a directory the result uses a disk-only local checkpoint") {
    val request = command(local = false)
    val name = request.getViewName
    var runs = 0
    withResult(request) {
      spark.sparkContext.checkpointDir = None
      for (_ <- 1 to 2) {
        GraphFramesConnectUtils.materializeResult(spark, request) {
          runs += 1
          spark.range(3).toDF().cache()
        }
      }
      assert(runs == 1)
      val rdds = spark.sessionState.catalog.getRawTempView(name).get.plan.get.collect {
        case logical: LogicalRDD => logical.rdd
      }
      assert(rdds.nonEmpty)
      assert(rdds.forall(_.getCheckpointFile.isEmpty))
      assert(rdds.forall(_.getStorageLevel == StorageLevel.DISK_ONLY))
      val result = spark.table(name)
      assert(!spark.catalog.isCached(name))
      assert(result.cache().count() == 3)
      result.unpersist(blocking = true)
      assert(result.count() == 3)
      assert(spark.catalog.dropTempView(name))
      assert(!spark.catalog.tableExists(name))
    }
  }

  test("a configured directory uses reliable checkpoints even with local intermediates") {
    withTempDir { dir =>
      val request = command(local = true)
      withResult(request) {
        spark.sparkContext.setCheckpointDir(dir.getAbsolutePath)
        val root = new Path(spark.sparkContext.getCheckpointDir.get)
        val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
        GraphFramesConnectUtils.materializeResult(spark, request) { spark.range(3).toDF() }
        val rdds = spark.sessionState.catalog
          .getRawTempView(request.getViewName)
          .get
          .plan
          .get
          .collect { case logical: LogicalRDD => logical.rdd }
        assert(rdds.nonEmpty)
        assert(rdds.forall(_.getCheckpointFile.nonEmpty))
        assert(spark.table(request.getViewName).count() == 3)
        val checkpoints = fs.listStatus(root).map(_.getPath)
        assert(checkpoints.nonEmpty)
        assert(spark.catalog.dropTempView(request.getViewName))
        assert(checkpoints.forall(fs.exists))
      }
    }
  }

  test("result names cannot overwrite an unrelated view or another request") {
    val request = command(local = true)
    val name = request.getViewName
    withResult(request) {
      spark.range(1).createTempView(name)
      intercept[IllegalArgumentException] {
        GraphFramesConnectUtils.materializeResult(spark, request) {
          fail("Existing views must not be overwritten")
        }
      }
      assert(spark.catalog.dropTempView(name))
      GraphFramesConnectUtils.materializeResult(spark, request) { spark.range(3).toDF() }
      val other = request.toBuilder
        .setApi(
          proto.GraphFramesAPI
            .newBuilder()
            .setConnectedComponents(
              proto.ConnectedComponents.newBuilder().setUseLocalCheckpoints(true).setMaxIter(5)))
        .build()
      intercept[IllegalArgumentException] {
        GraphFramesConnectUtils.materializeResult(spark, other) {
          fail("A different request must not reuse the name")
        }
      }
      assert(spark.table(name).count() == 3)
    }
  }

  test("checkpoint failure releases output caches and does not publish a view") {
    val request = command(local = true)
    val before = spark.sparkContext.getPersistentRDDs.keySet
    val output = spark.range(1).selectExpr("raise_error('checkpoint failed') AS id").cache()
    withResult(request) {
      spark.sparkContext.checkpointDir = None
      intercept[Exception] {
        GraphFramesConnectUtils.materializeResult(spark, request) { output }
      }
      assert(!spark.catalog.tableExists(request.getViewName))
      assert(spark.sparkContext.getPersistentRDDs.keySet == before)
      assert(spark.sharedState.cacheManager.lookupCachedData(output).isEmpty)
    }
  }
}
