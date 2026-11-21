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

package org.apache.spark.sql.streaming

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.rdd.{CartesianPartition, RDD}
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ProjectExec, RDDScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{CartesianProductExec, UnsafeCartesianRDD}
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingDeduplicateExec
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf

/**
 * Verifies that a constant-key Cartesian join prefers the stateful left input's location.
 */
class StreamingCartesianStateStorePlacementSuite
  extends SparkFunSuite with AdaptiveSparkPlanHelper {

  private def getSparkConf(): SparkConf = {
    new SparkConf()
      .setMaster("local[2]")
      .set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
        classOf[RocksDBStateStoreProvider].getCanonicalName)
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "8")
      // Force CartesianProductExec rather than a BroadcastNestedLoopJoin of the static side.
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
  }

  gridTest("SPARK-59877: Cartesian state-store locality with checkpoint format")(Seq(1, 2)) {
    checkpointFormat =>
    val builder = SparkSession.builder().config(getSparkConf())
      .config(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key, checkpointFormat)
    withSparkSession(builder.getOrCreate()) { spark =>
      import spark.implicits._

      val input = MemoryStream[Int](spark)
      val left = input.toDS().dropDuplicates().withColumn("k", lit(1))
      val right = spark.range(0, 8, 1, numPartitions = 4).withColumn("k", lit(1))
      val joined = left.join(right, "k")

      val query = joined.writeStream
        .format("memory")
        .outputMode("append")
        .queryName("spark59877_out")
        .start()

      try {
        input.addData((0 until 5): _*)
        query.processAllAvailable()
        input.addData((3 until 8): _*)
        query.processAllAvailable()
        assert(spark.table("spark59877_out").count() === 64)
        val streamingQuery = query.asInstanceOf[StreamingQueryWrapper].streamingQuery
        val plan = stripAQEPlan(streamingQuery.lastExecution.executedPlan)

        val cartesians = collect(plan) { case c: CartesianProductExec => c }
        assert(cartesians.nonEmpty, s"expected a CartesianProductExec, got plan:\n$plan")
        val cartesian = cartesians.head

        assert(collect(cartesian.left) { case d: StreamingDeduplicateExec => d }.nonEmpty,
          s"expected a StreamingDeduplicateExec under the Cartesian left, got:\n${cartesian.left}")

        val rightPartitions = cartesian.right.execute().getNumPartitions
        assert(rightPartitions >= 2,
          s"expected >= 2 right partitions so Cartesian output fans out to shared state " +
            s"partitions, got $rightPartitions")

        val stateInfo = collect(cartesian.left) {
          case dedup: StreamingDeduplicateExec => dedup.stateInfo.get
        }.head
        val coordinator = streamingQuery.sparkSession.streams.stateStoreCoordinator
        val sc = spark.sparkContext
        def checkLocations(plan: CartesianProductExec): Unit = {
          val rdd = plan.execute().dependencies.head.rdd.asInstanceOf[UnsafeCartesianRDD]
          rdd.partitions.take(rightPartitions).foreach { partition =>
            val leftPartition = partition.asInstanceOf[CartesianPartition].s1.index
            val providerId = StateStoreProviderId(
              StateStoreId(stateInfo.checkpointLocation, stateInfo.operatorId, leftPartition),
              stateInfo.queryRunId)
            val expected = coordinator.getLocation(providerId).toSeq
            assert(expected.nonEmpty, "expected the coordinator to track the stateful partition")
            assert(sc.dagScheduler.getPreferredLocs(rdd, partition.index).map(_.toString) ===
              expected)
          }
        }
        checkLocations(cartesian)

        val rightWithLocations = sc.makeRDD[InternalRow](
          (0 until rightPartitions).map { partitionId =>
            (InternalRow.empty, Seq(s"right-$partitionId"))
          })
        // Keep the synthetic locations visible at the Cartesian input, without scan wrappers.
        val rightScan = new RDDScanExec(
          cartesian.right.output, rightWithLocations, "right-locations") {
          override protected def doExecute(): RDD[InternalRow] = rightWithLocations
        }
        val wrappedLeft = ProjectExec(cartesian.left.output, cartesian.left)
        checkLocations(cartesian.copy(left = wrappedLeft, right = rightScan))

        val leftWithLocations = sc.makeRDD(Seq((InternalRow.empty, Seq("left"))))
        // An exchange with its own locality distinguishes preference from the default union.
        val exchange = new ShuffleExchangeExec(SinglePartition, wrappedLeft) {
          override protected def doExecute(): RDD[InternalRow] = leftWithLocations
        }
        val exchangedRdd = cartesian.copy(left = exchange, right = rightScan)
          .execute().dependencies.head.rdd
        assert(sc.dagScheduler.getPreferredLocs(exchangedRdd, 0).map(_.host) ===
          Seq("left", "right-0"))

        val leftScan = new RDDScanExec(
          cartesian.left.output, leftWithLocations, "left-locations") {
          override protected def doExecute(): RDD[InternalRow] = leftWithLocations
        }
        val stateless = cartesian.copy(left = leftScan, right = rightScan)
        val statelessRdd = stateless.execute().dependencies.head.rdd
        assert(sc.dagScheduler.getPreferredLocs(statelessRdd, 0).map(_.host) ===
          Seq("left", "right-0"))
      } finally {
        query.stop()
      }
    }
  }
}
