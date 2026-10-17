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

import org.apache.spark.{NarrowDependency, Partition, SparkConf, SparkFunSuite}
import org.apache.spark.rdd.{CartesianPartition, RDD}
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ProjectExec, RDDScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.{CartesianProductExec, UnsafeCartesianRDD}
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingDeduplicateExec
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.state.{BaseStateStoreRDD, RocksDBStateStoreProvider}
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

  test("SPARK-59877: constant-key join over a stateful dedup plans a Cartesian product that " +
    "fans one state partition out to independently scheduled tasks") {
    withSparkSession(SparkSession.builder().config(getSparkConf()).getOrCreate()) { spark =>
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
        val plan = stripAQEPlan(
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan)

        val cartesians = collect(plan) { case c: CartesianProductExec => c }
        assert(cartesians.nonEmpty, s"expected a CartesianProductExec, got plan:\n$plan")
        val cartesian = cartesians.head

        assert(collect(cartesian.left) { case d: StreamingDeduplicateExec => d }.nonEmpty,
          s"expected a StreamingDeduplicateExec under the Cartesian left, got:\n${cartesian.left}")

        val rightPartitions = cartesian.right.execute().getNumPartitions
        assert(rightPartitions >= 2,
          s"expected >= 2 right partitions so Cartesian output fans out to shared state " +
            s"partitions, got $rightPartitions")

        val rightWithLocations = spark.sparkContext.makeRDD[InternalRow](
          (0 until rightPartitions).map { partitionId =>
            (InternalRow.empty, Seq(s"right-$partitionId"))
          })
        val wrappedLeft = ProjectExec(cartesian.left.output, cartesian.left)
        val locationAwareCartesian = cartesian.copy(
          left = wrappedLeft,
          right = RDDScanExec(cartesian.right.output, rightWithLocations, "right-with-locations"))
        val cartesianRdd = locationAwareCartesian.execute().dependencies.head.rdd
          .asInstanceOf[UnsafeCartesianRDD]
        val siblingPartitions = cartesianRdd.partitions.take(rightPartitions)
        val firstSibling = siblingPartitions.head.asInstanceOf[CartesianPartition]
        assert(cartesianRdd.rdd1.preferredLocations(firstSibling.s1).isEmpty,
          "expected the ProjectExec RDD wrapper to hide its parent's preferred location")
        def stateStoreLocations(rdd: RDD[_], partition: Partition): Seq[String] = rdd match {
          case _: BaseStateStoreRDD[_, _] => rdd.preferredLocations(partition)
          case _ =>
            rdd.dependencies.iterator.collect { case dependency: NarrowDependency[_] => dependency }
              .flatMap { dependency =>
                dependency.getParents(partition.index).iterator.map { parentIndex =>
                  stateStoreLocations(dependency.rdd, dependency.rdd.partitions(parentIndex))
                }
              }
              .find(_.nonEmpty)
              .getOrElse(Nil)
        }
        val leftLocations = stateStoreLocations(cartesianRdd.rdd1, firstSibling.s1).distinct
        assert(leftLocations.nonEmpty, "expected the stateful left input to have a location")
        siblingPartitions.foreach { partition =>
          assert(cartesianRdd.preferredLocations(partition) === leftLocations)
        }
      } finally {
        query.stop()
      }
    }
  }
}
