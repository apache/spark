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

import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.CartesianProductExec
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingDeduplicateExec
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.internal.SQLConf

/**
 * End to end: a memory-sink query that joins a stateful dedup with a four-partition static side
 * on a constant key, which plans a Cartesian product in the stateful stage. Without the fix each
 * of the 8 state partitions is loaded by 4 sibling tasks at once, and the query fails with
 * CANNOT_LOAD_STATE_STORE.UNRELEASED_THREAD_ERROR. With the fix a shuffle ends the stateful stage,
 * each state partition is computed by one task, and the query succeeds.
 */
class StreamingStatefulCartesianEndToEndSuite
  extends SparkFunSuite with AdaptiveSparkPlanHelper with StatefulCartesianErrorChecks {

  private val statePartitions = 8
  private val staticPartitions = 4

  private def sparkConf(fixEnabled: Boolean): SparkConf = new SparkConf()
    .setMaster("local[2]")
    .set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      classOf[RocksDBStateStoreProvider].getCanonicalName)
    .set(SQLConf.SHUFFLE_PARTITIONS.key, statePartitions.toString)
    // Force CartesianProductExec rather than a BroadcastNestedLoopJoin of the static side.
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    // A sibling waiting on an already-loaded instance fails fast instead of after 120s.
    .set("spark.sql.streaming.stateStore.rocksdb.lockAcquireTimeoutMs", "300")
    .set(SQLConf.STATEFUL_OPERATOR_SHUFFLE_BEFORE_CARTESIAN_PRODUCT_ENABLED.key,
      fixEnabled.toString)

  /** Runs two batches; returns the last executed plan and numTasks of every state-loading stage. */
  private def runQuery(fixEnabled: Boolean): (CartesianProductExec, Seq[Int]) = {
    withSparkSession(SparkSession.builder().config(sparkConf(fixEnabled)).getOrCreate()) { spark =>
      import spark.implicits._

      val statefulStageTasks = mutable.ArrayBuffer.empty[Int]
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
          if (event.stageInfo.rddInfos.exists(_.name == "StateStoreRDD")) {
            statefulStageTasks.synchronized { statefulStageTasks += event.stageInfo.numTasks }
          }
        }
      })

      val input = MemoryStream[Int](spark)
      val left = input.toDS().dropDuplicates().withColumn("k", lit(1))
      val right = spark.range(0, 8, 1, numPartitions = staticPartitions).withColumn("k", lit(1))
      // Each joined row sleeps, so a task holds its state store longer than the lock timeout. The
      // udf is non-deterministic so the filter stays above the join instead of being pushed onto
      // the static side, where it would run before the state store is opened.
      val slow = udf((_: Long) => { Thread.sleep(500); true }).asNondeterministic()
      val query = left.join(right, "k").filter(slow(col("id"))).writeStream
        .format("memory")
        .outputMode("append")
        .queryName("sc245406_e2e")
        .start()
      try {
        input.addData(0 until 5: _*)
        query.processAllAvailable()
        // Duplicates of batch 0 must be dropped by the state loaded in batch 1.
        input.addData(3 until 10: _*)
        query.processAllAvailable()

        val rows = spark.table("sc245406_e2e").select("value", "id").as[(Int, Long)].collect()
        assert(rows.sorted.toSeq === (for (v <- 0 until 10; id <- 0L until 8L) yield (v, id)))

        val plan = stripAQEPlan(
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan)
        val cartesian = collectFirst(plan) { case c: CartesianProductExec => c }.getOrElse(
          fail(s"expected a CartesianProductExec, got plan:\n$plan"))
        assert(collect(cartesian.left) { case d: StreamingDeduplicateExec => d }.nonEmpty,
          s"expected a StreamingDeduplicateExec under the Cartesian left, got:\n${cartesian.left}")
        spark.sparkContext.listenerBus.waitUntilEmpty()
        (cartesian, statefulStageTasks.synchronized(statefulStageTasks.toSeq))
      } finally {
        query.stop()
      }
    }
  }

  test("fix disabled: sibling tasks time out loading the same state partition") {
    checkUnreleasedThreadError(intercept[StreamingQueryException](runQuery(fixEnabled = false)))
  }

  test("fix enabled: every state partition is computed by exactly one task") {
    val (cartesian, tasks) = runQuery(fixEnabled = true)
    assert(cartesian.left.isInstanceOf[ShuffleExchangeExec],
      s"expected a shuffle under the Cartesian left:\n$cartesian")
    assert(cartesian.left.outputPartitioning.numPartitions == statePartitions,
      s"expected one shuffle partition per state partition:\n$cartesian")
    assert(tasks === Seq.fill(2)(statePartitions), s"task counts: $tasks")
  }
}
