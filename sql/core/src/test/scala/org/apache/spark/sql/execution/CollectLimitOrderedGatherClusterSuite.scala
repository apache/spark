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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite,
  TestUtils}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

/**
 * Runs the ordered `CollectLimitExec` gather on two executors. In local mode, all map outputs are
 * local and arrive in map index order. Here, when the map outputs are on both executors, the
 * gather also reads those of the other executor, which arrive after its own ones, so some map
 * outputs arrive before their turn.
 */
class CollectLimitOrderedGatherClusterSuite extends SparkFunSuite with LocalSparkContext {

  private val numRows = 20000L
  private val numPartitions = 20

  /** Returns the `CollectLimitExec` that `df` is planned with. */
  private def plannedCollectLimit(df: Dataset[_]): CollectLimitExec = {
    df.queryExecution.executedPlan match {
      case collectLimit: CollectLimitExec => collectLimit
      case other => fail(s"Expected a CollectLimitExec, found:\n$other")
    }
  }

  // With `readHostLocalDisk`, the map outputs of the other executor, which runs on the same host,
  // are read from its disk. Otherwise they are fetched from it.
  for (readHostLocalDisk <- Seq(true, false)) {
    test("SPARK-61296: ordered gather returns the sorted rows when map outputs arrive before " +
      s"their turn (readHostLocalDisk = $readHostLocalDisk)") {
      sc = new SparkContext(new SparkConf()
        .setMaster("local-cluster[2,1,1024]")
        .setAppName("test")
        .set("spark.shuffle.readHostLocalDisk", readHostLocalDisk.toString)
        // So that the tasks are spread over both executors, instead of waiting for the executor
        // that holds the most input.
        .set("spark.locality.wait", "0")
        .set("spark.shuffle.reduceLocality.enabled", "false"))
      // Otherwise the first executor to start could run all tasks, so that every map output would
      // be local and arrive in order.
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
      val spark = new SparkSession(sc)
      // The plan runs through `execute()` rather than through a query execution, which would make
      // the session active.
      spark.withActive {
        val conf = spark.sessionState.conf
        conf.setConf(SQLConf.COLLECT_LIMIT_ORDERED_GATHER_ENABLED, true)
        conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, false)
        conf.setConfString(SQLConf.SHUFFLE_PARTITIONS.key, numPartitions.toString)
        // So that a LIMIT + OFFSET of a sort is not planned as a top-K sort.
        conf.setConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD, 1000)

        val sorted = spark.range(0, numRows, 1, numPartitions).orderBy(col("id").desc)
        val ids = (0L until numRows).reverse
        for ((df, expected) <- Seq(
            sorted.offset(15000).limit(100) -> ids.slice(15000, 15100),
            sorted.offset(19900) -> ids.drop(19900))) {
          val collectLimit = plannedCollectLimit(df)
          assert(collectLimit.childMayBeOrdered)
          val rdd = collectLimit.execute()
          assert(rdd.map(_.getLong(0)).collect().toSeq === expected)
          // Some map outputs can only arrive before their turn if they are on both executors.
          val gather = rdd.dependencies.head.rdd.asInstanceOf[ShuffledRowRDD]
          val executorIds = SparkEnv.get.mapOutputTracker
            .getMapSizesByExecutorId(gather.dependency.shuffleId, 0)
            .map(_._1.executorId).toSet
          assert(executorIds.size === 2, s"Map outputs on executors $executorIds")
          // The map outputs that arrived before their turn were spooled.
          assert(collectLimit.metrics("orderedGatherSpoolSize").value > 0)
        }
      }
    }
  }
}
