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

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.config.SHUFFLE_MANAGER
import org.apache.spark.shuffle.{BaseShuffleHandle, BlockStoreShuffleReader, ShuffleHandle,
  ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Runs the `CollectLimitExec` gather with a shuffle reader that returns the map outputs in reverse
 * map index order. In local mode, the map outputs otherwise arrive in map index order, so the
 * unordered gather would return the same rows as the ordered one.
 */
class CollectLimitOrderedGatherReversedReadSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SHUFFLE_MANAGER.key, classOf[ReversedBlockOrderShuffleManager].getName)

  /** Collects the long values of `df` through `CollectLimitExec.doExecute`. */
  private def executeCollect(df: DataFrame): Seq[Long] =
    df.queryExecution.executedPlan.execute().map(_.getLong(0)).collect().toSeq

  /** Collects the long values of `df` through `toLocalIterator`. */
  private def localIteratorCollect(df: DataFrame): Seq[Long] =
    df.toLocalIterator().asScala.map(_.getLong(0)).toList

  for (aqe <- Seq(false, true)) {
    test("SPARK-61296: ordered gather returns the sorted rows when map outputs arrive in " +
      s"reverse (AQE = $aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.SHUFFLE_PARTITIONS.key -> "8",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        // So that a LIMIT of a sort is not planned as a top-K sort.
        SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "1") {
        val ids = (0L until 80L).reverse
        for ((name, query, expected) <- Seq[(String, DataFrame => DataFrame, Seq[Long])](
            ("OFFSET", _.offset(19), ids.drop(19)),
            ("OFFSET + LIMIT", _.offset(10).limit(30), ids.slice(10, 40)),
            ("LIMIT", _.limit(30), ids.take(30)))) {
          // Plans the query again for each conf, as a plan keeps some of the conf it ran with.
          def sorted: DataFrame = spark.range(0, 80, 1, 8).toDF().orderBy(col("id").desc)
          withSQLConf(SQLConf.COLLECT_LIMIT_ORDERED_GATHER_ENABLED.key -> "true") {
            assert(executeCollect(query(sorted)) === expected, name)
            assert(localIteratorCollect(query(sorted)) === expected, name)
          }
          // The unordered gather returns the map outputs in the order they arrive, here in reverse,
          // so it returns other rows.
          withSQLConf(SQLConf.COLLECT_LIMIT_ORDERED_GATHER_ENABLED.key -> "false") {
            assert(executeCollect(query(sorted)) !== expected, name)
          }
        }
      }
    }
  }
}

/**
 * A sort-based shuffle manager whose readers list the blocks of each address in reverse map index
 * order. In local mode, all blocks are local and are read in the order they are listed, so the map
 * outputs arrive in reverse map index order.
 */
private[spark] class ReversedBlockOrderShuffleManager(conf: SparkConf)
    extends SortShuffleManager(conf) {

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val blocksByAddress = SparkEnv.get.mapOutputTracker
      .getMapSizesByExecutorId(
        handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
      .map { case (address, blocks) => (address, blocks.reverse) }
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context, metrics)
  }
}
