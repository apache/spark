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

package org.apache.spark.sql.connector

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.connector.expressions.Expressions.identity
import org.apache.spark.sql.execution.PartialReducerPartitionSpec
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

class PackV2ScanPartitionsAQESuite extends KeyGroupedPartitioningSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)

  after { catalog.clearTables() }

  test("SPARK-60265: scan packing respects AQE skew map-output boundaries") {
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      (1 to 9).map(price => s"($price, $price, NULL)").mkString(", "))
    for (targetMaps <- Seq(1, 2)) {
      val numKeys = 1
      val table = s"skew_items_$targetMaps"
      createTable(table, itemsColumns, Array(identity("id")))
      // Forty large rows share the join key; eight small reducer populations establish skew.
      // Seeded, incompressible payloads keep the byte thresholds independent of compression.
      val expected = (0 until 48).map { index =>
        val id = (index % numKeys + 1).toLong
        val price = if (index < 40) 1 else index - 38
        val payload = new Random(index).alphanumeric.take(16384).mkString
        Row(id, payload, price.toFloat, price.toLong)
      }
      sql(s"INSERT INTO testcat.ns.$table VALUES " + expected.map { row =>
        s"(${row.getLong(0)}, '${row.getString(1)}', ${row.getFloat(2)}, NULL)"
      }.mkString(", "))
      for (coalesce <- Seq(false, true)) {
        withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key -> coalesce.toString,
            SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key ->
              (4800 / targetMaps).toString,
            SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES.key -> "0",
            SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM.key -> "1",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
            SQLConf.LOCAL_SHUFFLE_READER_ENABLED.key -> "false",
            SQLConf.SKEW_JOIN_ENABLED.key -> "true",
            SQLConf.SHUFFLE_PARTITIONS.key -> "8",
            SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR.key -> "2",
            SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "65536",
            SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "32768") {
          val df = sql(s"SELECT /*+ MERGE(i, p) */ i.id, i.name, i.price, p.item_id " +
            s"FROM testcat.ns.$table i JOIN testcat.ns.$purchases p ON i.price = p.price")
          checkAnswer(df, expected)
          val joins = collect(df.queryExecution.executedPlan) { case j: SortMergeJoinExec => j }
          assert(joins.size === 1)
          val join = joins.head
          val stages = collect(join.left) { case s: ShuffleQueryStageExec => s }
          assert(stages.size === 1)
          val exchange = stages.head.shuffle.asInstanceOf[ShuffleExchangeExec]
          val numMaps = if (coalesce) targetMaps else 48
          assert(exchange.numMappers === numMaps)
          assert(collectScans(exchange).head.inputRDD.getNumPartitions === numMaps)
          assert(collectScans(exchange).head.inputPartitions.size === 48)
          val reducer = HashPartitioning(Seq(Literal(1.0f)), 8)
            .partitionIdExpression.eval().asInstanceOf[Int]
          assert(stages.head.mapStats.get.bytesByPartitionId(reducer) > 65536)
          val parts = collect(join.left) { case r: AQEShuffleReadExec => r }
            .flatMap(_.partitionSpecs.collect {
              case p: PartialReducerPartitionSpec if p.reducerIndex == reducer => p
            }).sortBy(_.startMapIndex)
          if (numMaps == 1) {
            // The reducer is large, but AQE cannot subdivide its only map output.
            assert(parts.isEmpty)
            assert(!join.isSkewJoin)
          } else {
            assert(join.isSkewJoin)
            assert(parts.size > 1 && parts.size <= numMaps)
            assert(parts.head.startMapIndex === 0)
            assert(parts.last.endMapIndex === numMaps)
            assert(parts.forall(p => p.startMapIndex < p.endMapIndex && p.dataSize > 0))
            assert(parts.zip(parts.tail).forall { case (a, b) =>
              a.endMapIndex == b.startMapIndex
            })
            if (coalesce) assert(parts.size === 2) else assert(parts.size > 2)
          }
          val rightStages = collect(join.right) { case s: ShuffleQueryStageExec => s }
          assert(rightStages.size === 1)
          assert(rightStages.head.shuffle.numMappers === (if (coalesce) 1 else 9))
          assert(collectShuffles(df.queryExecution.executedPlan).size === 2)
        }
      }
    }
  }

}
