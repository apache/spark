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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{Literal, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, KeyedPartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.connector.catalog.{Column, Identifier}
import org.apache.spark.sql.connector.expressions.Expressions.identity
import org.apache.spark.sql.execution.{CoalesceExec, PartialReducerPartitionSpec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{CoalesceV2ScanPartitions, EnsureRequirements, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StringType}

class CoalesceV2ScanPartitionsSuite extends KeyGroupedPartitioningSuiteBase {
  private val enabled = SQLConf.V2_BUCKETING_COALESCE_SCAN_PARTITIONS_ENABLED.key

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)

  after {
    catalog.clearTables()
  }

  private def createSplits(numKeys: Int = 2, splitsPerKey: Int = 24): Unit = {
    createTable(items, itemsColumns, Array(identity("id")))
    val values = (1 to numKeys).flatMap { id =>
      (1 to splitsPerKey).map { split =>
        s"($id, 'split$split', 1.0, cast('2020-01-01' as timestamp))"
      }
    }.mkString(", ")
    sql(s"INSERT INTO testcat.ns.$items VALUES $values")
  }

  private def coalesces(plan: SparkPlan): Seq[CoalesceExec] =
    collect(plan) { case c: CoalesceExec => c }

  private def shuffle(df: DataFrame): ShuffleExchangeExec = {
    val exchanges = collect(df.queryExecution.executedPlan) { case s: ShuffleExchangeExec => s }
    assert(exchanges.size === 1)
    exchanges.head
  }

  test("SPARK-60265: scan coalescing defaults to disabled without an explicit setting") {
    createSplits(splitsPerKey = 3)
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      spark.sessionState.conf.unsetConf(enabled)
      assert(!spark.sessionState.conf.getAllConfs.contains(enabled))
      val entry = SQLConf.V2_BUCKETING_COALESCE_SCAN_PARTITIONS_ENABLED
      assert(!spark.sessionState.conf.getConf(entry))
      val df = sql(s"SELECT name, count(*), max(id) FROM testcat.ns.$items GROUP BY name")
      checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 2L, 2L)))
      assert(coalesces(df.queryExecution.executedPlan).isEmpty)
    }
  }

  test("SPARK-60265: coalesce the actual scan stage, retaining the partial aggregate and shuffle") {
    createSplits()
    for (adaptive <- Seq(false, true); coalesce <- Seq(false, true)) {
      withSQLConf(enabled -> coalesce.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val completed = ArrayBuffer.empty[(Int, Int)]
        val listener = new SparkListener {
          override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
            completed.synchronized {
              completed += ((event.stageInfo.stageId, event.stageInfo.numTasks))
            }
          }
        }
        spark.sparkContext.listenerBus.waitUntilEmpty(10000)
        spark.sparkContext.addSparkListener(listener)
        try {
          val df = sql(s"SELECT name, count(*), max(id) FROM testcat.ns.$items GROUP BY name")
          checkAnswer(df, (1 to 24).map(i => Row(s"split$i", 2L, 2L)))
          spark.sparkContext.listenerBus.waitUntilEmpty(10000)
          assert(completed.synchronized(completed.minBy(_._1)._2) ===
            (if (coalesce) 2 else 48))
          val exchange = shuffle(df)
          assert(coalesces(exchange).size === (if (coalesce) 1 else 0))
          val pipeline = exchange.child match {
            case c: CoalesceExec => c.child
            case other => other
          }
          assert(collect(pipeline) { case a: BaseAggregateExec => a }.nonEmpty)
          assert(collectScans(pipeline).head.inputRDD.partitions.length === 48)
          assert(ValidateRequirements.validate(stripAQEPlan(df.queryExecution.executedPlan)))
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }
      }
    }
  }

  test("SPARK-60265: scan coalescing limits AQE skew splitting to map-output boundaries") {
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      (1 to 9).map(price => s"($price, $price, NULL)").mkString(", "))
    for (numKeys <- Seq(1, 2)) {
      val table = s"skew_items_$numKeys"
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
        withSQLConf(enabled -> coalesce.toString,
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
          val numMaps = if (coalesce) numKeys else 48
          assert(exchange.numMappers === numMaps)
          assert(coalesces(exchange).map(_.numPartitions) ===
            (if (coalesce) Seq(numKeys) else Seq.empty))
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
          assert(rightStages.head.shuffle.numMappers === 9)
          assert(coalesces(rightStages.head.shuffle).isEmpty)
          assert(collectShuffles(df.queryExecution.executedPlan).size === 2)
        }
      }
    }
  }

  test("SPARK-60265: GenericInternalRow conversion survives filters, codegen and coalescing") {
    createSplits(splitsPerKey = 3)
    for (adaptive <- Seq(false, true); codegen <- Seq(false, true); filter <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
        val where = if (filter) " WHERE id > 0" else ""
        val direct = sql(s"SELECT id, name FROM testcat.ns.$items$where")
        checkAnswer(direct, for (id <- 1 to 2; split <- 1 to 3) yield Row(id, s"split$split"))
        assert(coalesces(direct.queryExecution.executedPlan).isEmpty)
        val scan = collectScans(direct.queryExecution.executedPlan).head
        val reader = scan.readerFactory.createReader(scan.inputPartitions.head)
        try {
          assert(reader.next())
          assert(!reader.get().isInstanceOf[UnsafeRow])
        } finally {
          reader.close()
        }
        val df = sql(s"SELECT name, count(*), max(id) FROM testcat.ns.$items$where GROUP BY name")
        checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 2L, 2L)))
        val exchange = shuffle(df)
        assert(coalesces(exchange).size === 1)
        assert(collect(exchange) { case p: ProjectExec => p }.nonEmpty)
        val rows = exchange.child.execute().collect()
        assert(rows.forall(_.isInstanceOf[UnsafeRow]))
      }
    }
  }

  test("SPARK-60265: repeated requirements planning retains exactly one coalesce") {
    createSplits(splitsPerKey = 3)
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(s"SELECT name, count(*), max(id) FROM testcat.ns.$items GROUP BY name")
      val once = df.queryExecution.executedPlan
      val twice = EnsureRequirements()(once)
      val thrice = EnsureRequirements()(twice)
      assert(coalesces(once).size === 1)
      assert(coalesces(twice).size === 1)
      assert(coalesces(thrice).size === 1)
      assert(ValidateRequirements.validate(thrice))
      assert(CoalesceV2ScanPartitions(thrice) === thrice)
      checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 2L, 2L)))
    }
  }

  test("SPARK-60265: pruning a composite key does not lower the full-key task target") {
    val columns = Array(Column.create("id", LongType), Column.create("hour", LongType),
      Column.create("data", StringType))
    createTable(items, columns, Array(identity("id"), identity("hour")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 0, 'x'), (1, 0, 'x'), (1, 1, 'x'), (1, 1, 'x'), " +
      "(2, 0, 'y'), (2, 0, 'y'), (2, 1, 'y'), (2, 1, 'y')")
    for (subset <- Seq(false, true); adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> subset.toString) {
        val df = sql(s"SELECT data, count(*), max(id) FROM testcat.ns.$items GROUP BY data")
        checkAnswer(df, Seq(Row("x", 4L, 1L), Row("y", 4L, 2L)))
        val exchange = shuffle(df)
        val c = coalesces(exchange).head
        assert(c.numPartitions === 4)
        assert(c.outputPartitioning === UnknownPartitioning(4))
        assert(c.outputOrdering.isEmpty)
        val kp = collectScans(exchange).head.outputPartitioning.asInstanceOf[KeyedPartitioning]
        assert(kp.isCollapsed)
        assert(!kp.isGrouped)
      }
    }
  }

  test("SPARK-60265: unique, empty, unpartitioned and disabled scans remain unchanged") {
    createSplits(splitsPerKey = 1)
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 1.0, cast('2020-01-01' as timestamp)), " +
      "(1, 2.0, cast('2020-01-01' as timestamp))")
    createTable("empty", itemsColumns, Array(identity("id")))
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val unique = sql(s"SELECT name, count(*), max(id) FROM testcat.ns.$items GROUP BY name")
        checkAnswer(unique, Seq(Row("split1", 2L, 2L)))
        assert(coalesces(unique.queryExecution.executedPlan).isEmpty)
        val unpartitioned = sql(s"SELECT price, count(*) FROM testcat.ns.$purchases GROUP BY price")
        checkAnswer(unpartitioned, Seq(Row(1.0, 1L), Row(2.0, 1L)))
        assert(coalesces(unpartitioned.queryExecution.executedPlan).isEmpty)
        val empty = sql("SELECT name, count(*), max(id) FROM testcat.ns.empty GROUP BY name")
        checkAnswer(empty, Seq.empty)
        assert(coalesces(empty.queryExecution.executedPlan).isEmpty)
      }
    }
    catalog.dropTable(Identifier.of(Array("ns"), items))
    createSplits(splitsPerKey = 3)
    withSQLConf(enabled -> "true", SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
      val df = sql(s"SELECT name, count(*), max(id) FROM testcat.ns.$items GROUP BY name")
      checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 2L, 2L)))
      assert(coalesces(df.queryExecution.executedPlan).isEmpty)
    }
  }

  test("SPARK-60265: one-key coalescing retains the required shuffle and complete answers") {
    createSplits(numKeys = 1, splitsPerKey = 3)
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT name, count(*), max(id) FROM testcat.ns.$items GROUP BY name")
        checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 1L, 1L)))
        val c = coalesces(shuffle(df)).head
        assert(c.numPartitions === 1)
        assert(c.outputPartitioning === SinglePartition)
      }
    }
  }

  test("SPARK-60265: null reported keys count as a distinct source key") {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(NULL, 'aa', 1.0, NULL), (NULL, 'bb', 2.0, NULL), " +
      "(1, 'aa', 3.0, NULL), (1, 'bb', 4.0, NULL)")
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT name, count(*), max(id) FROM testcat.ns.$items GROUP BY name")
        checkAnswer(df, Seq(Row("aa", 2L, 1L), Row("bb", 2L, 1L)))
        assert(coalesces(shuffle(df)).head.numPartitions === 2)
      }
    }
  }

  test("SPARK-60265: explicit repartition and coalesce keep their requested task sizing") {
    createSplits(splitsPerKey = 3)
    import testImplicits._
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val input = spark.table(s"testcat.ns.$items")
      Seq(input.repartition(5), input.repartition(5, $"name")).foreach { df =>
        checkAnswer(df, input)
        assert(coalesces(df.queryExecution.executedPlan).isEmpty)
      }
      val df = input.coalesce(3).groupBy("name").count()
      checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 2L)))
      assert(coalesces(df.queryExecution.executedPlan).map(_.numPartitions) === Seq(3))
    }
  }
}

/** Re-run all existing SPJ, ordering and reuse regressions with the compatibility option on. */
class KeyGroupedPartitioningCoalesceScanPartitionsSuite extends KeyGroupedPartitioningSuite {
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.V2_BUCKETING_COALESCE_SCAN_PARTITIONS_ENABLED, true)
}

class KeyGroupedPartitioningCoalesceRuntimeFilterSuite
  extends KeyGroupedPartitioningRuntimeFilterSuite with CoalesceV2ScanPartitionsRuntimeFilterTests {
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.V2_BUCKETING_COALESCE_SCAN_PARTITIONS_ENABLED, true)
}

class KeyGroupedPartitioningCoalesceCatalystRuntimeFilterSuite
  extends KeyGroupedPartitioningCatalystRuntimeFilterSuite
  with CoalesceV2ScanPartitionsRuntimeFilterTests {
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.V2_BUCKETING_COALESCE_SCAN_PARTITIONS_ENABLED, true)
}
