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

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.connector.expressions.{FieldReference, SortDirection}
import org.apache.spark.sql.connector.expressions.Expressions.{identity, sort}
import org.apache.spark.sql.execution.{QueryExecution, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{V2ScanPartitionPacking, V2TableWriteExec}
import org.apache.spark.sql.execution.exchange.{PackV2ScanPartitions, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

class PackV2ScanPartitionsPlanSuite extends KeyGroupedPartitioningSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED, true)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES, 0L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES, 300L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM, 1)

  after { catalog.clearTables() }

  private def populate(): Unit = {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'cc', 1.0, NULL), (1, 'bb', 2.0, NULL), (1, 'aa', 3.0, NULL), " +
      "(2, 'cc', 4.0, NULL), (2, 'bb', 5.0, NULL), (2, 'aa', 6.0, NULL)")
  }

  test("SPARK-60265: ordered V2 writes retain sorting over packed scans") {
    populate()
    for (adaptive <- Seq(false, true)) {
      val sink = s"sink_$adaptive"
      createTable(sink, itemsColumns, Array.empty, Array(
        sort(FieldReference("name"), SortDirection.ASCENDING)))
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val writePlan = new AtomicReference[SparkPlan]()
        val listener = new QueryExecutionListener {
          override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit = {
            if (collect(qe.executedPlan) { case w: V2TableWriteExec => w }.nonEmpty) {
              writePlan.set(qe.executedPlan)
            }
          }
          override def onFailure(name: String, qe: QueryExecution, exception: Exception): Unit = {}
        }
        spark.listenerManager.register(listener)
        try {
          spark.table(s"testcat.ns.$items").writeTo(s"testcat.ns.$sink").append()
          eventually(timeout(10.seconds), interval(10.milliseconds)) {
            assert(writePlan.get() != null)
          }
          val plan = writePlan.get()
          val scan = collectScans(plan).head
          assert(scan.partitionPacking.isDefined)
          assert(scan.inputRDD.getNumPartitions === 2)
          assert(collect(plan) { case s: SortExec => s }.nonEmpty)
          assert(ValidateRequirements.validate(stripAQEPlan(plan)))
          checkAnswer(spark.table(s"testcat.ns.$sink"), spark.table(s"testcat.ns.$items"))
        } finally {
          spark.listenerManager.unregister(listener)
        }
      }
    }
  }

  test("SPARK-60265: packing is part of scan equality and exchange reuse identity") {
    populate()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.table(s"testcat.ns.$items")
      checkAnswer(df, Seq(Row(1, "cc", 1.0f, null), Row(1, "bb", 2.0f, null),
        Row(1, "aa", 3.0f, null), Row(2, "cc", 4.0f, null), Row(2, "bb", 5.0f, null),
        Row(2, "aa", 6.0f, null)))
      val packed = collectScans(df.queryExecution.executedPlan).head
      val original = packed.copy(partitionPacking = None)
      val another = original.withPartitionPacking(
        packed.partitionPacking.get.options.copy(maxPartitionBytes = 200))
      assert(!packed.sameResult(original))
      assert(!packed.sameResult(another))
      assert(packed.sameResult(packed.copy()))
      val distribution = ClusteredDistribution(Seq(original.output.head))
      assert(PackV2ScanPartitions(Some(distribution))(original) === original)
    }
  }

  test("SPARK-60265: packing reuses already planned inputs across scan copies") {
    val df = spark.read.format(classOf[SizedPackingRowSource].getName)
      .option("sizes", "30,40,200,20,30,90").load()
    val original = collectScans(df.queryExecution.sparkPlan).head
    val batch = original.batch
    val inputs = original.inputPartitions
    val factory = original.readerFactory
    original.outputPartitioning
    SizedPackingRowReaderFactory.planned.set(0)
    SizedPackingRowReaderFactory.factories.set(0)
    val packed = original.withPartitionPacking(V2ScanPartitionPacking(100L, 0L, 1, None))
    Seq(packed, packed.copy(), packed.canonicalized).foreach { plan =>
      val scan = collectScans(plan).head
      assert(scan.batch eq batch)
      assert(scan.inputPartitions eq inputs)
      assert(scan.readerFactory eq factory)
      assert(scan.inputRDD.getNumPartitions === 4)
      assert(scan.inputRDD.count() === 21L)
    }
    assert(SizedPackingRowReaderFactory.planned.get() === 0)
    assert(SizedPackingRowReaderFactory.factories.get() === 0)
  }

  test("SPARK-60265: replacing Scan invalidates its captured inputs and batch") {
    def read(sizes: String): DataFrame = spark.read.format(classOf[SizedPackingRowSource].getName)
      .option("sizes", sizes).load()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val original = collectScans(read("30,40,200,20,30,90").queryExecution.executedPlan).head
      assert(original.partitionPacking.isDefined)
      val fresh = collectScans(read("10,20").queryExecution.sparkPlan).head
      // These fixture scans compare equal by schema; snapshots must use object identity.
      assert(original.scan == fresh.scan)
      assert(!(original.scan eq fresh.scan))
      val replaced = original.copy(scan = fresh.scan)
      assert(replaced.inputPartitions.size === 2)
      val rows = replaced.inputRDD.map(row => (row.getInt(0), row.getInt(1))).collect().toSeq
      assert(rows === Seq((0, 0), (1, 0), (1, 1)))
    }
  }

  test("SPARK-60265: explicit repartition keeps its output count while scan inputs are packed") {
    populate()
    import testImplicits._
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val input = spark.table(s"testcat.ns.$items")
      Seq(input.repartition(5), input.repartition(5, $"name")).foreach { df =>
        checkAnswer(df, input)
        assert(df.rdd.getNumPartitions === 5)
        val plan = df.queryExecution.executedPlan
        assert(collectScans(plan).head.inputRDD.getNumPartitions === 2)
        assert(collect(plan) { case s: ShuffleExchangeExec => s }.size === 1)
        assert(ValidateRequirements.validate(plan))
      }
    }
  }

  /** Keyed `items` with two splits each for keys 1 and 3, and keyed `purchases`. */
  private def createKeyedItemsAndPurchases(): Seq[Row] = {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, NULL), (1, 'ab', 41.0, NULL), (3, 'cc', 15.5, NULL), (3, 'cd', 16.5, NULL)")
    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (3, 19.5, NULL)")
    Seq(Row(1L, "aa", 42.0f), Row(1L, "ab", 42.0f), Row(3L, "cc", 19.5f), Row(3L, "cd", 19.5f))
  }

  private def joinWithPurchases(left: DataFrame): DataFrame = {
    import testImplicits._
    left.as("i").join(spark.table(s"testcat.ns.$purchases").as("p"), $"i.id" === $"p.item_id")
      .select($"i.id", $"i.name", $"p.price")
  }

  test("SPARK-60265: a cached keyed scan keeps its layout for later storage-partitioned joins") {
    val expected = createKeyedItemsAndPurchases()
    for (adaptive <- Seq(false, true); canChange <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> canChange.toString) {
        val input = spark.table(s"testcat.ns.$items")
        assert(collectScans(input.queryExecution.executedPlan).head.partitionPacking.isDefined,
          "test setup: without a consumer, the standalone scan is packed")
        input.cache()
        try {
          input.count()
          // The root of a cached plan has no consumer yet. Its keyed layout survives unless the
          // user lets a cached plan's output partitioning change.
          val cachedPlan = spark.sharedState.cacheManager.lookupCachedData(input).get
            .cachedRepresentation.cachedPlan
          assert(collectScans(cachedPlan).forall(_.partitionPacking.isDefined == canChange))
          val df = sql(s"SELECT i.id, i.name, p.price FROM testcat.ns.$items i " +
            s"JOIN testcat.ns.$purchases p ON i.id = p.item_id")
          checkAnswer(df, expected)
          val plan = df.queryExecution.executedPlan
          val shuffles = collect(plan) { case s: ShuffleExchangeExec => s }
          if (canChange) {
            assert(shuffles.nonEmpty, plan.treeString)
          } else {
            assert(shuffles.isEmpty, plan.treeString)
            assert(collectGroupPartitions(plan).nonEmpty, plan.treeString)
          }
        } finally {
          input.unpersist(blocking = true)
        }
      }
    }
  }

  test("SPARK-60265: checkpointed keyed scans keep their layout for later joins") {
    val expected = createKeyedItemsAndPurchases()
    withTempDir { dir =>
      spark.sparkContext.setCheckpointDir(dir.getPath)
      for (reliable <- Seq(true, false); adaptive <- Seq(false, true)) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val input = spark.table(s"testcat.ns.$items")
          assert(collectScans(input.queryExecution.executedPlan).head.partitionPacking.isDefined,
            "test setup: without a consumer, the standalone scan is packed")
          val checkpointed = if (reliable) input.checkpoint() else input.localCheckpoint()
          val df = joinWithPurchases(checkpointed)
          checkAnswer(df, expected)
          val plan = df.queryExecution.executedPlan
          withClue(s"reliable=$reliable adaptive=$adaptive\n${plan.treeString}") {
            assert(collect(plan) { case s: ShuffleExchangeExec => s }.isEmpty)
            assert(collectGroupPartitions(plan).nonEmpty)
          }
        }
      }
    }
  }
}
