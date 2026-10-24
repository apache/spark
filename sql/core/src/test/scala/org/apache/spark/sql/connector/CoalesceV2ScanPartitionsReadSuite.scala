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

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.{FieldReference, SortDirection, SortOrder}
import org.apache.spark.sql.connector.expressions.Expressions.{identity, sort}
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReader, PartitionReaderFactory, ScanBuilder, SupportsReportOrdering, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning}
import org.apache.spark.sql.execution.{CoalesceExec, SortExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

class CoalesceV2ScanPartitionsReadSuite extends KeyGroupedPartitioningSuiteBase {
  private val enabled = SQLConf.V2_BUCKETING_COALESCE_SCAN_PARTITIONS_ENABLED.key

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)

  after {
    catalog.clearTables()
  }

  private def populate(): Unit = {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 1.0, NULL), (1, 'bb', 2.0, NULL), (1, 'cc', 3.0, NULL), " +
      "(2, 'aa', 4.0, NULL), (2, 'bb', 5.0, NULL), (2, 'cc', 6.0, NULL)")
  }

  test("SPARK-60265: columnar readers keep reused batches correct through a shuffled pipeline") {
    withTempView("columnar") {
      spark.read.format(classOf[CoalescingColumnarScanSource].getName).load()
        .createOrReplaceTempView("columnar")
      val expected = Seq((1, 1, Seq(20, 21, 22, 10, 11)), (1, 2, Seq(5, 6)),
        (2, 3, Seq(30, 31))).flatMap { case (i, k, values) =>
        values.map(j => Row(j, 1L, i, k))
      }
      for (adaptive <- Seq(false, true); codegen <- Seq(false, true); filter <- Seq(false, true)) {
        withSQLConf(enabled -> "true",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
          val where = if (filter) " WHERE j > 10" else ""
          val df = sql("SELECT j, count(*), max(i), max(k) FROM columnar" + where + " GROUP BY j")
          val answers = if (filter) expected.filter(_.getInt(0) > 10) else expected
          checkAnswer(df, answers)
          checkAnswer(df, answers)
          val plan = df.queryExecution.executedPlan
          assert(collect(plan) { case c: CoalesceExec => c.numPartitions } === Seq(3))
          assert(collect(plan) { case s: ShuffleExchangeExec => s }.size === 1)
          assert(collectScans(plan).head.supportsColumnar)
          assert(ValidateRequirements.validate(stripAQEPlan(plan)))
          assert(sql("SELECT * FROM columnar LIMIT 1").collect().length === 1)
          checkAnswer(sql("SELECT j, count(*), max(i), max(k) FROM columnar GROUP BY j"), expected)
        }
      }
    }
  }

  test("SPARK-60265: local SORT BY retains the source's reported ordering") {
    createTable(items, itemsColumns, Array(identity("id")), Array(
      sort(FieldReference("id"), SortDirection.ASCENDING),
      sort(FieldReference("name"), SortDirection.ASCENDING)))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 1.0, NULL), (1, 'bb', 2.0, NULL), (2, 'cc', 3.0, NULL)")
    for (adaptive <- Seq(false, true); preserve <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> preserve.toString) {
        val plans = Seq(false, true).map { coalesce =>
          withSQLConf(enabled -> coalesce.toString) {
            val df = sql(s"SELECT id, name FROM testcat.ns.$items SORT BY id, name")
            checkAnswer(df, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc")))
            val plan = df.queryExecution.executedPlan
            assert(collect(plan) { case c: CoalesceExec => c }.isEmpty)
            assert(collect(plan) { case s: ShuffleExchangeExec => s }.isEmpty)
            collect(plan) { case s: SortExec => s }.size
          }
        }
        assert(plans === Seq(0, 0))
      }
    }
  }

  test("SPARK-60265: range exchanges retain complete global ordering") {
    populate()
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT id, name FROM testcat.ns.$items ORDER BY name, id")
        assert(df.collect().toSeq === Seq(Row(1, "aa"), Row(2, "aa"), Row(1, "bb"),
          Row(2, "bb"), Row(1, "cc"), Row(2, "cc")))
        val plan = df.queryExecution.executedPlan
        assert(collect(plan) { case c: CoalesceExec => c.numPartitions } === Seq(2))
        assert(collect(plan) { case s: ShuffleExchangeExec => s }.size === 1)
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
      }
    }
  }

  test("SPARK-60265: collect, write, broadcast and union do not acquire scan coalescing") {
    populate()
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val input = spark.table(s"testcat.ns.$items")
      assert(input.collect().length === 6)
      assert(collect(input.queryExecution.executedPlan) { case c: CoalesceExec => c }.isEmpty)
      val broadcast = sql(s"SELECT /*+ BROADCAST(p) */ i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON i.id = p.item_id")
      checkAnswer(broadcast, for (id <- 1 to 2; name <- Seq("aa", "bb", "cc")) yield {
        Row(id, name, if (id == 1) 42.0f else 10.0f)
      })
      assert(collect(broadcast.queryExecution.executedPlan) { case c: CoalesceExec => c }.isEmpty)
      val union = input.union(input).groupBy("name").count()
      checkAnswer(union, Seq(Row("aa", 4L), Row("bb", 4L), Row("cc", 4L)))
      assert(collect(union.queryExecution.executedPlan) { case c: CoalesceExec => c }.isEmpty)
      withTempPath { dir =>
        input.write.parquet(dir.getCanonicalPath)
        checkAnswer(spark.read.parquet(dir.getCanonicalPath), input)
        val files = Files.list(dir.toPath)
        try {
          assert(files.filter(_.getFileName.toString.startsWith("part-")).count() === 6)
        } finally {
          files.close()
        }
      }
    }
  }

  test("SPARK-60265: a shuffle above an SPJ does not coalesce the join's keyed inputs") {
    populate()
    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    for (adaptive <- Seq(false, true); partial <- Seq(false, true)) {
      withSQLConf(enabled -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> partial.toString) {
        val df = sql(s"SELECT /*+ MERGE(i, p) */ i.name, count(*), max(i.id) " +
          s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON i.id = p.item_id " +
          "GROUP BY i.name")
        checkAnswer(df, Seq(Row("aa", 2L, 2L), Row("bb", 2L, 2L), Row("cc", 2L, 2L)))
        val plan = df.queryExecution.executedPlan
        assert(collect(plan) { case c: CoalesceExec => c }.isEmpty)
        assert(collect(plan) { case s: ShuffleExchangeExec => s }.size === 1)
        assert(collectGroupPartitions(plan).nonEmpty)
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
      }
    }
  }

  test("SPARK-60265: coalesced exchanges remain reusable and execute repeatedly") {
    populate()
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {
        val query = s"SELECT name, count(*), max(id) FROM testcat.ns.$items GROUP BY name"
        val df = sql(s"($query) UNION ALL ($query)")
        val expected = Seq(Row("aa", 2L, 2L), Row("bb", 2L, 2L), Row("cc", 2L, 2L))
        checkAnswer(df, expected ++ expected)
        checkAnswer(df, expected ++ expected)
        val plan = df.queryExecution.executedPlan
        assert(collect(plan) { case r: ReusedExchangeExec => r }.nonEmpty)
        assert(collect(plan) { case c: CoalesceExec => c.numPartitions }.toSet === Set(2))
      }
    }
  }
}

trait CoalesceV2ScanPartitionsRuntimeFilterTests extends KeyGroupedPartitioningSuiteBase {
  test("SPARK-60265: adaptive runtime filtering retains coalescing and pruned input slots") {
    createTable(items, itemsColumns, Array(identity("id")))
    createTable(purchases, purchasesColumns, Array.empty)
    val values = (1 to 2).flatMap { id =>
      (1 to 24).map(split => s"($id, 'split$split', 1.0, NULL)")
    }.mkString(", ")
    sql(s"INSERT INTO testcat.ns.$items VALUES $values")
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    for (adaptive <- Seq(false, true); price <- Seq(40, 100)) {
      withSQLConf(SQLConf.V2_BUCKETING_COALESCE_SCAN_PARTITIONS_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
          SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10",
          // Keep the fully pruned scan visible; adaptive runtime-filter planning stays enabled.
          SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key ->
            "org.apache.spark.sql.execution.adaptive.AQEPropagateEmptyRelation") {
        val df = sql(s"SELECT i.id, i.name, p.price FROM testcat.ns.$items i " +
          s"JOIN testcat.ns.$purchases p ON i.id = p.item_id WHERE p.price > $price")
        val expected = if (price == 40) (1 to 24).map(i => Row(1, s"split$i", 42.0)) else Nil
        checkAnswer(df, expected)
        val plan = df.queryExecution.executedPlan
        assert(collectShuffles(plan).size === 2)
        val scan = collectScans(plan).find(_.table.name().endsWith(items)).get
        assert(scan.runtimeFilters.nonEmpty)
        assert(scan.filteredPartitions.size === 48)
        assert(scan.filteredPartitions.count(_.isEmpty) === (if (price == 40) 24 else 48))
        val c = collect(plan) {
          case c: CoalesceExec if collectScans(c).exists(_.table.name().endsWith(items)) => c
        }.head
        assert(c.numPartitions === 2)
        val rowsPerTask = c.execute().mapPartitions(rows => Iterator.single(rows.size)).collect()
        assert(rowsPerTask.length === 2)
        assert(rowsPerTask.sum === (if (price == 40) 24 else 0))
        assert(c.outputOrdering.isEmpty)
      }
    }
  }
}

/** A columnar-only source that reuses two-row batches, including a short final batch. */
class CoalescingColumnarScanSource extends TestingV2Source {
  private val sourceSchema = TestingV2Source.schema.add("k", IntegerType)

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = sourceSchema

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def schema(): StructType = sourceSchema

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      new SimpleScanBuilder with SupportsReportPartitioning with SupportsReportOrdering {
        override def readSchema(): StructType = sourceSchema

        override def planInputPartitions(): Array[InputPartition] = Array(
          CoalescingColumnarInputPartition(1, 1, Array(20, 21, 22)),
          CoalescingColumnarInputPartition(1, 1, Array(10, 11)),
          CoalescingColumnarInputPartition(1, 2, Array(5, 6)),
          CoalescingColumnarInputPartition(2, 3, Array(30, 31)))

        override def outputPartitioning(): Partitioning =
          new KeyGroupedPartitioning(Array(FieldReference("i"), FieldReference("k")), 4)

        override def outputOrdering(): Array[SortOrder] = Array(
          sort(FieldReference("i"), SortDirection.ASCENDING),
          sort(FieldReference("j"), SortDirection.ASCENDING))

        override def createReaderFactory(): PartitionReaderFactory = CoalescingColumnarReaderFactory
      }
  }
}

case class CoalescingColumnarInputPartition(i: Int, k: Int, values: Array[Int])
    extends InputPartition with HasPartitionKey {
  override def partitionKey(): InternalRow = InternalRow(i, k)
}

object CoalescingColumnarReaderFactory extends PartitionReaderFactory {
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("This source supports only columnar readers")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val input = partition.asInstanceOf[CoalescingColumnarInputPartition]
    new PartitionReader[ColumnarBatch] {
      private val vectors = Array.fill(3)(new OnHeapColumnVector(2, IntegerType))
      private val batch = new ColumnarBatch(Array(vectors(0), vectors(1), vectors(2)))
      private var offset = 0

      override def next(): Boolean = {
        vectors.foreach(_.reset())
        val count = math.min(2, input.values.length - offset)
        (0 until count).foreach { index =>
          vectors(0).putInt(index, input.i)
          vectors(1).putInt(index, input.values(offset + index))
          vectors(2).putInt(index, input.k)
        }
        offset += count
        batch.setNumRows(count)
        count > 0
      }

      override def get(): ColumnarBatch = batch
      override def close(): Unit = batch.close()
    }
  }
}
