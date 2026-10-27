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
import java.util.OptionalLong
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.{FieldReference, SortDirection, SortOrder}
import org.apache.spark.sql.connector.expressions.Expressions.{identity, sort}
import org.apache.spark.sql.connector.read.{HasPartitionKey, HasPartitionStatistics, InputPartition, PartitionReader, PartitionReaderFactory, ScanBuilder, SupportsReportOrdering, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning}
import org.apache.spark.sql.execution.{CoalesceExec, SortExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

class PackV2ScanPartitionsReadSuite extends KeyGroupedPartitioningSuiteBase {
  private val enabled = SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES, 0L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES, 300L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM, 1)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM, 2)

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
          assert(collectScans(plan).head.inputRDD.getNumPartitions === 2)
          assert(collect(plan) { case s: ShuffleExchangeExec => s }.size === 1)
          assert(collectScans(plan).head.supportsColumnar)
          assert(ValidateRequirements.validate(stripAQEPlan(plan)))
          assert(sql("SELECT * FROM columnar LIMIT 1").collect().length === 1)
          checkAnswer(sql("SELECT j, count(*), max(i), max(k) FROM columnar GROUP BY j"), expected)
        }
      }
    }
  }

  test("SPARK-60265: real unkeyed readers honor byte bounds and the missing-size fallback") {
    withTempView("sized") {
      def read(sizes: String): Unit = {
        spark.read.format(classOf[SizedPackingRowSource].getName).option("sizes", sizes).load()
          .createOrReplaceTempView("sized")
      }
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key -> "100") {
        read("30,40,200,20,30,90")
        val df = sql("SELECT * FROM sized")
        val expected = (0 until 6).flatMap(i => (0 to i).map(j => Row(i, j)))
        checkAnswer(df, expected)
        val plan = df.queryExecution.executedPlan
        val counts = plan.execute().mapPartitions(rows => Iterator.single(rows.size)).collect()
        assert(counts.toSeq === Seq(3, 3, 9, 6))
        val scan = collectScans(plan).head
        assert(scan.partitionPacking.isDefined)
        val hosts = scan.inputRDD.partitions.map { partition =>
          scan.inputRDD.preferredLocations(partition).head
        }
        assert(hosts.toSeq === Seq("worker-a", "worker-b", "worker-a", "worker-b"))

        read("?,20,-1,40,?")
        withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key -> "3") {
          val capped = sql("SELECT * FROM sized")
          checkAnswer(capped, (0 until 5).flatMap(i => (0 to i).map(j => Row(i, j))))
          val sizes = capped.queryExecution.executedPlan.execute()
            .mapPartitions(rows => Iterator.single(rows.size)).collect()
          assert(sizes.toSeq === Seq(1, 5, 9))
        }
        val conf = spark.sessionState.conf
        val cap = conf.getConf(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM)
        conf.unsetConf(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key)
        try {
          val unchanged = sql("SELECT * FROM sized")
          checkAnswer(unchanged, (0 until 5).flatMap(i => (0 to i).map(j => Row(i, j))))
          assert(collectScans(unchanged.queryExecution.executedPlan).head.partitionPacking.isEmpty)
        } finally {
          cap.foreach { value =>
            conf.setConfString(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key,
              value.toString)
          }
        }
      }
    }
  }

  test("SPARK-60265: packed columnar readers close on early stop and task failure") {
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.read.format(classOf[CoalescingColumnarScanSource].getName).load()
      val scan = collectScans(df.queryExecution.executedPlan).head
      assert(scan.partitionPacking.isDefined)
      CoalescingColumnarReaderFactory.opened.set(0)
      CoalescingColumnarReaderFactory.closed.set(0)
      assert(scan.inputRDD.mapPartitions(_.take(1)).count() === 2)
      assert(CoalescingColumnarReaderFactory.opened.get() === 2)
      assert(CoalescingColumnarReaderFactory.closed.get() === 2)
      intercept[SparkException] {
        scan.inputRDD.mapPartitions { rows =>
          if (rows.hasNext) throw new IllegalStateException("intentional reader failure")
          rows
        }.count()
      }
      assert(CoalescingColumnarReaderFactory.opened.get() > 2)
      eventually(timeout(10.seconds), interval(10.milliseconds)) {
        assert(CoalescingColumnarReaderFactory.closed.get() ===
          CoalescingColumnarReaderFactory.opened.get())
      }
    }
  }

  test("SPARK-60265: reported ordering continues to eliminate a redundant local SORT BY") {
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

  test("SPARK-60265: a necessary local sort remains correct over packed inputs") {
    populate()
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(s"SELECT id, name FROM testcat.ns.$items SORT BY name DESC, id DESC")
      checkAnswer(df, for (id <- 1 to 2; name <- Seq("cc", "bb", "aa")) yield Row(id, name))
      val plan = df.queryExecution.executedPlan
      assert(collectScans(plan).head.partitionPacking.isDefined)
      assert(collect(plan) { case s: SortExec => s }.size === 1)
      assert(ValidateRequirements.validate(plan))
      val ordered = plan.execute().mapPartitions { rows =>
        val values = rows.map(row => (row.getUTF8String(1).toString, row.getLong(0))).toVector
        Iterator.single(values == values.sorted.reverse)
      }.collect()
      assert(ordered.forall(value => value))
    }
  }

  test("SPARK-60265: a broadcast streamed layout needed by an aggregate stays unchanged") {
    populate()
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    for (adaptive <- Seq(false, true)) {
      val counts = Seq(false, true).map { packing =>
        withSQLConf(enabled -> packing.toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          val df = sql(s"SELECT /*+ BROADCAST(p) */ i.id, count(*), max(p.price) " +
            s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON i.id = p.item_id " +
            "GROUP BY i.id")
          checkAnswer(df, Seq(Row(1, 3L, 42.0f), Row(2, 3L, 10.0f)))
          val plan = df.queryExecution.executedPlan
          val scan = collectScans(plan).find(_.table.name().endsWith(items)).get
          assert(scan.partitionPacking.isEmpty)
          assert(ValidateRequirements.validate(stripAQEPlan(plan)))
          collect(plan) { case s: ShuffleExchangeExec => s }.size
        }
      }
      assert(counts.distinct.size === 1)
    }
  }

  test("SPARK-60265: compatible two-sided SPJ keeps keyed groups and complete join alignment") {
    populate()
    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, NULL), (1, 43.0, NULL), (2, 10.0, NULL), (2, 11.0, NULL)")
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT /*+ MERGE(i, p) */ i.id, i.name, p.price " +
          s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON i.id = p.item_id")
        val expected = for (id <- 1 to 2; name <- Seq("aa", "bb", "cc");
            price <- (if (id == 1) Seq(42.0f, 43.0f) else Seq(10.0f, 11.0f))) yield {
          Row(id, name, price)
        }
        checkAnswer(df, expected)
        val plan = df.queryExecution.executedPlan
        assert(collectShuffles(plan).isEmpty)
        assert(collectScans(plan).forall(_.partitionPacking.isEmpty))
        val groups = collectGroupPartitions(plan)
        assert(groups.size === 2)
        val taskRows = groups.map { group =>
          group.execute().mapPartitions(rows => Iterator.single(rows.size)).collect().toSeq
        }.toSet
        assert(taskRows === Set(Seq(3, 3), Seq(2, 2)))
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
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
        assert(collectScans(plan).head.inputRDD.getNumPartitions === 2)
        assert(collect(plan) { case s: ShuffleExchangeExec => s }.size === 1)
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
      }
    }
  }

  test("SPARK-60265: collect, write and broadcast pack scans; union stays conservative") {
    populate()
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val input = spark.table(s"testcat.ns.$items")
      assert(input.collect().length === 6)
      assert(collectScans(input.queryExecution.executedPlan).head.partitionPacking.isDefined)
      val broadcast = sql(s"SELECT /*+ BROADCAST(p) */ i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON i.id = p.item_id")
      checkAnswer(broadcast, for (id <- 1 to 2; name <- Seq("aa", "bb", "cc")) yield {
        Row(id, name, if (id == 1) 42.0f else 10.0f)
      })
      assert(collectScans(broadcast.queryExecution.executedPlan).head.partitionPacking.isDefined)
      val union = input.union(input).groupBy("name").count()
      checkAnswer(union, Seq(Row("aa", 4L), Row("bb", 4L), Row("cc", 4L)))
      assert(collect(union.queryExecution.executedPlan) { case c: CoalesceExec => c }.isEmpty)
      withTempPath { dir =>
        input.write.parquet(dir.getCanonicalPath)
        checkAnswer(spark.read.parquet(dir.getCanonicalPath), input)
        val files = Files.list(dir.toPath)
        try {
          assert(files.filter(_.getFileName.toString.startsWith("part-")).count() === 2)
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
        assert(collectScans(plan).exists(_.partitionPacking.isDefined))
      }
    }
  }
}

trait PackV2ScanPartitionsRuntimeFilterTests extends KeyGroupedPartitioningSuiteBase {
  test("SPARK-60265: adaptive runtime filtering retains coalescing and pruned input slots") {
    createTable(items, itemsColumns, Array(identity("id")))
    createTable(purchases, purchasesColumns, Array.empty)
    val values = (1 to 2).flatMap { id =>
      (1 to 24).map(split => s"($id, 'split$split', 1.0, NULL)")
    }.mkString(", ")
    sql(s"INSERT INTO testcat.ns.$items VALUES $values")
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    for (adaptive <- Seq(false, true); price <- Seq(40, 100)) {
      withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key -> "true",
          SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key -> "2400",
          SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES.key -> "0",
          SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM.key -> "1",
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
        assert(scan.partitionPacking.isDefined)
        val rowsPerTask = scan.inputRDD.mapPartitions(rows => Iterator.single(rows.size)).collect()
        assert(rowsPerTask.length === 1)
        assert(rowsPerTask.sum === (if (price == 40) 24 else 0))
        assert(scan.outputOrdering.isEmpty)
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
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
  val opened = new AtomicInteger()
  val closed = new AtomicInteger()
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("This source supports only columnar readers")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val input = partition.asInstanceOf[CoalescingColumnarInputPartition]
    opened.incrementAndGet()
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
      override def close(): Unit = {
        batch.close()
        closed.incrementAndGet()
      }
    }
  }
}

/** Row source with independently reported split sizes, including missing and invalid values. */
class SizedPackingRowSource extends TestingV2Source {
  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      new SimpleScanBuilder {
        override def planInputPartitions(): Array[InputPartition] = {
          SizedPackingRowReaderFactory.planned.incrementAndGet()
          options.get("sizes").split(",").zipWithIndex.map { case (size, index) =>
            SizedPackingRowInput(index, if (size == "?") None else Some(size.toLong))
          }
        }
        override def createReaderFactory(): PartitionReaderFactory = {
          SizedPackingRowReaderFactory.factories.incrementAndGet()
          SizedPackingRowReaderFactory
        }
      }
  }
}

case class SizedPackingRowInput(index: Int, bytes: Option[Long]) extends HasPartitionStatistics {
  override def preferredLocations(): Array[String] = Array("worker-a", "worker-b")
  override def sizeInBytes(): OptionalLong = bytes.map(OptionalLong.of)
    .getOrElse(OptionalLong.empty())
  override def numRows(): OptionalLong = OptionalLong.of(index + 1)
  override def filesCount(): OptionalLong = OptionalLong.empty()
}

object SizedPackingRowReaderFactory extends PartitionReaderFactory {
  val planned = new AtomicInteger()
  val factories = new AtomicInteger()
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val input = partition.asInstanceOf[SizedPackingRowInput]
    new PartitionReader[InternalRow] {
      private var rowIndex = -1
      override def next(): Boolean = { rowIndex += 1; rowIndex <= input.index }
      override def get(): InternalRow = InternalRow(input.index, rowIndex)
      override def close(): Unit = {}
    }
  }
}
