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

import java.util.OptionalLong
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Random

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite, TaskContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Literal}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.connector.catalog.{Column, Table}
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue, NamedReference, SortDirection, SortOrder}
import org.apache.spark.sql.connector.expressions.Expressions.{identity, sort}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, HasPartitionStatistics, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsReportOrdering, SupportsReportPartitioning, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ColumnarToRowExec, FormattedMode, InputAdapter, PartialReducerPartitionSpec, ProjectedBroadcastValueSubqueryExec, SimpleMode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan, PackScanPartitionsExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, PipelinedShuffleTestSession, ReusedExchangeExec, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions.{broadcast, expr, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

class PackV2ScanPartitionsReadSuite extends KeyGroupedPartitioningSuiteBase {
  import testImplicits._

  private val enabled = SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES, 0L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES, 300L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM, 1)

  after {
    catalog.clearTables()
  }

  private def populate(): Unit = {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 1.0, NULL), (1, 'bb', 2.0, NULL), (1, 'cc', 3.0, NULL), " +
      "(2, 'aa', 4.0, NULL), (2, 'bb', 5.0, NULL), (2, 'cc', 6.0, NULL)")
  }

  private def packNodes(plan: SparkPlan): Seq[PackScanPartitionsExec] =
    collect(plan) { case p: PackScanPartitionsExec => p }

  // The number of rows in each partition of the only packing node of `df`.
  private def rowsPerPackedTask(df: DataFrame): Seq[Int] = {
    val nodes = packNodes(df.queryExecution.executedPlan)
    assert(nodes.size === 1)
    nodes.head.execute().mapPartitions(rows => Iterator.single(rows.size)).collect().toSeq
  }

  private def sized(sizes: String): DataFrame =
    spark.read.format(classOf[SizedPackingRowSource].getName).option("sizes", sizes).load()

  test("SPARK-60265: packed readers preserve storage filters and their checked column") {
    // The storage filter source reports no sizes, so the count cap packs it.
    withSQLConf(enabled -> "true", SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key -> "2",
        SQLConf.V2_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000") {
      withTable("packing_bloom_build") {
        spark.range(0, 30).selectExpr("id AS bk", "id AS bv")
          .write.format("parquet").saveAsTable("packing_bloom_build")
        for (columnar <- Seq(false, true); adaptive <- Seq(false, true);
            giveUp <- Seq(false, true)) {
          withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
            StorageFilterTestSource.reset()
            val probe = spark.read.format(classOf[StorageFilterTestSource].getName)
              .option("numPartitions", "10").option("columnar", columnar.toString)
              .option("giveUp", giveUp.toString).load()
            val build = spark.table("packing_bloom_build").where("bv = 5")
            val df = probe.join(build, probe("k") === build("bk"))
              .select(probe("k"), probe("v"))
            assert(df.collect().toSeq === Seq(Row(5L, 50L)))
            val plan = df.queryExecution.executedPlan
            val scan = collectScans(plan).find(_.storageFilters.nonEmpty).get
            val node = packNodes(plan).find(n => collectScans(n).contains(scan)).get
            assert(scan.inputPartitions.size === 10)
            assert(node.execute().getNumPartitions === 2)
            assert(scan.appendsCheckedColumn)
            assert(scan.supportsColumnar === columnar)
            assert(StorageFilterTestSource.filteredReaders.get() === 10)
            assert(StorageFilterTestSource.checkedColumnRequests.get() === 10)
            assert(ValidateRequirements.validate(stripAQEPlan(plan)))
          }
        }
      }
    }
  }

  test("SPARK-60265: columnar readers keep reused batches correct through a packed shuffle") {
    withTempView("columnar") {
      spark.read.format(classOf[PackingColumnarScanSource].getName).load()
        .createOrReplaceTempView("columnar")
      val expected = Seq((1, 1, Seq(20, 21, 22, 10, 11)), (1, 2, Seq(5, 6)),
        (2, 3, Seq(30, 31))).flatMap { case (i, k, values) =>
        values.map(j => Row(j, 1L, i, k))
      }
      for (adaptive <- Seq(false, true); codegen <- Seq(false, true); filter <- Seq(false, true)) {
        // This source reports no sizes, so the count cap packs it.
        withSQLConf(enabled -> "true",
            SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key -> "2",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
          val where = if (filter) " WHERE j > 10" else ""
          val df = sql("SELECT j, count(*), max(i), max(k) FROM columnar" + where + " GROUP BY j")
          val answers = if (filter) expected.filter(_.getInt(0) > 10) else expected
          checkAnswer(df, answers)
          checkAnswer(df, answers)
          assert(rowsPerPackedTask(df).size === 2)
          assert(ValidateRequirements.validate(stripAQEPlan(df.queryExecution.executedPlan)))
          // Directly below the shuffle, the node packs the scan's batches, and the rows are
          // converted above it.
          val repartitioned = spark.table("columnar").repartition(3)
          checkAnswer(repartitioned,
            expected.map(row => Row(row.getInt(2), row.getInt(0), row.getInt(3))))
          val node = packNodes(repartitioned.queryExecution.executedPlan).head
          assert(node.supportsColumnar)
          assert(node.executeColumnar().getNumPartitions === 2)
          assert(collect(repartitioned.queryExecution.executedPlan) {
            case ColumnarToRowExec(InputAdapter(_: PackScanPartitionsExec)) => true
            case ColumnarToRowExec(_: PackScanPartitionsExec) => true
          }.size === 1)
        }
      }
    }
  }

  test("SPARK-60265: real unkeyed readers honor byte bounds and the missing-size fallback") {
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key -> "100") {
      def expected(n: Int): Seq[Row] = (0 until n).flatMap(i => (0 to i).map(j => Row(i, j)))
      val bounded = sized("30,40,200,20,30,90").repartition(2)
      checkAnswer(bounded, expected(6))
      assert(rowsPerPackedTask(bounded) === Seq(3, 3, 9, 6))

      // As with spark.sql.files.maxPartitionNum, above the cap the inputs are packed by the
      // scan's total size over it.
      withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key -> "2") {
        val capped = sized(Seq.fill(8)("100").mkString(",")).repartition(2)
        checkAnswer(capped, expected(8))
        assert(rowsPerPackedTask(capped) === Seq(10, 26))
      }

      // A scan with an input that reports no size is grouped by count, and only under the cap.
      val unsized = "?,20,-1,40,?"
      withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key -> "3") {
        val capped = sized(unsized).repartition(2)
        checkAnswer(capped, expected(5))
        assert(rowsPerPackedTask(capped) === Seq(1, 5, 9))
      }
      val unchanged = sized(unsized).repartition(2)
      checkAnswer(unchanged, expected(5))
      assert(rowsPerPackedTask(unchanged) === Seq(1, 2, 3, 4, 5))
    }
  }

  test("SPARK-60265: packed tasks prefer the host that most of their inputs report") {
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // Three inputs per task. Each task's first input reports the other task's host, which is
      // the one the scheduler would take through the lineage if the task named no host.
      val df = spark.read.format(classOf[SizedPackingRowSource].getName)
        .option("sizes", Seq.fill(6)("100").mkString(","))
        .option("hosts", "h2,h1,h1,h1,h2,h2")
        .load()
        .repartition(2)
      checkAnswer(df, (0 until 6).flatMap(i => (0 to i).map(j => Row(i, j))))
      val nodes = packNodes(df.queryExecution.executedPlan)
      assert(nodes.size === 1)
      val packed = nodes.head.execute()
      assert(packed.partitions.indices.map(sparkContext.getPreferredLocs(packed, _).map(_.host))
        === Seq(Seq("h1"), Seq("h2")))
    }
  }

  test("SPARK-60265: packed columnar readers close on early stop and task failure") {
    withSQLConf(enabled -> "true", SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key -> "2",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.read.format(classOf[PackingColumnarScanSource].getName).load().repartition(3)
      val packed = packNodes(df.queryExecution.executedPlan).head.executeColumnar()
      PackingColumnarReaderFactory.opened.set(0)
      PackingColumnarReaderFactory.closed.set(0)
      assert(packed.mapPartitions(_.take(1)).count() === 2)
      assert(PackingColumnarReaderFactory.opened.get() === 2)
      assert(PackingColumnarReaderFactory.closed.get() === 2)
      intercept[SparkException] {
        packed.mapPartitions { batches =>
          if (batches.hasNext) throw new IllegalStateException("intentional reader failure")
          batches
        }.count()
      }
      assert(PackingColumnarReaderFactory.opened.get() > 2)
      eventually(timeout(10.seconds), interval(10.milliseconds)) {
        assert(PackingColumnarReaderFactory.closed.get() ===
          PackingColumnarReaderFactory.opened.get())
      }
    }
  }

  test("SPARK-60265: a range shuffle over packed inputs keeps the complete global ordering") {
    populate()
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT id, name FROM testcat.ns.$items ORDER BY name, id")
        assert(df.collect().toSeq === Seq(Row(1, "aa"), Row(2, "aa"), Row(1, "bb"),
          Row(2, "bb"), Row(1, "cc"), Row(2, "cc")))
        assert(rowsPerPackedTask(df).size === 2)
        assert(ValidateRequirements.validate(stripAQEPlan(df.queryExecution.executedPlan)))
      }
    }
  }

  test("SPARK-60265: a shuffle above a storage-partitioned join leaves the join's inputs alone") {
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
        assert(packNodes(plan).isEmpty)
        assert(collect(plan) { case s: ShuffleExchangeExec => s }.size === 1)
        assert(collectGroupPartitions(plan).nonEmpty)
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
      }
    }
  }

  test("SPARK-60265: shuffles over packed scans remain reusable and execute repeatedly") {
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
        val nodes = packNodes(plan)
        assert(nodes.nonEmpty && nodes.forall(_.execute().getNumPartitions == 2))
      }
    }
  }

  test("SPARK-60265: V2 file scans keep the partitions spark.sql.files.* plans") {
    withTempPath { dir =>
      spark.range(0, 80, 1, 8).selectExpr("id", "id % 4 AS k").write.parquet(dir.getCanonicalPath)
      for (adaptive <- Seq(false, true)) {
        withSQLConf(enabled -> "true", SQLConf.USE_V1_SOURCE_LIST.key -> "",
            SQLConf.FILES_MIN_PARTITION_NUM.key -> "8",
            SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key -> "2",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          val df = spark.read.parquet(dir.getCanonicalPath).groupBy("k").count()
          checkAnswer(df, (0L until 4L).map(k => Row(k, 20L)))
          val plan = df.queryExecution.executedPlan
          val scan = collectScans(plan).head
          assert(scan.scan.isInstanceOf[FileScan])
          assert(packNodes(plan).isEmpty)
          assert(scan.inputRDD.getNumPartitions === 8)
        }
      }
    }
  }

  test("SPARK-60265: a sort aggregate below the node keeps the scan ordering it uses") {
    createTable(items, itemsColumns, Array.empty, Array(
      sort(FieldReference("name"), SortDirection.ASCENDING)))
    (1 to 6).foreach { i =>
      sql(s"INSERT INTO testcat.ns.$items VALUES ($i, 'n${i % 2}', 1.0, NULL)")
    }
    // Hash and object-hash aggregates, which ReplaceHashWithSortAgg both replaces.
    for (adaptive <- Seq(false, true); packing <- Seq(false, true);
        (aggregate, count) <- Seq("count(*)" -> 3L, "size(collect_list(id))" -> 3)) {
      withSQLConf(enabled -> packing.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT name, $aggregate FROM testcat.ns.$items GROUP BY name")
        checkAnswer(df, Seq(Row("n0", count), Row("n1", count)))
        val plan = df.queryExecution.executedPlan
        withClue(plan.treeString) {
          assert(packNodes(plan).size === (if (packing) 1 else 0))
          val exchange = collect(plan) { case s: ShuffleExchangeExec => s }.head
          assert(collect(exchange.child) { case a: BaseAggregateExec => a.getClass } ===
            Seq(classOf[SortAggregateExec]))
          // The partial sort aggregate still reads the scan's ordering.
          assert(ValidateRequirements.validate(stripAQEPlan(plan)))
        }
      }
    }
  }

  test("SPARK-60265: a keyed scan's inputs are packed in key order only with V2 bucketing") {
    for (bucketing <- Seq(true, false)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ENABLED.key -> bucketing.toString,
          SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key -> "128",
          SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES.key -> "4") {
        val df = spark.read.format(classOf[SizedPackingRowSource].getName)
          .option("sizes", "100,20,100,20").option("keys", "1,2,1,2").load()
          .groupBy("j").count()
        checkAnswer(df, Seq(Row(0, 4L), Row(1, 3L), Row(2, 2L), Row(3, 1L)))
        // In key order, the two 100-byte inputs cannot share a task.
        assert(packNodes(df.queryExecution.executedPlan).head.execute().getNumPartitions ===
          (if (bucketing) 3 else 2))
      }
    }
  }

  test("SPARK-60265: the packing node reports what it packed and shows in both explain modes") {
    populate()
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
        // What the SQL tab shows for the node.
        val metrics = runAndFetchMetrics(df.collect()).collect {
          case ((_, node, name), value) if node == "PackScanPartitions" => name -> value
        }
        assert(metrics.get("number of input partitions").contains("6"), metrics)
        assert(metrics.get("number of partitions").contains("2"), metrics)
        assert(metrics.get("max partitions per group").contains("3"), metrics)
        checkAnswer(df, Seq(Row("aa", 2L), Row("bb", 2L), Row("cc", 2L)))
        Seq(SimpleMode, FormattedMode).foreach { mode =>
          assert(df.queryExecution.explainString(mode).contains("PackScanPartitions"), mode)
        }
      }
    }
  }

  test("SPARK-60265: partition-dependent expressions below the node keep their values") {
    populate()
    val query = s"SELECT pid, count(*) FROM (SELECT spark_partition_id() AS pid " +
      s"FROM testcat.ns.$items) GROUP BY pid"
    val expected = withSQLConf(enabled -> "false") { sql(query).collect().toSeq }
    assert(expected.size === 6, "test setup: one partition per split")
    withSQLConf(enabled -> "true") {
      val df = sql(query)
      checkAnswer(df, expected)
      assert(packNodes(df.queryExecution.executedPlan).size === 1)
      // Code that reads the task's partition id instead, such as this UDF, sees the packed task.
      val taskPartition = udf(() => TaskContext.getPartitionId()).asNondeterministic()
      val ids = spark.table(s"testcat.ns.$items")
        .select(expr("spark_partition_id()").as("pid"), taskPartition().as("tid"))
        .groupBy("pid", "tid").count()
      val rows = ids.collect()
      assert(packNodes(ids.queryExecution.executedPlan).map(_.execute().getNumPartitions) ===
        Seq(2))
      assert(rows.map(_.getInt(0)).toSet === (0 until 6).toSet)
      assert(rows.map(_.getInt(1)).toSet === Set(0, 1))
    }
  }

  test("SPARK-60265: barrier mode runs over a stage that reads a packed shuffle") {
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sized("30,40,20").repartition(2)
      assert(packNodes(df.queryExecution.executedPlan).head.execute().getNumPartitions === 1)
      // The node lives in the shuffle's map stage, so the barrier stage reads the shuffle.
      assert(df.queryExecution.toRdd.barrier().mapPartitions(_.map(_ => 1)).count() === 6)
    }
  }

  test("SPARK-60265: packing a shuffle's input adds no connector calls") {
    // planInputPartitions, toBatch and createReaderFactory calls.
    def counters: Seq[Int] = Seq(SizedPackingRowReaderFactory.planned.get(),
      SizedPackingRowReaderFactory.batches.get(), SizedPackingRowReaderFactory.factories.get())
    def calls(packing: Boolean, keyed: Boolean, mode: String, adaptive: Boolean): Seq[Int] = {
      withSQLConf(enabled -> packing.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        SizedPackingRowReaderFactory.reset()
        val reader = spark.read.format(classOf[SizedPackingRowSource].getName)
          .option("sizes", "30,40,200,20,30,90").option("columnar", mode)
        val df = (if (keyed) reader.option("keys", "0,1,2,3,4,5") else reader).load()
          .groupBy("j").count()
        df.queryExecution.explainString(SimpleMode)
        val whilePlanning = counters
        checkAnswer(df, (0 until 6).map(j => Row(j, (6 - j).toLong)))
        assert(packNodes(df.queryExecution.executedPlan).size === (if (packing) 1 else 0))
        whilePlanning ++ counters
      }
    }
    for (keyed <- Seq(false, true); mode <- Seq("PARTITION_DEFINED", "UNSUPPORTED");
        adaptive <- Seq(false, true)) {
      val unpacked = calls(packing = false, keyed, mode, adaptive)
      val packed = calls(packing = true, keyed, mode, adaptive)
      withClue(s"keyed=$keyed mode=$mode adaptive=$adaptive unpacked=$unpacked packed=$packed") {
        assert(packed === unpacked)
      }
    }
  }

  test("SPARK-60265: a packed shuffle's input is planned only when it executes (SPARK-44505)") {
    for (mode <- Seq("SUPPORTED", "UNSUPPORTED"); adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = spark.read.format(classOf[ScanDefinedColumnarSupport].getName)
          .option("columnar", mode).load().groupBy("i").count()
        df.queryExecution.explainString(SimpleMode)
        if (!adaptive) assert(packNodes(df.queryExecution.executedPlan).size === 1)
        Seq(() => df.count(), () => df.collect()).foreach { action =>
          val e = intercept[Exception](action())
          assert(Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
            .exists(_.getMessage == "planInputPartitions must not be called"), e)
        }
      }
    }
  }

  test("SPARK-60265: dynamic partition pruning below a packed shuffle plans and reuses as before") {
    createTable("dim", Array(Column.create("k", IntegerType), Column.create("v", IntegerType)),
      Array.empty)
    sql("INSERT INTO testcat.ns.dim VALUES (2, 1), (5, 1), (7, 0)")
    def run(packing: Boolean, keyed: Boolean, adaptive: Boolean): (Int, Seq[Int], SparkPlan) = {
      withSQLConf(enabled -> packing.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        SizedPackingRowReaderFactory.reset()
        val reader = spark.read.format(classOf[SizedPackingRowSource].getName)
          .option("sizes", Seq.fill(8)("100").mkString(",")).option("columnar", "UNSUPPORTED")
        val fact = (if (keyed) reader.option("keys", (0 until 8).mkString(",")) else reader).load()
        val dim = spark.table("testcat.ns.dim").where("v = 1")
        // Grouped by a column that is not the key, so a shuffle reads the pruned scan.
        val df = fact.join(broadcast(dim), fact("i") === dim("k")).groupBy(fact("j")).count()
        val qe = df.queryExecution
        qe.executedPlan
        val whilePlanning = SizedPackingRowReaderFactory.planned.get()
        val rows = qe.executedPlan.executeCollect().map(row => (row.getInt(0), row.getLong(1)))
        assert(rows.toSet === Set((0, 2L), (1, 2L), (2, 2L), (3, 1L), (4, 1L), (5, 1L)))
        (whilePlanning, SizedPackingRowReaderFactory.plannedInputs.asScala.toSeq, qe.executedPlan)
      }
    }
    // Scans that DPP prunes, broadcasts, reused exchanges.
    def reuse(plan: SparkPlan): (Int, Int, Int) = (
      collectWithSubqueries(plan) {
        case s: BatchScanExec if s.runtimeFilters.exists {
          case DynamicPruningExpression(Literal.TrueLiteral) => false
          case _: DynamicPruningExpression => true
          case _ => false
        } => s
      }.size,
      collectWithSubqueries(plan) { case b: BroadcastExchangeLike => b }.size,
      collectWithSubqueries(plan) { case r: ReusedExchangeExec => r }.size)
    for (keyed <- Seq(false, true); adaptive <- Seq(false, true)) {
      val (unpackedPlanning, unpackedInputs, unpackedPlan) = run(false, keyed, adaptive)
      val (packedPlanning, packedInputs, packedPlan) = run(true, keyed, adaptive)
      withClue(s"keyed=$keyed adaptive=$adaptive unpacked=$unpackedInputs packed=$packedInputs") {
        assert(reuse(unpackedPlan)._1 > 0, "test setup: DPP prunes the scan")
        assert(reuse(unpackedPlan)._3 > 0, "test setup: DPP reuses the join's broadcast")
        assert(packNodes(packedPlan).size === 1)
        assert(reuse(packedPlan) === reuse(unpackedPlan))
        assert(packedPlanning === unpackedPlanning)
        assert(packedInputs === unpackedInputs)
      }
    }
  }

  test("SPARK-60265: DPP projected from a broadcast is planned as with packing off") {
    createTable("codes",
      Array(Column.create("join_id", IntegerType), Column.create("part", IntegerType)), Array.empty)
    sql("INSERT INTO testcat.ns.codes VALUES (1, 2), (2, 5), (3, 7)")
    createTable("selectors",
      Array(Column.create("join_id", IntegerType), Column.create("keep", BooleanType)),
      Array.empty)
    sql("INSERT INTO testcat.ns.selectors VALUES (1, true), (2, true), (3, false)")
    def run(packing: Boolean, adaptive: Boolean): SparkPlan = {
      withSQLConf(enabled -> packing.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_BROADCAST_PROJECTION_ENABLED.key -> "true") {
        sized(Seq.fill(8)("100").mkString(",")).createOrReplaceTempView("fact")
        // DPP projects the fact's keys from the rows of the codes broadcast, which it reuses.
        val df = sql(
          """WITH selected AS (
            |  SELECT /*+ BROADCAST(c) */ c.part FROM testcat.ns.codes c
            |  JOIN testcat.ns.selectors x ON c.join_id = x.join_id WHERE x.keep AND x.join_id > 0
            |)
            |SELECT /*+ MERGE(f, s) */ f.i, count(*) FROM fact f JOIN selected s ON f.i = s.part
            |GROUP BY f.i
            |""".stripMargin)
        val qe = df.queryExecution
        val rows = qe.executedPlan.executeCollect().map(row => (row.getInt(0), row.getLong(1)))
        assert(rows.toSet === Set((2, 3L), (5, 6L)))
        qe.executedPlan
      }
    }
    def projected(plan: SparkPlan): (Int, Int) = (
      collectWithSubqueries(plan) { case s: ProjectedBroadcastValueSubqueryExec => s }.size,
      collectWithSubqueries(plan) { case r: ReusedExchangeExec => r }.size)
    withTempView("fact") {
      for (adaptive <- Seq(false, true)) {
        val unpacked = run(packing = false, adaptive)
        val packed = run(packing = true, adaptive)
        withClue(s"adaptive=$adaptive") {
          assert(projected(unpacked)._1 === 1, "test setup: DPP projects the broadcast")
          assert(projected(unpacked)._2 > 0, "test setup: DPP reuses the broadcast")
          assert(projected(packed) === projected(unpacked))
          assert(packNodes(packed).nonEmpty)
        }
      }
    }
  }

  test("SPARK-60265: cached plans and checkpoints keep the layout of scans at their root") {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, NULL), (1, 'ab', 41.0, NULL), (3, 'cc', 15.5, NULL), (3, 'cd', 16.5, NULL)")
    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (3, 19.5, NULL)")
    val expected =
      Seq(Row(1L, "aa", 42.0f), Row(1L, "ab", 42.0f), Row(3L, "cc", 19.5f), Row(3L, "cd", 19.5f))
    def join(left: DataFrame): DataFrame =
      left.as("i").join(spark.table(s"testcat.ns.$purchases").as("p"), $"i.id" === $"p.item_id")
        .select($"i.id", $"i.name", $"p.price")
    withTempDir { dir =>
      spark.sparkContext.setCheckpointDir(dir.getPath)
      for (adaptive <- Seq(false, true)) {
        withSQLConf(enabled -> "true",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          val input = spark.table(s"testcat.ns.$items")
          val checkpointed = join(input.checkpoint())
          checkAnswer(checkpointed, expected)
          input.cache()
          try {
            input.count()
            val cached = join(input)
            checkAnswer(cached, expected)
            Seq(checkpointed, cached).foreach { df =>
              val plan = df.queryExecution.executedPlan
              assert(collect(plan) { case s: ShuffleExchangeExec => s }.isEmpty, plan.treeString)
            }
          } finally {
            input.unpersist(blocking = true)
          }
        }
      }
    }
  }

  test("SPARK-60265: scan packing respects AQE skew map-output boundaries") {
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      (1 to 9).map(price => s"($price, $price, NULL)").mkString(", "))
    for (targetMaps <- Seq(1, 2)) {
      val table = s"skew_items_$targetMaps"
      createTable(table, itemsColumns, Array(identity("id")))
      // Forty large rows share the join key; eight small reducer populations establish skew.
      // Seeded, incompressible payloads keep the byte thresholds independent of compression.
      val expected = (0 until 48).map { index =>
        val price = if (index < 40) 1 else index - 38
        val payload = new Random(index).alphanumeric.take(16384).mkString
        Row(1L, payload, price.toFloat, price.toLong)
      }
      sql(s"INSERT INTO testcat.ns.$table VALUES " + expected.map { row =>
        s"(${row.getLong(0)}, '${row.getString(1)}', ${row.getFloat(2)}, NULL)"
      }.mkString(", "))
      for (packing <- Seq(false, true)) {
        withSQLConf(enabled -> packing.toString,
            SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key ->
              (4800 / targetMaps).toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
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
          val numMaps = if (packing) targetMaps else 48
          assert(exchange.numMappers === numMaps)
          assert(collectScans(exchange).head.inputRDD.getNumPartitions === 48)
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
            assert(parts.zip(parts.tail).forall { case (a, b) =>
              a.endMapIndex == b.startMapIndex
            })
            if (packing) assert(parts.size === 2) else assert(parts.size > 2)
          }
          val rightStages = collect(join.right) { case s: ShuffleQueryStageExec => s }
          assert(rightStages.size === 1)
          assert(rightStages.head.shuffle.numMappers === (if (packing) 1 else 9))
        }
      }
    }
  }
}

/** Packing with the opt-in local pipelined shuffle, which needs a session of its own. */
class PackV2ScanPartitionsPipelinedSuite extends SparkFunSuite
  with AdaptiveSparkPlanHelper with PipelinedShuffleTestSession {

  test("SPARK-60265: a plan with a packed shuffle keeps regular shuffles") {
    for (aqe <- Seq(false, true); packing <- Seq(false, true)) {
      withPipelinedSession("pack-v2-scan-partitions-pipelined", aqe) { spark =>
        spark.conf.set(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key, packing.toString)
        spark.conf.set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key, "300")
        spark.conf.set(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES.key, "0")
        spark.conf.set(SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM.key, "1")
        // The keyed scan reports its six inputs, so without packing every width is known.
        val df = spark.read.format(classOf[SizedPackingRowSource].getName)
          .option("sizes", Seq.fill(6)("100").mkString(","))
          .option("keys", "0,1,2,3,4,5")
          .load()
          .groupBy("j")
          .count()
        // A pipelined shuffle runs once, so one action gives both the answer and the plan.
        assert(df.collect().toSet === (0 until 6).map(j => Row(j, (6 - j).toLong)).toSet)
        val plan = df.queryExecution.executedPlan
        withClue(s"aqe=$aqe packing=$packing\n$plan") {
          assert(collect(plan) { case p: PackScanPartitionsExec => p }.nonEmpty === packing)
          assert(collect(plan) { case s: ShuffleExchangeExec if s.pipelined => s }.nonEmpty ===
            !packing)
        }
      }
    }
  }
}

/** A columnar-only source that reuses two-row batches, including a short final batch. */
class PackingColumnarScanSource extends TestingV2Source {
  private val sourceSchema = TestingV2Source.schema.add("k", IntegerType)

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = sourceSchema

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def schema(): StructType = sourceSchema

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      new SimpleScanBuilder with SupportsReportPartitioning with SupportsReportOrdering {
        override def readSchema(): StructType = sourceSchema

        override def planInputPartitions(): Array[InputPartition] = Array(
          PackingColumnarInputPartition(1, 1, Array(20, 21, 22)),
          PackingColumnarInputPartition(1, 1, Array(10, 11)),
          PackingColumnarInputPartition(1, 2, Array(5, 6)),
          PackingColumnarInputPartition(2, 3, Array(30, 31)))

        override def outputPartitioning(): Partitioning =
          new KeyGroupedPartitioning(Array(FieldReference("i"), FieldReference("k")), 4)

        override def outputOrdering(): Array[SortOrder] = Array(
          sort(FieldReference("i"), SortDirection.ASCENDING),
          sort(FieldReference("j"), SortDirection.ASCENDING))

        override def createReaderFactory(): PartitionReaderFactory = PackingColumnarReaderFactory
      }
  }
}

case class PackingColumnarInputPartition(i: Int, k: Int, values: Array[Int])
    extends InputPartition with HasPartitionKey {
  override def partitionKey(): InternalRow = InternalRow(i, k)
}

object PackingColumnarReaderFactory extends PartitionReaderFactory {
  val opened = new AtomicInteger()
  val closed = new AtomicInteger()
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("This source supports only columnar readers")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val input = partition.asInstanceOf[PackingColumnarInputPartition]
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

/**
 * Row source with independently reported split sizes, including missing and invalid values. With
 * `keys`, each split holds rows of one partition key and the scan reports a keyed partitioning on
 * `i`; `columnar` sets the scan's columnar support mode; `hosts` gives each split's preferred host.
 * Runtime filters on `i` keep the splits of the listed keys. It counts the planning calls it gets
 * and records how many inputs each returns.
 */
class SizedPackingRowSource extends TestingV2Source {
  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      val sizes = options.get("sizes").split(",")
      val keys = Option(options.get("keys")).map(_.split(",").map(_.toInt))
      val mode = Option(options.get("columnar")).map(Scan.ColumnarSupportMode.valueOf)
      val hosts = Option(options.get("hosts")).map(_.split(","))
      new SimpleScanBuilder with SupportsReportPartitioning with SupportsRuntimeV2Filtering {
        private var keptKeys: Option[Set[Int]] = None
        override def toBatch: Batch = {
          SizedPackingRowReaderFactory.batches.incrementAndGet()
          this
        }
        override def filterAttributes(): Array[NamedReference] = Array(FieldReference("i"))
        override def filter(predicates: Array[V2Predicate]): Unit = predicates.foreach {
          case p if p.name() == "IN" =>
            keptKeys = Some(p.children().tail.collect {
              case v: LiteralValue[_] => v.value.asInstanceOf[Int]
            }.toSet)
          case _ =>
        }
        override def planInputPartitions(): Array[InputPartition] = {
          SizedPackingRowReaderFactory.planned.incrementAndGet()
          val inputs = sizes.zipWithIndex.map { case (size, index) =>
            SizedPackingRowInput(index, if (size == "?") None else Some(size.toLong),
              keys.map(_(index)), hosts.map(_(index)).toSeq)
          }.filter(input => keptKeys.forall(_.contains(input.keyValue)))
          SizedPackingRowReaderFactory.plannedInputs.add(inputs.length)
          inputs.toArray[InputPartition]
        }
        override def outputPartitioning(): Partitioning = keys match {
          case Some(_) => new KeyGroupedPartitioning(Array(FieldReference("i")), sizes.length)
          case None => new UnknownPartitioning(sizes.length)
        }
        override def columnarSupportMode(): Scan.ColumnarSupportMode =
          mode.getOrElse(Scan.ColumnarSupportMode.PARTITION_DEFINED)
        override def createReaderFactory(): PartitionReaderFactory = {
          SizedPackingRowReaderFactory.factories.incrementAndGet()
          SizedPackingRowReaderFactory
        }
      }
    }
  }
}

/** Rows `(key, 0)` to `(key, index)`, where the key defaults to the split's index. */
case class SizedPackingRowInput(
    index: Int,
    bytes: Option[Long],
    key: Option[Int] = None,
    hosts: Seq[String] = Nil)
  extends HasPartitionStatistics with HasPartitionKey {
  def keyValue: Int = key.getOrElse(index)
  override def preferredLocations(): Array[String] = hosts.toArray
  override def partitionKey(): InternalRow = InternalRow(keyValue)
  override def sizeInBytes(): OptionalLong = bytes.map(OptionalLong.of)
    .getOrElse(OptionalLong.empty())
  override def numRows(): OptionalLong = OptionalLong.of(index + 1)
  override def filesCount(): OptionalLong = OptionalLong.empty()
}

object SizedPackingRowReaderFactory extends PartitionReaderFactory {
  val batches = new AtomicInteger()
  val planned = new AtomicInteger()
  val factories = new AtomicInteger()
  val plannedInputs = new ConcurrentLinkedQueue[Int]()
  def reset(): Unit = {
    Seq(batches, planned, factories).foreach(_.set(0))
    plannedInputs.clear()
  }
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val input = partition.asInstanceOf[SizedPackingRowInput]
    new PartitionReader[InternalRow] {
      private var rowIndex = -1
      override def next(): Boolean = { rowIndex += 1; rowIndex <= input.index }
      override def get(): InternalRow = InternalRow(input.keyValue, rowIndex)
      override def close(): Unit = {}
    }
  }
}
