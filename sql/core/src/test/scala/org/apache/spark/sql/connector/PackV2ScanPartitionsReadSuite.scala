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
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue, NamedReference, SortDirection, SortOrder}
import org.apache.spark.sql.connector.expressions.Expressions.{identity, sort}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, HasPartitionStatistics, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsReportOrdering, SupportsReportPartitioning, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{FormattedMode, SimpleMode, SortExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.v2.{FileScan, GroupPartitionsExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
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

  after {
    catalog.clearTables()
  }

  private def populate(): Unit = {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 1.0, NULL), (1, 'bb', 2.0, NULL), (1, 'cc', 3.0, NULL), " +
      "(2, 'aa', 4.0, NULL), (2, 'bb', 5.0, NULL), (2, 'cc', 6.0, NULL)")
  }

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
            val scan = collectScans(plan).head
            assert(scan.partitionPacking.isDefined)
            assert(scan.inputPartitions.size === 10)
            assert(scan.inputRDD.getNumPartitions === 2)
            assert(scan.storageFilters.size === 1)
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

  test("SPARK-60265: columnar readers keep reused batches correct through a shuffled pipeline") {
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

  test("SPARK-60265: storage-partitioned groups over a scan take their first input's location") {
    withSQLConf(enabled -> "false", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // The first three splits share a key. Most of them prefer h1, the first one h2.
      val df = spark.read.format(classOf[SizedPackingRowSource].getName)
        .option("sizes", "10,10,10,10").option("keys", "0,0,0,1").option("hosts", "h2,h1,h1,h3")
        .load().groupBy("i").count()
      checkAnswer(df, Seq(Row(0, 6), Row(1, 4)))
      val groups = collect(df.queryExecution.executedPlan) { case g: GroupPartitionsExec => g }
      assert(groups.size === 1, "test setup: the aggregate groups the scan's splits by key")
      val rdd = groups.head.execute()
      assert(rdd.getNumPartitions === 2)
      // The coalescer reads the locations of the scan's mapped RDD, which reports none, so no
      // group names a location, with or without the coalescer's tie-break.
      assert(rdd.partitions.forall(p => rdd.preferredLocations(p).isEmpty))
      // The scheduler takes the first location it finds through a group's inputs, in order.
      val hosts = rdd.partitions.indices.map(sparkContext.getPreferredLocs(rdd, _).map(_.host))
      assert(hosts === Seq(Seq("h2"), Seq("h3")))
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
        // Every input lists both workers. A packed group prefers the worker that the largest number
        // of its inputs list, spreading ties across groups, and an input left alone keeps all of
        // its workers.
        val hosts = scan.inputRDD.partitions.indices.map { index =>
          sparkContext.getPreferredLocs(scan.inputRDD, index).map(_.host).toSet
        }
        val both = Set("worker-a", "worker-b")
        assert(hosts === Seq(Set("worker-a"), both, Set("worker-b"), both))

        read("?,20,-1,40,?")
        withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key -> "3") {
          val capped = sql("SELECT * FROM sized")
          checkAnswer(capped, (0 until 5).flatMap(i => (0 to i).map(j => Row(i, j))))
          val sizes = capped.queryExecution.executedPlan.execute()
            .mapPartitions(rows => Iterator.single(rows.size)).collect()
          assert(sizes.toSeq === Seq(1, 5, 9))
        }
        // Without the count cap, a scan with an input that reports no size is marked, but its
        // inputs stay as they are.
        val unchanged = sql("SELECT * FROM sized")
        checkAnswer(unchanged, (0 until 5).flatMap(i => (0 to i).map(j => Row(i, j))))
        val unchangedScan = collectScans(unchanged.queryExecution.executedPlan).head
        assert(unchangedScan.partitionPacking.isDefined)
        assert(unchangedScan.inputRDD.getNumPartitions === 5)
      }
    }
  }

  test("SPARK-60265: packed columnar readers close on early stop and task failure") {
    withSQLConf(enabled -> "true", SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM.key -> "2",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.read.format(classOf[PackingColumnarScanSource].getName).load()
      val scan = collectScans(df.queryExecution.executedPlan).head
      assert(scan.partitionPacking.isDefined)
      PackingColumnarReaderFactory.opened.set(0)
      PackingColumnarReaderFactory.closed.set(0)
      assert(scan.inputRDD.mapPartitions(_.take(1)).count() === 2)
      assert(PackingColumnarReaderFactory.opened.get() === 2)
      assert(PackingColumnarReaderFactory.closed.get() === 2)
      intercept[SparkException] {
        scan.inputRDD.mapPartitions { rows =>
          if (rows.hasNext) throw new IllegalStateException("intentional reader failure")
          rows
        }.count()
      }
      assert(PackingColumnarReaderFactory.opened.get() > 2)
      eventually(timeout(10.seconds), interval(10.milliseconds)) {
        assert(PackingColumnarReaderFactory.closed.get() ===
          PackingColumnarReaderFactory.opened.get())
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
        val plans = Seq(false, true).map { packing =>
          withSQLConf(enabled -> packing.toString) {
            val df = sql(s"SELECT id, name FROM testcat.ns.$items SORT BY id, name")
            checkAnswer(df, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc")))
            val plan = df.queryExecution.executedPlan
            // The scan's ordering is what makes the sort redundant, so the scan stays unpacked.
            assert(collectScans(plan).forall(_.partitionPacking.isEmpty))
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
      assert(collectScans(plan).head.inputRDD.getNumPartitions === 2)
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
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT /*+ BROADCAST(p) */ i.id, count(*), max(p.price) " +
          s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON i.id = p.item_id " +
          "GROUP BY i.id")
        checkAnswer(df, Seq(Row(1, 3L, 42.0f), Row(2, 3L, 10.0f)))
        val plan = df.queryExecution.executedPlan
        // The aggregate needs the streamed side's keyed layout, so the items scan stays as it is.
        val scan = collectScans(plan).find(_.table.name().endsWith(items)).get
        assert(scan.partitionPacking.isEmpty)
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
      }
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
      assert(collectScans(input.queryExecution.executedPlan).head.inputRDD.getNumPartitions === 2)
      val broadcast = sql(s"SELECT /*+ BROADCAST(p) */ i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON i.id = p.item_id")
      checkAnswer(broadcast, for (id <- 1 to 2; name <- Seq("aa", "bb", "cc")) yield {
        Row(id, name, if (id == 1) 42.0f else 10.0f)
      })
      val broadcastScans = collectScans(broadcast.queryExecution.executedPlan)
      val streamed = broadcastScans.find(_.table.name().endsWith(items)).get
      assert(streamed.inputRDD.getNumPartitions === 2)
      // The build side is below the broadcast exchange.
      assert(broadcastScans.find(_.table.name().endsWith(purchases)).get.partitionPacking.isDefined)
      val union = input.union(input).groupBy("name").count()
      checkAnswer(union, Seq(Row("aa", 4L), Row("bb", 4L), Row("cc", 4L)))
      assert(collectScans(union.queryExecution.executedPlan).forall(_.partitionPacking.isEmpty))
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

  test("SPARK-60265: a shuffle above an SPJ leaves the join's keyed inputs unpacked") {
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
        assert(collectScans(plan).forall(_.partitionPacking.isEmpty))
        assert(collect(plan) { case s: ShuffleExchangeExec => s }.size === 1)
        assert(collectGroupPartitions(plan).nonEmpty)
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
      }
    }
  }

  test("SPARK-60265: exchanges over packed scans remain reusable and execute repeatedly") {
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
        val packed = collectScans(plan).filter(_.partitionPacking.isDefined)
        assert(packed.nonEmpty && packed.forall(_.inputRDD.getNumPartitions == 2))
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
          val scan = collectScans(df.queryExecution.executedPlan).head
          assert(scan.scan.isInstanceOf[FileScan])
          assert(scan.partitionPacking.isEmpty)
          assert(scan.inputRDD.getNumPartitions === 8)
        }
      }
    }
  }

  test("SPARK-60265: a scan ordering that a sort aggregate uses stays unpacked") {
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
          assert(collectScans(plan).forall(_.partitionPacking.isEmpty))
          val exchange = collect(plan) { case s: ShuffleExchangeExec => s }.head
          assert(collect(exchange.child) { case a: BaseAggregateExec => a }
            .forall(_.isInstanceOf[SortAggregateExec]))
        }
      }
    }
  }

  test("SPARK-60265: a keyed scan packs its inputs in key order only with V2 bucketing") {
    for (bucketing <- Seq(true, false)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ENABLED.key -> bucketing.toString,
          SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key -> "128",
          SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES.key -> "4") {
        val df = spark.read.format(classOf[SizedPackingRowSource].getName)
          .option("sizes", "100,20,100,20").option("keys", "1,2,1,2").load()
          .groupBy("j").count()
        checkAnswer(df, Seq(Row(0, 4L), Row(1, 3L), Row(2, 2L), Row(3, 1L)))
        val scan = collectScans(df.queryExecution.executedPlan).head
        // In key order, the two 100-byte inputs cannot share a task.
        assert(scan.inputRDD.getNumPartitions === (if (bucketing) 3 else 2))
      }
    }
  }

  test("SPARK-60265: a packed scan reports what it packed and is marked in both explain modes") {
    populate()
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
        // What the SQL tab shows for the scan node.
        val metrics = runAndFetchMetrics(df.collect()).collect {
          case ((_, node, name), value) if node.startsWith("BatchScan") => name -> value
        }
        assert(metrics.get("number of input partitions").contains("6"), metrics)
        assert(metrics.get("number of partitions").contains("2"), metrics)
        assert(metrics.get("max partitions per group").contains("3"), metrics)
        checkAnswer(df, Seq(Row("aa", 2L), Row("bb", 2L), Row("cc", 2L)))
        Seq(SimpleMode, FormattedMode).foreach { mode =>
          assert(df.queryExecution.explainString(mode).contains("PartitionPacking: true"), mode)
        }
      }
      withSQLConf(enabled -> "false", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
        df.collect()
        assert(!collectScans(df.queryExecution.executedPlan).head.metrics
          .contains("packingNumInputPartitions"))
        assert(!df.queryExecution.explainString(FormattedMode).contains("PartitionPacking"))
      }
    }
  }

  test("SPARK-60265: a broadcast nested loop join's streamed side is packed") {
    populate()
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(s"SELECT /*+ BROADCAST(p) */ i.id, i.name, p.price " +
          s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON i.id < p.item_id")
        checkAnswer(df, Seq("aa", "bb", "cc").map(name => Row(1, name, 10.0f)))
        val plan = df.queryExecution.executedPlan
        assert(collect(plan) { case j: BroadcastNestedLoopJoinExec => j }.size === 1)
        val streamed = collectScans(plan).find(_.table.name().endsWith(items)).get
        assert(streamed.inputRDD.getNumPartitions === 2)
      }
    }
  }

  test("SPARK-60265: barrier mode runs over a marked scan only if its inputs do not merge") {
    def read(sizes: String): DataFrame =
      spark.read.format(classOf[SizedPackingRowSource].getName).option("sizes", sizes).load()
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // Two 200-byte inputs do not fit in one 300-byte task, so each runs in its own task.
      val separate = read("200,200")
      assert(collectScans(separate.queryExecution.executedPlan).head.partitionPacking.isDefined)
      assert(separate.queryExecution.toRdd.barrier().mapPartitions(_.map(_ => 1)).count() === 3)
      // Merged inputs are read through a coalesced RDD, which barrier mode cannot run over.
      val merged = read("30,40")
      val e = intercept[SparkException] {
        merged.queryExecution.toRdd.barrier().mapPartitions(_.map(_ => 1)).count()
      }
      assert(e.getMessage.contains("SPARK-24820"), e.getMessage)
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
 * `i`; `columnar` sets the scan's columnar support mode. `hosts` gives each split one preferred
 * location; otherwise every split lists two workers. Runtime filters on `i` keep the splits of
 * the listed keys. It counts the planning calls it gets and records how many inputs each returns.
 */
class SizedPackingRowSource extends TestingV2Source {
  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      val sizes = options.get("sizes").split(",")
      val keys = Option(options.get("keys")).map(_.split(",").map(_.toInt))
      val hosts = Option(options.get("hosts")).map(_.split(","))
      val mode = Option(options.get("columnar")).map(Scan.ColumnarSupportMode.valueOf)
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
            val inputHosts = hosts.map(h => Seq(h(index))).getOrElse(Seq("worker-a", "worker-b"))
            SizedPackingRowInput(index, if (size == "?") None else Some(size.toLong),
              keys.map(_(index)), inputHosts)
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
    hosts: Seq[String] = Seq("worker-a", "worker-b"))
  extends HasPartitionStatistics with HasPartitionKey {
  def keyValue: Int = key.getOrElse(index)
  override def partitionKey(): InternalRow = InternalRow(keyValue)
  override def preferredLocations(): Array[String] = hosts.toArray
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
