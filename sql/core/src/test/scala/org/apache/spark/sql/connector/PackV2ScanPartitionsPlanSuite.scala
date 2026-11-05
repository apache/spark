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

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Observation, QueryTest, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, KeyedPartitioning}
import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.connector.expressions.{FieldReference, SortDirection}
import org.apache.spark.sql.connector.expressions.Expressions.{identity, sort}
import org.apache.spark.sql.execution.{ProjectedBroadcastValueSubqueryExec, QueryExecution, SimpleMode, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, PackV2ScanPartitions, V2ScanPartitionPacking, V2TableWriteExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, EnsureRequirements, ReusedExchangeExec, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.{broadcast, count, current_database, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.apache.spark.util.ThreadUtils

class PackV2ScanPartitionsPlanSuite extends KeyGroupedPartitioningSuiteBase {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)

  after { catalog.clearTables() }

  private def populate(): Unit = {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'cc', 1.0, NULL), (1, 'bb', 2.0, NULL), (1, 'aa', 3.0, NULL), " +
      "(2, 'cc', 4.0, NULL), (2, 'bb', 5.0, NULL), (2, 'aa', 6.0, NULL)")
  }

  test("SPARK-60265: an ordered V2 write over a packed scan") {
    populate()
    for (adaptive <- Seq(false, true)) {
      val sink = s"sink_$adaptive"
      createTable(sink, itemsColumns, Array.empty, Array(
        sort(FieldReference("name"), SortDirection.ASCENDING)))
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val writes = QueryTest.withQueryExecutionsCaptured(spark) {
          spark.table(s"testcat.ns.$items").writeTo(s"testcat.ns.$sink").append()
        }.map(_.executedPlan)
          .filter(plan => collect(plan) { case w: V2TableWriteExec => w }.nonEmpty)
        assert(writes.size === 1)
        val plan = writes.head
        val scan = collectScans(plan).head
        assert(scan.partitionPacking.isDefined)
        assert(scan.inputRDD.getNumPartitions === 2)
        // The write's required ordering comes from a sort above the scan.
        assert(collect(plan) { case s: SortExec => s }.nonEmpty)
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
        checkAnswer(spark.table(s"testcat.ns.$sink"), spark.table(s"testcat.ns.$items"))
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
        packed.partitionPacking.get.copy(maxPartitionBytes = 200))
      assert(!packed.sameResult(original))
      assert(!packed.sameResult(another))
      assert(packed.sameResult(packed.copy()))
      val distribution = ClusteredDistribution(Seq(original.output.head))
      assert(PackV2ScanPartitions(Some(distribution))(original) === original)
    }
  }

  test("SPARK-60265: keeping the root layout keeps a keyed scan below a local sort unpacked") {
    populate()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // The sort on `name` is not redundant, so only the root's partitioning reaches the scan.
      val sparkPlan = spark.table(s"testcat.ns.$items").sortWithinPartitions("name")
        .queryExecution.sparkPlan
      val planned = EnsureRequirements()(sparkPlan)
      assert(collect(planned) { case s: SortExec => s }.size === 1)
      assert(collectScans(PackV2ScanPartitions()(planned)).head.partitionPacking.isDefined)
      assert(collectScans(PackV2ScanPartitions(keepRootLayout = true)(planned)).head
        .partitionPacking.isEmpty)
    }
  }

  test("SPARK-60265: a packed scan plans its inputs no more often than an unpacked one") {
    // planInputPartitions, toBatch and createReaderFactory calls.
    def counters: Seq[Int] = Seq(SizedPackingRowReaderFactory.planned.get(),
      SizedPackingRowReaderFactory.batches.get(), SizedPackingRowReaderFactory.factories.get())
    def planningCalls(
        packing: Boolean, keyed: Boolean, mode: String, adaptive: Boolean): Seq[Int] = {
      withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key -> packing.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        SizedPackingRowReaderFactory.reset()
        val reader = spark.read.format(classOf[SizedPackingRowSource].getName)
          .option("sizes", "30,40,200,20,30,90").option("columnar", mode)
        val df = (if (keyed) reader.option("keys", "0,1,2,3,4,5") else reader).load()
          .groupBy("j").count()
        df.queryExecution.executedPlan
        val whilePlanning = counters
        checkAnswer(df, (0 until 6).map(j => Row(j, (6 - j).toLong)))
        val scan = collectScans(df.queryExecution.executedPlan).head
        assert(scan.inputRDD.getNumPartitions === (if (packing) 2 else 6))
        whilePlanning ++ counters
      }
    }
    for (keyed <- Seq(false, true); mode <- Seq("PARTITION_DEFINED", "UNSUPPORTED");
        adaptive <- Seq(false, true)) {
      val unpacked = planningCalls(packing = false, keyed, mode, adaptive)
      val packed = planningCalls(packing = true, keyed, mode, adaptive)
      withClue(s"keyed=$keyed mode=$mode adaptive=$adaptive unpacked=$unpacked packed=$packed") {
        assert(packed.zip(unpacked).forall { case (p, u) => p <= u })
        // An unkeyed scan in a fixed columnar mode plans its inputs only when it executes.
        if (!keyed && mode == "UNSUPPORTED") assert(packed.head === 0 && packed(2) === 0)
      }
    }
  }

  test("SPARK-60265: marking a scan for packing does not plan its inputs (SPARK-44505)") {
    for (mode <- Seq("SUPPORTED", "UNSUPPORTED"); adaptive <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> adaptive.toString) {
        val df = spark.read.format(classOf[ScanDefinedColumnarSupport].getName)
          .option("columnar", mode).load()
        df.queryExecution.explainString(SimpleMode)
        assert(collectScans(df.queryExecution.executedPlan).head.partitionPacking.isDefined)
        // The marked copy keeps the scan's tags, such as the logical link AQE asks for.
        Seq(() => df.count(), () => df.collect()).foreach { action =>
          val e = intercept[IllegalArgumentException](action())
          assert(e.getMessage == "planInputPartitions must not be called")
        }
      }
    }
  }

  test("SPARK-60265: copies of a marked scan reuse what the scan it was marked from planned") {
    val df = spark.read.format(classOf[SizedPackingRowSource].getName)
      .option("sizes", "30,40,200,20,30,90").load()
    val original = collectScans(df.queryExecution.sparkPlan).head
    val batch = original.batch
    val inputs = original.inputPartitions
    val factory = original.readerFactory
    SizedPackingRowReaderFactory.reset()
    val packed = original.withPartitionPacking(V2ScanPartitionPacking(100L, 0L, Some(1), None))
    val filtered = packed.copy(runtimeFilters = Seq(DynamicPruningExpression(Literal.TrueLiteral)))
    Seq(packed, packed.copy(), packed.canonicalized, filtered).foreach { plan =>
      val scan = collectScans(plan).head
      assert(scan.batch eq batch)
      assert(scan.inputPartitions eq inputs)
      assert(scan.readerFactory eq factory)
    }
    assert(packed.inputRDD.getNumPartitions === 4)
    assert(packed.inputRDD.count() === 21L)
    assert(SizedPackingRowReaderFactory.planned.get() === 0)
    assert(SizedPackingRowReaderFactory.batches.get() === 0)
    assert(SizedPackingRowReaderFactory.factories.get() === 0)
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
      for (reliable <- Seq(true, false); adaptive <- Seq(false, true);
          packingAtCheckpoint <- Seq(true, false)) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val table = spark.table(s"testcat.ns.$items")
          // A local sort on another column keeps the keyed layout below it.
          for (input <- Seq(table, table.sortWithinPartitions("name"))) {
            assert(collectScans(input.queryExecution.executedPlan).head.partitionPacking.isDefined,
              "test setup: without a consumer, the standalone scan is packed")
            // The plan was prepared with packing on, which can be off by the checkpoint.
            val checkpointed = withSQLConf(
                SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key -> packingAtCheckpoint.toString) {
              if (reliable) input.checkpoint() else input.localCheckpoint()
            }
            val df = joinWithPurchases(checkpointed)
            checkAnswer(df, expected)
            val plan = df.queryExecution.executedPlan
            withClue(s"reliable=$reliable adaptive=$adaptive " +
                s"packingAtCheckpoint=$packingAtCheckpoint\n${plan.treeString}") {
              assert(collect(plan) { case s: ShuffleExchangeExec => s }.isEmpty)
              assert(collectGroupPartitions(plan).nonEmpty)
            }
          }
        }
      }
    }
  }

  test("SPARK-60265: a checkpoint puts back packed root scans only if that changes its layout") {
    createKeyedItemsAndPurchases()
    def putsBack(df: DataFrame): Boolean = df.queryExecution.forCheckpoint ne df.queryExecution
    for (adaptive <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        withClue(s"adaptive=$adaptive") {
          // No scan, a scan below the exchange the root reports, and a scan with no layout.
          assert(!putsBack(spark.range(10).toDF()))
          assert(!putsBack(spark.table(s"testcat.ns.$items").groupBy("name").count()))
          assert(!putsBack(spark.read.format(classOf[SizedPackingRowSource].getName)
            .option("sizes", "30,40").load()))
          // A packed keyed scan at the root, which no adaptive plan wraps without an exchange.
          val df = spark.table(s"testcat.ns.$items")
          assert(collectScans(df.queryExecution.executedPlan).head.partitionPacking.isDefined)
          assert(putsBack(df))
          val checkpointExecution = df.queryExecution.forCheckpoint
          // Built for each checkpoint, in the same session, around this execution's plans.
          assert(checkpointExecution ne df.queryExecution.forCheckpoint)
          assert(checkpointExecution.sparkSession eq spark)
          assert(checkpointExecution.logical eq df.queryExecution.logical)
          assert(!checkpointExecution.refreshPhaseEnabled)
          assert(checkpointExecution.analyzed eq df.queryExecution.analyzed)
          assert(checkpointExecution.optimizedPlan eq df.queryExecution.optimizedPlan)
          assert(checkpointExecution.sparkPlan eq df.queryExecution.sparkPlan)
          val scans = collectScans(checkpointExecution.executedPlan)
          assert(scans.forall(s => s.partitionPacking.isEmpty && (s.session eq spark)))
          // The scans put back keep the tags of the packed scans, such as their logical plans.
          assert(scans.forall(_.logicalLink.isDefined))
          assert(scans.map(_.logicalLink) ===
            collectScans(df.queryExecution.executedPlan).map(_.logicalLink))
        }
      }
    }
    // An adaptive root reports no layout, whatever its scans do.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val keyed = spark.table(s"testcat.ns.$items")
      val purchased = spark.table(s"testcat.ns.$purchases")
      assert(!putsBack(keyed.join(broadcast(purchased), keyed("id") === purchased("item_id"))))
    }
    // With packing off, a Dataset that is not planned yet is left to the checkpoint's execution
    // to plan, as without packing.
    withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key -> "false") {
      val unplanned = spark.table(s"testcat.ns.$items")
      assert(!putsBack(unplanned))
      assert(!unplanned.queryExecution.tracker.phases.contains(QueryPlanningTracker.PLANNING))
    }
  }

  test("SPARK-60265: a checkpoint that puts back its root scans still packs below exchanges") {
    val expected = createKeyedItemsAndPurchases()
    import testImplicits._
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.table(s"testcat.ns.$items").as("i")
        .join(broadcast(spark.table(s"testcat.ns.$purchases").as("p")), $"i.id" === $"p.item_id")
        .select($"i.id", $"i.name", $"p.price")
      checkAnswer(df, expected)
      val checkpointPlan = df.queryExecution.forCheckpoint.executedPlan
      val scans = collectScans(checkpointPlan)
      // The streamed keyed scan reaches the root; the build side is below a broadcast exchange.
      assert(scans.find(_.table.name().endsWith(items)).get.partitionPacking.isEmpty)
      val build = scans.find(_.table.name().endsWith(purchases)).get
      assert(build.partitionPacking.isDefined)
      assert(build.inputRDD.getNumPartitions === 1)
      // The checkpoint reuses the broadcast the Dataset's plan built.
      def broadcasts(plan: SparkPlan): Seq[BroadcastExchangeLike] =
        collect(plan) { case b: BroadcastExchangeLike => b }
      assert(broadcasts(checkpointPlan).head eq broadcasts(df.queryExecution.executedPlan).head)
    }
  }

  test("SPARK-60265: observations of a checkpointed Dataset complete with packing enabled") {
    createKeyedItemsAndPurchases()
    import testImplicits._
    withTempDir { dir =>
      spark.sparkContext.setCheckpointDir(dir.getPath)
      for (reliable <- Seq(true, false); adaptive <- Seq(false, true)) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          // Without AQE, the streamed keyed scan reaches the root, so the checkpoint puts it back.
          // The observed build side is shared by both plans.
          val observation = Observation(s"purchases_${reliable}_$adaptive")
          val observed = spark.table(s"testcat.ns.$purchases")
            .observe(observation, count(lit(1)).as("rows"))
          val df = spark.table(s"testcat.ns.$items")
            .join(broadcast(observed), $"id" === $"item_id")
          val putBack = df.queryExecution.forCheckpoint ne df.queryExecution
          assert(putBack === !adaptive)
          val checkpointed = if (reliable) df.checkpoint() else df.localCheckpoint()
          assert(checkpointed.count() === 4)
          withClue(s"reliable=$reliable adaptive=$adaptive") {
            assert(ThreadUtils.awaitResult(observation.future, 10.seconds) === Row(2L))
          }
        }
      }
    }
  }

  test("SPARK-60265: a checkpoint puts back packed root scans for the layout its root reports") {
    def putsBack(df: DataFrame): Boolean = df.queryExecution.forCheckpoint ne df.queryExecution
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // An unkeyed scan ordered on `name`: the ordering reaches the root.
      createTable("ordered", itemsColumns, Array.empty,
        Array(sort(FieldReference("name"), SortDirection.ASCENDING)))
      sql("INSERT INTO testcat.ns.ordered VALUES (1, 'aa', 1.0, NULL), (2, 'bb', 2.0, NULL)")
      val ordered = spark.table("testcat.ns.ordered")
      assert(putsBack(ordered))
      val orderedLayout = ordered.queryExecution.forCheckpoint.executedPlan.outputOrdering
      assert(orderedLayout.map(_.child.references.map(_.name).toSet) === Seq(Set("name")))
      val checkpointed = ordered.localCheckpoint()
      val sorted = checkpointed.sortWithinPartitions("name").queryExecution.executedPlan
      assert(collect(sorted) { case s: SortExec => s }.isEmpty)
      // The same scan without `name`, whose ordering it then cannot report, and a local sort on
      // another column, whose ordering the root reports either way.
      assert(!putsBack(ordered.select("id")))
      assert(!putsBack(ordered.sortWithinPartitions("id")))
      // A keyed table read with V2 bucketing off reports no keyed partitioning.
      createKeyedItemsAndPurchases()
      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        assert(!putsBack(spark.table(s"testcat.ns.$items")))
      }
      // A projection that drops the key: the root reports an unknown partitioning, whose count
      // later planning reads only when it is 1.
      assert(!putsBack(spark.table(s"testcat.ns.$items").selectExpr("id + 1 AS x", "name")))
      createTable("single", itemsColumns, Array(identity("id")))
      sql("INSERT INTO testcat.ns.single VALUES (1, 'aa', 1.0, NULL)")
      assert(putsBack(spark.table("testcat.ns.single").selectExpr("id + 1 AS x", "name")))
    }
  }

  test("SPARK-60265: a checkpoint that puts back its root scans stores the Dataset's rows") {
    createKeyedItemsAndPurchases()
    import testImplicits._
    for (adaptive <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = spark.table(s"testcat.ns.$items").select($"id", $"name", current_database())
        val collected = df.collect().toSeq
        assert(df.queryExecution.forCheckpoint ne df.queryExecution)
        // A new row and another current database must not reach the checkpoint.
        sql(s"INSERT INTO testcat.ns.$items VALUES (9, 'zz', 1.0, NULL)")
        sql("USE testcat.ns")
        try {
          checkAnswer(df.localCheckpoint(), collected)
        } finally {
          sql("USE spark_catalog.default")
          sql(s"DELETE FROM testcat.ns.$items WHERE id = 9")
        }
      }
    }
  }

  test("SPARK-60265: a checkpoint that fails to plan reports the failure to its listeners") {
    for (adaptive <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val observation = Observation(s"unplanned_$adaptive")
        val df = spark.read.format(classOf[ScanDefinedColumnarSupport].getName)
          .option("columnar", "PARTITION_DEFINED").load()
          .observe(observation, count(lit(1)).as("rows"))
        val executions = QueryTest.withQueryExecutionsCaptured(spark) {
          val e = intercept[IllegalArgumentException](df.localCheckpoint())
          assert(e.getMessage == "planInputPartitions must not be called")
        }
        assert(executions.size === 1)
        // The observation has already failed with the query, rather than waiting for an action.
        assert(observation.future.value.flatMap(_.failed.toOption).map(_.getMessage) ===
          Some("planInputPartitions must not be called"))
      }
    }
  }

  test("SPARK-60265: DPP plans only a packed scan's surviving inputs and reuses its broadcast") {
    createTable("dim", Array(Column.create("k", IntegerType), Column.create("v", IntegerType)),
      Array.empty)
    sql("INSERT INTO testcat.ns.dim VALUES (2, 1), (5, 1), (7, 0)")
    def run(
        packing: Boolean,
        keyed: Boolean,
        adaptive: Boolean,
        keepRootLayout: Boolean = false): (Int, Seq[Int], SparkPlan) = {
      withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key -> packing.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        SizedPackingRowReaderFactory.reset()
        val reader = spark.read.format(classOf[SizedPackingRowSource].getName)
          .option("sizes", Seq.fill(8)("100").mkString(",")).option("columnar", "UNSUPPORTED")
        val fact = (if (keyed) reader.option("keys", (0 until 8).mkString(",")) else reader).load()
        val dim = spark.table("testcat.ns.dim").where("v = 1")
        val df = fact.join(broadcast(dim), fact("i") === dim("k")).select(fact("i"), fact("j"))
        // An execution whose root layout reaches a consumer, as a streaming micro-batch's does.
        val qe = if (keepRootLayout) {
          new QueryExecution(spark, df.logicalPlan) {
            override private[sql] def keepsRootLayout: Boolean = true
          }
        } else {
          df.queryExecution
        }
        qe.executedPlan
        val whilePlanning = SizedPackingRowReaderFactory.planned.get()
        val rows = qe.executedPlan.executeCollect().map(row => (row.getInt(0), row.getInt(1)))
        assert(rows.toSet === Seq(2, 5).flatMap(i => (0 to i).map(j => (i, j))).toSet)
        (whilePlanning, SizedPackingRowReaderFactory.plannedInputs.asScala.toSeq, qe.executedPlan)
      }
    }
    // Scans that DPP prunes, broadcasts, and reused exchanges.
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
    for (keyed <- Seq(false, true); adaptive <- Seq(false, true);
        keepRootLayout <- Seq(false, true)) {
      val (unpackedPlanning, unpackedInputs, unpackedPlan) = run(false, keyed, adaptive)
      val (packedPlanning, packedInputs, packedPlan) = run(true, keyed, adaptive, keepRootLayout)
      withClue(s"keyed=$keyed adaptive=$adaptive keepRootLayout=$keepRootLayout " +
          s"unpacked=$unpackedInputs packed=$packedInputs") {
        assert(reuse(unpackedPlan)._1 > 0, "test setup: DPP prunes the scan")
        assert(reuse(unpackedPlan)._3 > 0, "test setup: DPP reuses the join's broadcast")
        assert(reuse(packedPlan) === reuse(unpackedPlan))
        assert(packedPlanning <= unpackedPlanning)
        assert(packedInputs.size <= unpackedInputs.size)
        if (!keyed) {
          // Nothing is planned while planning, then one call returns the two kept inputs.
          assert(packedPlanning === 0)
          assert(packedInputs === Seq(2))
        }
        // The broadcast below DPP's subquery and the join, which may reuse the other.
        val dimScans = collectWithSubqueries(packedPlan) {
          case s: BatchScanExec if s.table.name().endsWith("dim") => s
        }
        assert(dimScans.nonEmpty && dimScans.forall(_.partitionPacking.isDefined))
      }
    }
  }

  test("SPARK-60265: DPP projected from a broadcast is planned as with packing off") {
    createTable("codes",
      Array(Column.create("join_id", IntegerType), Column.create("part", IntegerType)), Array.empty)
    sql("INSERT INTO testcat.ns.codes VALUES (1, 2), (2, 5), (3, 7)")
    createTable("selectors",
      Array(Column.create("join_id", IntegerType), Column.create("keep", BooleanType)), Array.empty)
    sql("INSERT INTO testcat.ns.selectors VALUES (1, true), (2, true), (3, false)")
    def run(packing: Boolean, adaptive: Boolean, keepRootLayout: Boolean): SparkPlan = {
      // Without an exchange in the projection's source plan, AQE plans that plan only when forced,
      // as AQE's test suites force it.
      withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key -> packing.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_BROADCAST_PROJECTION_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        spark.read.format(classOf[SizedPackingRowSource].getName)
          .option("sizes", Seq.fill(8)("100").mkString(",")).option("columnar", "UNSUPPORTED")
          .load().createOrReplaceTempView("fact")
        // DPP projects the fact's keys from the rows of the codes broadcast, which it reuses.
        val df = sql(
          """WITH selected AS (
            |  SELECT /*+ BROADCAST(c) */ c.part FROM testcat.ns.codes c
            |  JOIN testcat.ns.selectors x ON c.join_id = x.join_id WHERE x.keep AND x.join_id > 0
            |)
            |SELECT /*+ MERGE(f, s) */ f.i, f.j FROM fact f JOIN selected s ON f.i = s.part
            |""".stripMargin)
        // A micro-batch's AQE plans a DPP source plan as an adaptive plan of its own.
        val qe = if (keepRootLayout) {
          new QueryExecution(spark, df.logicalPlan) {
            override private[sql] def keepsRootLayout: Boolean = true
          }
        } else {
          df.queryExecution
        }
        val rows = qe.executedPlan.executeCollect().map(row => (row.getInt(0), row.getInt(1)))
        assert(rows.toSet === Seq(2, 5).flatMap(i => (0 to i).map(j => (i, j))).toSet)
        qe.executedPlan
      }
    }
    // Projections of a broadcast, and reused exchanges.
    def projected(plan: SparkPlan): (Int, Int) = (
      collectWithSubqueries(plan) { case s: ProjectedBroadcastValueSubqueryExec => s }.size,
      collectWithSubqueries(plan) { case r: ReusedExchangeExec => r }.size)
    withTempView("fact") {
      for (adaptive <- Seq(false, true); keepRootLayout <- Seq(false, true)) {
        val unpacked = run(packing = false, adaptive, keepRootLayout)
        val packed = run(packing = true, adaptive, keepRootLayout)
        withClue(s"adaptive=$adaptive keepRootLayout=$keepRootLayout") {
          assert(projected(unpacked)._1 === 1, "test setup: DPP projects the broadcast")
          assert(projected(unpacked)._2 > 0, "test setup: DPP reuses the broadcast")
          assert(projected(packed) === projected(unpacked))
          val codesScans = collectWithSubqueries(packed) {
            case s: BatchScanExec if s.table.name().endsWith("codes") => s
          }
          assert(codesScans.nonEmpty && codesScans.forall(_.partitionPacking.isDefined))
        }
      }
    }
  }

  test("SPARK-60265: a streaming micro-batch keeps the layout of a scan reaching its root") {
    createKeyedItemsAndPurchases()
    import testImplicits._
    implicit val sqlContext: SQLContext = spark.sqlContext
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val input = MemoryStream[Long]
      val shuffles = new ConcurrentLinkedQueue[Int]()
      val sink: (DataFrame, Long) => Unit = { (batch, _) =>
        val grouped = batch.groupBy("id").count()
        assert(grouped.collect().map(_.getLong(1)).toSeq === Seq(2L, 2L))
        shuffles.add(collect(grouped.queryExecution.executedPlan) {
          case s: ShuffleExchangeExec => s
        }.size)
      }
      val keys = input.toDF().withColumnRenamed("value", "key")
      val query = spark.table(s"testcat.ns.$items").join(broadcast(keys), $"id" === $"key")
        .writeStream.foreachBatch(sink).start()
      try {
        input.addData(1L, 3L)
        query.processAllAvailable()
      } finally {
        query.stop()
      }
      // The batch reports the scan's keyed layout, so grouping by the key needs no shuffle.
      assert(shuffles.asScala.toSeq === Seq(0))
    }
  }

  test("SPARK-60265: a checkpoint after an action records the keys planned before DPP") {
    createTable("dim", Array(Column.create("k", IntegerType), Column.create("v", IntegerType)),
      Array.empty)
    sql("INSERT INTO testcat.ns.dim VALUES (2, 1), (5, 1), (7, 0)")
    for (packing <- Seq(false, true)) {
      withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key -> packing.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val fact = spark.read.format(classOf[SizedPackingRowSource].getName)
          .option("sizes", Seq.fill(8)("100").mkString(","))
          .option("keys", (0 until 8).mkString(",")).option("columnar", "UNSUPPORTED").load()
        val dim = spark.table("testcat.ns.dim").where("v = 1")
        val df = fact.join(broadcast(dim), fact("i") === dim("k")).select(fact("i"), fact("j"))
        // The action narrows the shared Scan to the two keys DPP keeps.
        SizedPackingRowReaderFactory.reset()
        val collected = df.collect().toSeq
        assert(SizedPackingRowReaderFactory.plannedInputs.asScala.toSeq.last === 2)
        // With packing, the first checkpoint's put-back scan applies its runtime filter to the
        // Scan once more; it reuses the inputs planned before DPP for the keys it reports.
        val added = (if (packing) Seq(Seq(2), Seq.empty) else Seq(Seq.empty, Seq.empty))
        for ((expectedPlanning, checkpoint) <- added.zipWithIndex) {
          val before = SizedPackingRowReaderFactory.plannedInputs.size
          val checkpointed = df.localCheckpoint()
          checkAnswer(checkpointed, collected)
          val layout = checkpointed.queryExecution.executedPlan.outputPartitioning
          withClue(s"packing=$packing checkpoint=$checkpoint $layout") {
            assert(SizedPackingRowReaderFactory.plannedInputs.asScala.toSeq.drop(before) ===
              expectedPlanning)
            assert(layout.isInstanceOf[KeyedPartitioning])
            assert(layout.numPartitions === 8)
          }
        }
      }
    }
  }

  test("SPARK-60265: a streaming file sink keeps the ordering of a scan reaching its root") {
    createKeyedItemsAndPurchases()
    import testImplicits._
    implicit val sqlContext: SQLContext = spark.sqlContext
    for (adaptive <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withTempDir { dir =>
          val input = MemoryStream[Long]
          val keys = input.toDF().withColumnRenamed("value", "key").as("s")
          val df = spark.table(s"testcat.ns.$items").as("i")
            .join(broadcast(keys), $"i.id" === $"s.key")
            .join(broadcast(spark.table(s"testcat.ns.$purchases").as("p")),
              $"i.id" === $"p.item_id")
            .select($"i.id", $"i.name", $"p.price")
          val output = new File(dir, "output").getPath
          val query = df.writeStream.format("parquet").partitionBy("id")
            .option("checkpointLocation", new File(dir, "checkpoint").getPath).start(output)
          try {
            input.addData(1L, 3L)
            query.processAllAvailable()
            // The keyed scan's ordering on the partition column reaches the writer, so it does not
            // sort, while the broadcast purchases scan is still packed.
            assert(FileFormatWriter.outputOrderingMatched, s"adaptive=$adaptive")
            val plan = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution
              .executedPlan
            assert(plan.isInstanceOf[AdaptiveSparkPlanExec] === adaptive)
            val scans = collectScans(plan)
            assert(scans.find(_.table.name().endsWith(items)).get.partitionPacking.isEmpty)
            assert(scans.find(_.table.name().endsWith(purchases)).get.partitionPacking.isDefined)
          } finally {
            query.stop()
          }
          checkAnswer(spark.read.parquet(output).select("id", "name", "price"),
            Seq(Row(1L, "aa", 42.0f), Row(1L, "ab", 42.0f), Row(3L, "cc", 19.5f),
              Row(3L, "cd", 19.5f)))
        }
      }
    }
  }

  test("SPARK-60265: a checkpoint on a thread without an active session puts back its scans") {
    createKeyedItemsAndPurchases()
    for (adaptive <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = spark.table(s"testcat.ns.$items")
        val expected = df.collect().toSeq
        // As on a PySpark thread other than the one that created the session.
        var result: Try[DataFrame] = null
        val thread = new Thread(() => {
          SparkSession.clearActiveSession()
          result = Try(df.localCheckpoint())
        })
        thread.start()
        thread.join()
        val checkpointed = result.get
        checkAnswer(checkpointed, expected)
        assert(checkpointed.queryExecution.executedPlan.outputPartitioning
          .isInstanceOf[KeyedPartitioning])
        // The Dataset can be checkpointed again on this thread.
        checkAnswer(df.localCheckpoint(), expected)
      }
    }
  }
}
