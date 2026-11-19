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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.rdd.SortedMergeCoalescedRDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, BloomFilterMightContain, DynamicPruningExpression, DynamicPruningSubquery, Expression, GetStructField, Literal, PlanExpression, Rand, ScalarSubquery, TransformExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.{CleanupPerPartitionRuntimeFilters, PerPartitionRuntimeFilterSubquery}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PER_PARTITION_RUNTIME_FILTER_SUBQUERY
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryRowLevelOperationTableCatalog, SupportsRowLevelOperations, Table}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, BucketFunction, LongDaysFunction, ScalarFunction, UnboundBucketFunction, UnboundFunction, UnboundIntBucketFunction, UnboundPhysicalDaysFunction}
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, NullOrdering, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, days, identity, sort}
import org.apache.spark.sql.connector.read.{Scan, SupportsStorageFilters}
import org.apache.spark.sql.connector.write.{RowLevelOperation, RowLevelOperationInfoImpl, RowLevelOperationTable}
import org.apache.spark.sql.execution.{FormattedMode, ProjectExec, ScalarSubquery => ExecScalarSubquery, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, DataSourceV2ScanRelation, GroupPartitionsExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec}
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.DataSourceV2Test

/**
 * Tests per-partition runtime bloom filters, which the scan of the application side applies as a
 * storage filter. Each split of that scan gets the bloom filter of the creation side's partition
 * with the split's partition key. The scans of `testcat` take storage filters, and record what
 * their readers read, see [[StorageFilterInMemoryCatalog]].
 */
@DataSourceV2Test
class PerPartitionRuntimeFilterSuite extends KeyGroupedPartitioningSuiteBase {

  override protected def catalogClassName: String = classOf[StorageFilterInMemoryCatalog].getName

  // The in-memory scans are small, so the minimum size of the application side is set to 0.
  override def sparkConf: SparkConf = super.sparkConf
    .set(V2_BUCKETING_ENABLED, true)
    .set(AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    .set(RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD, 0L)
    .set(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED, true)
    .set(V2_STORAGE_FILTER_PUSHDOWN_ENABLED, true)

  before {
    catalog.createFunction(Identifier.of(Array.empty, UnboundBucketFunction.name()),
      UnboundBucketFunction)
  }

  after {
    catalog.clearTables()
    catalog.clearFunctions()
  }

  // A table with the table properties `properties`, one split per row. With `runtimeFiltering`,
  // its scans take runtime filters on the source columns of its partition transforms, as Iceberg's
  // do. Without it they take none, so no DPP filter keeps a key from a bloom filter.
  private def createTableWith(
      table: String,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: Map[String, String] = Map.empty,
      runtimeFiltering: Boolean = false,
      ordering: Array[SortOrder] = Array.empty): Unit = {
    val filtering =
      if (runtimeFiltering) Map.empty[String, String] else Map("filter-attributes" -> "")
    catalog.createTable(Identifier.of(Array("ns"), table), columns, partitions,
      (filtering ++ properties).asJava, Distributions.unspecified(), ordering, None, None,
      numRowsPerSplit = 1)
  }

  // The table property of a table whose scans read in batches, when `columnar`.
  private def columnarReads(columnar: Boolean): Map[String, String] =
    Option.when(columnar)(StorageFilterInMemoryCatalog.ColumnarKey -> "true").toMap

  // `fact` and `dim`, both bucketed by `id` into 4 by default. Then the 6 rows of `dim` have 3
  // partition keys, since the test catalog's `bucket` takes the value modulo the number of buckets.
  // The rows of `dim` with `flag = 1` have the ids 1, 5 and 9 in bucket 1, and 3 in bucket 3. With
  // `factDeclared`, the scan of `fact` takes storage filters on those columns only. With
  // `factColumnar`, it reads in batches.
  private def createTables(
      idType: DataType = LongType,
      factPartitioning: Array[Transform] = Array(bucket(4, "id")),
      dimPartitioning: Array[Transform] = Array(bucket(4, "id")),
      numFactRows: Int = 16,
      factDeclared: Option[String] = None,
      runtimeFiltering: Boolean = false,
      factColumnar: Boolean = false): Unit = {
    createTableWith("fact", Array(Column.create("id", idType), Column.create("v", IntegerType)),
      factPartitioning, factDeclared.map(StorageFilterInMemoryCatalog.DeclaredKey -> _).toMap ++
        columnarReads(factColumnar), runtimeFiltering)
    sql("INSERT INTO testcat.ns.fact VALUES " +
      (0 until numFactRows).map(i => s"($i, ${i * 10})").mkString(", "))
    createTableWith("dim",
      Array(Column.create("id", idType), Column.create("flag", IntegerType)), dimPartitioning,
      runtimeFiltering = runtimeFiltering)
    sql("INSERT INTO testcat.ns.dim VALUES (1, 1), (2, 0), (3, 1), (5, 1), (6, 0), (9, 1)")
  }

  // A table `name` of `id BIGINT` and an INT column `column`, bucketed by `id` into 4, with the
  // rows of the VALUES list `values`.
  private def createIdTable(
      name: String,
      column: String,
      values: String,
      runtimeFiltering: Boolean = false): Unit = {
    createTableWith(name, Array(Column.create("id", LongType), Column.create(column, IntegerType)),
      Array(bucket(4, "id")), runtimeFiltering = runtimeFiltering)
    sql(s"INSERT INTO testcat.ns.$name VALUES $values")
  }

  // Replaces the catalog's `bucket` with a function that computes `produce`.
  private def createBucketFunction(deterministic: Boolean = true)(
      produce: InternalRow => Integer): Unit = {
    catalog.clearFunctions()
    catalog.createFunction(Identifier.of(Array.empty, "bucket"), new UnboundFunction {
      override def bind(inputType: StructType): BoundFunction = new ScalarFunction[Integer] {
        override def inputTypes(): Array[DataType] = BucketFunction.inputTypes()
        override def resultType(): DataType = BucketFunction.resultType()
        override def name(): String = BucketFunction.name()
        override def canonicalName(): String = BucketFunction.canonicalName()
        override def isDeterministic(): Boolean = deterministic
        override def produceResult(input: InternalRow): Integer = produce(input)
      }
      override def description(): String = name()
      override def name(): String = "bucket"
    })
  }

  private def join(): DataFrame = sql(
    """
      |SELECT f.id, f.v
      |FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id
      |WHERE d.flag = 1
      |""".stripMargin)

  // A table `other` of `id BIGINT`, with the ids 0 to 15 bucketed into 4.
  private def createOtherTable(): Unit = {
    createTableWith("other", Array(Column.create("id", LongType)), Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.other VALUES " + (0 until 16).map(i => s"($i)").mkString(", "))
  }

  // The join with `dim` of the join of `fact` and `other`, on the same key.
  private def joinOverJoin(): DataFrame = {
    createOtherTable()
    sql(
      """
        |SELECT f.id, f.v
        |FROM (SELECT fact.id, fact.v FROM testcat.ns.fact JOIN testcat.ns.other
        |      ON fact.id = other.id) f
        |JOIN testcat.ns.dim d ON f.id = d.id
        |WHERE d.flag = 1
        |""".stripMargin)
  }

  private val expectedRows = Seq(Row(1, 10), Row(3, 30), Row(5, 50), Row(9, 90))

  // The size of the creation side that a join on `column` gets from `table` with `flag = 1`.
  private def creationSideSize(table: String, column: String = "id"): BigInt =
    spark.table(s"testcat.ns.$table").where("flag = 1").select(column)
      .queryExecution.optimizedPlan.stats.sizeInBytes

  // Runs `f` with the creation side threshold at about half the size of the creation side that a
  // join gets from `table`. That side is then too big for one bloom filter of all its keys. With 2
  // partition keys or more, each share is within the threshold.
  private def withoutGlobalBloomFilter(table: String, column: String = "id")(f: => Unit): Unit =
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
      ((creationSideSize(table, column) + 1) / 2).toString)(f)

  // `fact` and `dim` with a second join key `x`, which the partitioning leaves out.
  private def createTwoKeyTables(factColumnar: Boolean = false): Unit = {
    createTableWith("fact", Array(Column.create("id", LongType), Column.create("x", IntegerType),
      Column.create("v", IntegerType)), Array(bucket(4, "id")), columnarReads(factColumnar))
    sql("INSERT INTO testcat.ns.fact VALUES " +
      (0 until 16).map(i => s"($i, ${i % 3}, ${i * 10})").mkString(", "))
    createTableWith("dim", Array(Column.create("id", LongType), Column.create("x", IntegerType),
      Column.create("flag", IntegerType)), Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.dim VALUES (1, 1, 1), (2, 2, 0), (3, 0, 1), (5, 2, 1), (9, 0, 1)")
  }

  // The inner joins of `plan` with no other inner join below them.
  private def lowestInnerJoins(plan: LogicalPlan): Seq[Join] = {
    def isInner(p: LogicalPlan): Boolean = p match {
      case j: Join => j.joinType == Inner
      case _ => false
    }
    plan.collect { case j: Join if isInner(j) && !j.children.exists(_.exists(isInner)) => j }
  }

  // The placeholders of per-partition bloom filters in the expressions of `plan`.
  private def placeholdersIn(plan: LogicalPlan): Seq[PerPartitionRuntimeFilterSubquery] =
    plan.flatMap(_.expressions.flatMap(_.collect {
      case p: PerPartitionRuntimeFilterSubquery => p
    }))

  // The placeholders that the optimized plan of `df` kept for the scans below them.
  private def perPartitionFilters(df: DataFrame): Seq[PerPartitionRuntimeFilterSubquery] =
    placeholdersIn(df.queryExecution.optimizedPlan)

  // The placeholders that the optimized plan of `df` kept, in its subqueries too.
  private def perPartitionFiltersWithSubqueries(
      df: DataFrame): Seq[PerPartitionRuntimeFilterSubquery] =
    df.queryExecution.optimizedPlan.collectWithSubqueries { case node => node.expressions }
      .flatten.flatMap(_.collect { case p: PerPartitionRuntimeFilterSubquery => p })

  // Whether `CleanupPerPartitionRuntimeFilters` gave a placeholder of `df` way to `true`.
  private def replacedPlaceholders(df: DataFrame): Boolean = {
    df.queryExecution.optimizedPlan
    df.queryExecution.tracker.rules.get(CleanupPerPartitionRuntimeFilters.ruleName)
      .exists(_.numEffectiveInvocations > 0)
  }

  // The scans of `table` that `plan` runs, in its subqueries too. A reused subquery counts once.
  private def executedScans(plan: SparkPlan, table: String): Seq[BatchScanExec] =
    collectWithSubqueries(plan) { case scan: BatchScanExec => scan }
      .filter(_.table.name().split('.').last == table)

  // The scans of `plan` with per-partition bloom filters.
  private def storageFilterScans(plan: SparkPlan): Seq[BatchScanExec] =
    collectWithSubqueries(plan) {
      case scan: BatchScanExec if scan.perPartitionBloomFilter.isDefined => scan
    }

  // The table of a scan.
  private def tableOf(scan: BatchScanExec): String = scan.table.name().split('.').last

  // A bloom filter of the whole creation side, which a subquery computes, alone or merged with
  // others into one.
  private def isGlobalCheck(e: Expression): Boolean = e match {
    case BloomFilterMightContain(
        _: PlanExpression[_] | GetStructField(_: PlanExpression[_], _, _), _) => true
    case _ => false
  }

  private def hasGlobalBloomFilter(plan: LogicalPlan): Boolean = plan.exists {
    case Filter(condition, _) => condition.exists(isGlobalCheck)
    case _ => false
  }

  // With `dim` too big for a bloom filter of all its keys, the optimized plan of `df` has no
  // runtime bloom filter of either kind.
  private def checkNoRuntimeFilter(df: => DataFrame): Unit = withoutGlobalBloomFilter("dim") {
    val plan = df.queryExecution.optimizedPlan
    assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY), plan)
    assert(!hasGlobalBloomFilter(plan), plan)
    checkAnswer(df, expectedRows)
  }

  private def executedShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] =
    collect(plan) { case s: ShuffleExchangeExec => s }

  // The rows of a new run of `query`, and the reads of `fact`'s splits in it: the split's partition
  // key, the rows the reader read, and those it kept. It fails when a reader of `fact` gets no
  // storage filter. It needs a new DataFrame, not one `checkAnswer` ran. Another run of the same
  // plan reuses its shuffles. With `probes`, it also returns what `probed` records, for the
  // readers of every table.
  private def factReadsOf(query: => DataFrame, probes: Seq[Any] = Nil)
      : (Set[Row], Seq[(String, Int, Int)], Set[(String, Set[Any])]) = {
    StorageFilterInMemoryCatalog.reads.clear()
    StorageFilterInMemoryCatalog.plainReads.clear()
    StorageFilterInMemoryCatalog.probed.clear()
    StorageFilterInMemoryCatalog.probes = probes
    try {
      val rows = query.collect().toSet
      val plainReads = StorageFilterInMemoryCatalog.plainReads.asScala.toSeq
        .filter(_._1.split('.').last == "fact")
      assert(plainReads.isEmpty, s"`fact` must not be read without a storage filter: $plainReads")
      val reads = StorageFilterInMemoryCatalog.reads.asScala.toSeq.collect {
        case (t, key, read, kept) if t.split('.').last == "fact" => (key, read, kept)
      }
      (rows, reads, StorageFilterInMemoryCatalog.probed.asScala.toSet)
    } finally {
      StorageFilterInMemoryCatalog.probes = Nil
    }
  }

  // The partition keys of `fact`'s splits that a new run of `query` reads, see `factReadsOf`.
  private def factReadKeysOf(query: => DataFrame): Set[String] =
    factReadsOf(query)._2.map(_._1).toSet

  // The number of readers of `table` that a new run of `query` opens without a storage filter.
  private def plainReadsOf(table: String)(query: => DataFrame): Int = {
    StorageFilterInMemoryCatalog.plainReads.clear()
    query.collect()
    StorageFilterInMemoryCatalog.plainReads.asScala.count(_._1.split('.').last == table)
  }

  test("a creation side within the size threshold gets only a bloom filter of all its keys") {
    createTables()
    val df = join()
    val plan = df.queryExecution.optimizedPlan
    assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY), plan)
    assert(hasGlobalBloomFilter(plan), plan)
    checkAnswer(df, expectedRows)
  }

  test("a creation side too big for a bloom filter of all its keys gets a per-partition one") {
    createTables()
    withoutGlobalBloomFilter("dim") {
      val df = join()
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionFilters(df).size == 1 && !hasGlobalBloomFilter(plan), plan)
      checkAnswer(df, expectedRows)
    }
  }

  Seq(true, false).foreach { adaptive =>
    Seq(false, true).foreach { columnar =>
      test(s"each split of the scan gets the bloom filter of its partition key (AQE $adaptive, " +
          s"columnar $columnar)") {
        createTables(factColumnar = columnar)
        withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          withoutGlobalBloomFilter("dim") {
            val df = join()
            checkAnswer(df, expectedRows)
            val plan = df.queryExecution.executedPlan
            assert(executedShuffles(plan).isEmpty, plan)
            val scans = storageFilterScans(plan)
            assert(scans.map(tableOf) == Seq("fact"), plan)
            // A columnar scan reads the aggregate in batches too.
            assert(scans.forall(s => s.supportsColumnar == columnar &&
              s.bloomFilterAggregate.forall(_.supportsColumnar == columnar)), s"test setup:\n$plan")
            // The scan reads its columns, which its child does not give it.
            assert(scans.forall(_.missingInput.isEmpty), plan)
            // Bucket 2 has no row of `dim` with `flag = 1`, so no reader opens a split of it. The
            // scan counts its 4 splits as skipped. The join reads none of bucket 0, which `dim`
            // does not have. Each split of buckets 1 and 3 is read with the bloom filter of its
            // bucket, which holds the keys of that bucket only.
            val measured = join()
            val (rows, reads, probed) = factReadsOf(measured, probes = Seq(1L, 3L, 5L, 9L))
            assert(rows == expectedRows.toSet)
            assert(reads.map(_._1).sorted == Seq("1", "1", "1", "1", "3", "3", "3", "3"), reads)
            assert(reads.map(_._3).sum == 4, s"the readers keep only the matching ids: $reads")
            assert(probed == Set("1" -> Set(1L, 5L, 9L), "3" -> Set(3L)), probed)
            val skipped = storageFilterScans(measured.queryExecution.executedPlan)
              .map(_.metrics("numSkippedSplits").value)
            assert(skipped == Seq(4L), s"the 4 splits of bucket 2 are skipped: $skipped")
          }
        }
      }
    }

    test(s"a task reads the aggregate of a key once for all its splits of that key " +
        s"(AQE $adaptive)") {
      // Each task of the storage-partitioned join reads the 4 splits of one bucket of `fact`, and
      // the aggregate of that bucket, with `dim` under it, once. So `dim` is read at most twice as
      // often as without per-partition bloom filters, once by the join and once by the aggregate.
      // That holds also when one task reads all the buckets, one after another.
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          Seq[() => DataFrame](() => join(), () => join().coalesce(1)).foreach { query =>
            val df = query()
            checkAnswer(df, expectedRows)
            val plan = df.queryExecution.executedPlan
            assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
            val without = withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
              plainReadsOf("dim")(query())
            }
            val reads = plainReadsOf("dim")(query())
            assert(reads <= 2 * without, s"`dim` is read $reads times, and $without without")
          }
        }
      }
    }

    test(s"the scan reads without its bloom filters when the join shuffles it and a key has " +
        s"several splits (AQE $adaptive)") {
      // Without pushing the partition values, the two sides' different keys cannot pair, so the
      // join shuffles. The 4 splits of each bucket of `fact` then have a task each. Each such task
      // would read the aggregate of its key, and `dim` under it, again. So the scan reads without
      // its bloom filters, and `dim` is read as often as without per-partition bloom filters.
      createTables()
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false") {
        withoutGlobalBloomFilter("dim") {
          val df = join()
          assert(perPartitionFilters(df).size == 1, "test setup: the rule added it")
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedShuffles(plan).nonEmpty, s"test setup: the join must shuffle:\n$plan")
          assert(storageFilterScans(plan).isEmpty, plan)
          val without = withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
            plainReadsOf("dim")(join())
          }
          assert(plainReadsOf("dim")(join()) == without)
        }
      }
    }

    test(s"under a join that shuffles it, the scan keeps its bloom filters when it has one split " +
        s"for each key (AQE $adaptive)") {
      // `fact` has one row in each bucket, so one split for each key. Without pushing the partition
      // values, the two sides' different keys cannot pair, so the join shuffles. Each task still
      // reads the splits of one key.
      createTables(numFactRows = 4)
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false") {
        withoutGlobalBloomFilter("dim") {
          val df = join()
          checkAnswer(df, Seq(Row(1, 10), Row(3, 30)))
          val plan = df.queryExecution.executedPlan
          assert(executedShuffles(plan).nonEmpty, s"test setup: the join must shuffle:\n$plan")
          assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
          assert(factReadKeysOf(join()) == Set("1", "3"))
        }
      }
    }

    test(s"a shuffle of the scan with its bloom filters is not reused for a scan of the same " +
        s"table without them (AQE $adaptive)") {
      // Without pushing the partition values, both joins shuffle `fact` by `id`. `fact` has one
      // row in each bucket, so one split for each key. The scan below the join with `dim` then
      // keeps its bloom filters under that shuffle. Only that scan gets them, so the join with
      // `plain` reads all of `fact`.
      createTables(numFactRows = 4)
      createTableWith("plain", Array(Column.create("id", LongType)), Array.empty)
      sql("INSERT INTO testcat.ns.plain VALUES " + (0 until 16).map(i => s"($i)").mkString(", "))
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false") {
        withoutGlobalBloomFilter("dim") {
          val df = sql(
            """SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id
              |WHERE d.flag = 1
              |UNION ALL
              |SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.plain p ON f.id = p.id
              |""".stripMargin)
          checkAnswer(df, Seq(Row(1, 10), Row(3, 30)) ++ (0 until 4).map(i => Row(i, i * 10)))
          val plan = df.queryExecution.executedPlan
          assert(executedShuffles(plan).size == 4, s"test setup: both joins shuffle:\n$plan")
          assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
        }
      }
    }

    test(s"the scan reads without its bloom filters when the join shuffles on more keys " +
        s"(AQE $adaptive)") {
      // The join's keys are more than the partition key, so it shuffles both sides. The 4 splits
      // of each bucket of `fact` then have a task each.
      createTwoKeyTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
            "ON f.id = d.id AND f.x = d.x WHERE d.flag = 1")
          assert(perPartitionFilters(df).size == 1, "test setup: the rule added it")
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedShuffles(plan).nonEmpty, s"test setup: the join must shuffle:\n$plan")
          assert(storageFilterScans(plan).isEmpty, plan)
        }
      }
    }

    test(s"a placeholder that pushdown leaves above a join gives way to true (AQE $adaptive)") {
      // `rand()` keeps the placeholders of `dim1` and `dim2` above the projection over the join of
      // `fact1` and `fact2`, so no scan is right below them.
      createIdTable("fact1", "v", (0 until 16).map(i => s"($i, ${i * 10})").mkString(", "))
      createIdTable("fact2", "w", (0 until 16).map(i => s"($i, ${i * 10})").mkString(", "))
      Seq("dim1", "dim2").foreach { name =>
        createIdTable(name, "flag", "(1, 1), (2, 0), (3, 1), (5, 1), (6, 0), (9, 1)")
      }
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
            ((creationSideSize("dim1") + 1) / 2).toString) {
        val df = sql("SELECT t.id, t.v, t.w, t.r < 1 FROM (SELECT f1.id, f1.v, f2.id AS id2, " +
          "f2.w, rand() AS r FROM testcat.ns.fact1 f1 JOIN testcat.ns.fact2 f2 ON f1.id = f2.id) " +
          "t JOIN testcat.ns.dim1 d1 ON t.id = d1.id JOIN testcat.ns.dim2 d2 ON t.id2 = d2.id " +
          "WHERE d1.flag = 1 AND d2.flag = 1")
        val plan = df.queryExecution.optimizedPlan
        assert(replacedPlaceholders(df), s"test setup: the rule must have had placeholders:\n$plan")
        assert(perPartitionFilters(df).isEmpty, plan)
        checkAnswer(df, Seq(1, 3, 5, 9).map(i => Row(i, i * 10, i * 10, true)))
      }
    }

    test(s"with partially clustered distribution, each split gets the bloom filter of its " +
        s"bucket (AQE $adaptive)") {
      createTables(numFactRows = 400)
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        withoutGlobalBloomFilter("dim") {
          val df = join()
          checkAnswer(df, expectedRows)
          assert(storageFilterScans(df.queryExecution.executedPlan).size == 1,
            df.queryExecution.executedPlan)
          // Buckets 1 and 3 have 100 splits each, and each holds one row.
          val (rows, reads, probed) = factReadsOf(join(), probes = Seq(1L, 3L, 5L, 9L))
          assert(rows == expectedRows.toSet)
          assert(reads.map(_._1).toSet == Set("1", "3") && reads.size == 200, reads)
          val kept = reads.map(_._3).sum
          assert(kept >= 4 && kept < 100, s"the bloom filters must reject most rows: $kept")
          assert(probed == Set("1" -> Set(1L, 5L, 9L), "3" -> Set(3L)), probed)
        }
      }
    }

    test(s"the scan keeps its bloom filters when it is read more than once (AQE $adaptive)") {
      // A cartesian product reads each side once for each partition of the other. A broadcast
      // nested loop join that also returns the broadcast rows with no match reads its streamed side
      // twice. A range shuffle reads its child once to sample it, and once more to shuffle it. Each
      // read reads the bloom filters again.
      createTables()
      createOtherTable()
      val filteredSide = "SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
        "ON f.id = d.id WHERE d.flag = 1"
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          Seq(
            s"SELECT count(*), sum(x.v) FROM ($filteredSide) x CROSS JOIN testcat.ns.other o" ->
              Seq(Row(64, 2880)),
            s"SELECT count(*), sum(x.v) FROM ($filteredSide) x FULL OUTER JOIN " +
              "testcat.ns.other o ON x.v < o.id" -> Seq(Row(19, 220)),
            s"$filteredSide ORDER BY f.v" -> expectedRows).foreach { case (query, answer) =>
            val df = sql(query)
            checkAnswer(df, answer)
            val plan = df.queryExecution.executedPlan
            assert(collect(plan) {
              case j: CartesianProductExec => j
              case j: BroadcastNestedLoopJoinExec => j
              case s: ShuffleExchangeExec if s.outputPartitioning.isInstanceOf[RangePartitioning] =>
                s
            }.size == 1, s"test setup: the side must be read again:\n$plan")
            assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
            assert(factReadKeysOf(sql(query)) == Set("1", "3"))
          }
        }
      }
    }

    test(s"with partially clustered distribution, the scan keeps its bloom filters when the " +
        s"join repeats it (AQE $adaptive)") {
      // `fact` is the smaller side, so the join repeats it and keeps the splits of `dim`.
      createIdTable("fact", "v", "(1, 10), (2, 20), (3, 30), (5, 50)")
      createIdTable("dim", "flag", (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
      // The join of `fact` and `dim`, with its answer checked.
      def checkedJoin(): DataFrame = {
        val df = join()
        checkAnswer(df, Seq(Row(1, 10), Row(3, 30), Row(5, 50)))
        df
      }
      // The partitions of the join of `fact` and `dim`.
      def joinPartitions(plan: SparkPlan): Seq[Int] = collect(plan) {
        case j: BaseJoinExec if j.joinType == Inner => j.outputPartitioning.numPartitions
      }
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        withoutGlobalBloomFilter("dim") {
          val without = withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
            joinPartitions(checkedJoin().queryExecution.executedPlan)
          }
          assert(without.size == 1 && without.head > 4,
            s"test setup: the join must keep the splits of `dim`, got $without")
          val plan = checkedJoin().queryExecution.executedPlan
          assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
          assert(joinPartitions(plan) == without, plan)
          assert(factReadKeysOf(join()) == Set("1", "3"))
        }
      }
    }

    test(s"with partially clustered distribution, the scan keeps its bloom filters under a " +
        s"limit, a top-N or a write (AQE $adaptive)") {
      createTables()
      createTableWith("target",
        Array(Column.create("id", LongType), Column.create("v", IntegerType)), Array.empty)
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        withoutGlobalBloomFilter("dim") {
          Seq[() => DataFrame](() => join(), () => join().limit(100),
              () => join().orderBy("id").limit(100)).foreach { query =>
            val df = query()
            checkAnswer(df, expectedRows)
            val plan = df.queryExecution.executedPlan
            assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
            assert(factReadKeysOf(query()) == Set("1", "3"))
          }
          val writes = withQueryExecutionsCaptured(spark) {
            assert(factReadKeysOf(sql("INSERT INTO testcat.ns.target SELECT f.id, f.v " +
              "FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1")) ==
              Set("1", "3"))
          }
          checkAnswer(spark.table("testcat.ns.target"), expectedRows)
          val write = writes.map(_.executedPlan)
          assert(write.flatMap(storageFilterScans).map(tableOf) == Seq("fact"), write)
        }
      }
    }

    test(s"the scan gets its bloom filters below a join on the application side " +
        s"(AQE $adaptive)") {
      // The application side is the storage-partitioned join of `fact` and `other`. Pushdown puts
      // the placeholder on `fact`, below that join. So the join gets only the rows the bloom
      // filters keep.
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          val df = joinOverJoin()
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
          assert(executedShuffles(plan).isEmpty, plan)
          // The join of `fact` and `other` is the inner join with no other one below it.
          def isInner(p: SparkPlan): Boolean = p match {
            case j: BaseJoinExec => j.joinType == Inner
            case _ => false
          }
          val joinBelow = collect(plan) {
            case j: BaseJoinExec if isInner(j) && !j.children.exists(_.exists(isInner)) => j
          }
          assert(joinBelow.size == 1, plan)
          assert(joinBelow.head.metrics("numOutputRows").value == expectedRows.size, plan)
        }
      }
    }

    test(s"the scan keeps its bloom filters below a broadcast join under a storage-partitioned " +
        s"join (AQE $adaptive)") {
      // The broadcast join with `small` sits between the scan of `fact` and the
      // `GroupPartitionsExec` that groups the scan's splits. That join keeps the scan's partitions
      // as they are. So each task still reads the splits of one key.
      createTables()
      createTableWith("small", Array(Column.create("v", IntegerType)), Array.empty)
      sql("INSERT INTO testcat.ns.small VALUES " +
        (0 until 16).map(i => s"(${i * 10})").mkString(", "))
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          def query: DataFrame = sql("SELECT /*+ BROADCAST(s) */ f.id, f.v " +
            "FROM testcat.ns.fact f JOIN testcat.ns.small s ON f.v = s.v " +
            "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1")
          val df = query
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(collect(plan) { case j: BroadcastHashJoinExec => j }.size == 1,
            s"test setup: `small` must be broadcast:\n$plan")
          assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
          assert(factReadKeysOf(query) == Set("1", "3"))
        }
      }
    }

    test(s"the per-partition bloom filter follows a renamed join key (AQE $adaptive)") {
      // The placeholder is pushed below the rename, so it checks `id` rather than `fid`.
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          def query: DataFrame = sql("SELECT f.fid, f.v FROM (SELECT id AS fid, v " +
            "FROM testcat.ns.fact) f JOIN testcat.ns.dim d ON f.fid = d.id WHERE d.flag = 1")
          val df = query
          val keys = perPartitionFilters(df).map(_.key)
          assert(keys.map(_.references.map(_.name).toSet) == Seq(Set("id")),
            df.queryExecution.optimizedPlan)
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
          assert(factReadKeysOf(query) == Set("1", "3"))
        }
      }
    }

    test(s"a later join on the key still gets its bloom filter, which the scan also gets " +
        s"(AQE $adaptive)") {
      // The creation side of `dim` is too big for one bloom filter of all its keys, so its join
      // gets only the per-partition one. The join with `dim2` on the same key still gets its bloom
      // filter. It reaches the scan of `fact` as a runtime filter and a storage filter. Each is a
      // subquery that the scan node holds, so physical planning plans them.
      createTables(runtimeFiltering = true)
      createIdTable("dim2", "flag", "(1, 1), (3, 1)")
      def query: DataFrame = sql("SELECT f.id, f.v FROM testcat.ns.fact f " +
        "JOIN testcat.ns.dim d ON f.id = d.id JOIN testcat.ns.dim2 d2 ON f.id = d2.id " +
        "WHERE d.flag = 1 AND d2.flag = 1")
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          val df = query
          val optimized = df.queryExecution.optimizedPlan
          assert(perPartitionFilters(df).size == 1, optimized)
          assert(hasGlobalBloomFilter(optimized), optimized)
          checkAnswer(df, Seq(Row(1, 10), Row(3, 30)))
          val plan = df.queryExecution.executedPlan
          val scans = storageFilterScans(plan)
          assert(scans.map(tableOf) == Seq("fact"), plan)
          assert(scans.head.runtimeFilters.exists(_.exists(_.isInstanceOf[ExecScalarSubquery])) &&
            scans.head.storageFilters.exists(_.exists(_.isInstanceOf[ExecScalarSubquery])), plan)
          assert(df.queryExecution.explainString(FormattedMode)
            .contains("might_contain(bloomFilter"))
          // Each split of bucket 1 gets a filter that keeps only the key 1 of `dim` there, since
          // `dim2` has no 5 or 9.
          val (rows, reads, probed) = factReadsOf(query, probes = Seq(1L, 3L, 5L, 9L))
          assert(rows == Set(Row(1, 10), Row(3, 30)))
          assert(reads.map(_._1).toSet == Set("1", "3") && reads.map(_._3).sum >= 2, reads)
          assert(probed == Set("1" -> Set(1L), "3" -> Set(3L)), probed)
        }
      }
    }

    test(s"a GroupPartitionsExec above the scan still merges its splits in order " +
        s"(AQE $adaptive)") {
      // `fact` reports its order by `id`, and has 4 splits in each bucket. With order preserved on
      // coalescing, the join's GroupPartitionsExec merges them in order rather than sorting them.
      // The aggregate under the scan gives the GroupPartitionsExec no rows, so the aggregate does
      // not keep it from merging.
      createTableWith("fact", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
        Array(bucket(4, "id")), ordering = Array(sort(FieldReference("id"),
          SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)))
      sql("INSERT INTO testcat.ns.fact VALUES " +
        (0 until 16).map(i => s"($i, ${i * 10})").mkString(", "))
      createIdTable("dim", "flag", "(1, 1), (2, 0), (3, 1), (5, 1), (6, 0), (9, 1)")
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
        withoutGlobalBloomFilter("dim") {
          val df = join()
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(storageFilterScans(plan).map(tableOf) == Seq("fact"), plan)
          val merges = collect(plan) {
            case g: GroupPartitionsExec if storageFilterScans(g).nonEmpty => g
          }
          assert(merges.size == 1 &&
            merges.head.execute().isInstanceOf[SortedMergeCoalescedRDD[_]], plan)
        }
      }
    }

    Seq(false, true).foreach { columnar =>
      test(s"the bloom filter of a split joins the other storage filters of the scan " +
          s"(AQE $adaptive, columnar $columnar)") {
        // `fact` gets a bloom filter of all the keys of `small` on `x`, which its scan takes as a
        // storage filter, with the checked column. `dim` gives it a per-partition one on `id`.
        createTwoKeyTables(factColumnar = columnar)
        createTableWith("small", Array(Column.create("x", IntegerType),
          Column.create("flag", IntegerType)), Array.empty)
        sql("INSERT INTO testcat.ns.small VALUES (0, 1)")
        def query: DataFrame = sql("SELECT f.id, f.v FROM testcat.ns.fact f " +
          "JOIN (SELECT id, flag FROM testcat.ns.dim) d ON f.id = d.id " +
          "JOIN testcat.ns.small s ON f.x = s.x WHERE d.flag = 1 AND s.flag = 1")
        withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          withoutGlobalBloomFilter("dim") {
            val df = query
            checkAnswer(df, Seq(Row(3, 30), Row(9, 90)))
            val plan = df.queryExecution.executedPlan
            val scans = storageFilterScans(plan)
            assert(scans.map(tableOf) == Seq("fact") && scans.head.storageFilters.nonEmpty,
              s"test setup: the scan must also get the bloom filter of `small`:\n$plan")
            assert(scans.head.appendsCheckedColumn, plan)
            assert(scans.head.supportsColumnar == columnar, s"test setup:\n$plan")
            val (rows, reads, _) = factReadsOf(query)
            assert(rows == Set(Row(3, 30), Row(9, 90)))
            assert(reads.map(_._1).toSet == Set("1", "3"), reads)
            assert(reads.map(_._3).sum == 2, s"the readers keep only ids 3 and 9: $reads")
          }
        }
      }
    }
  }

  test("a plan optimized with per-partition bloom filters on keeps them when planned with " +
      "them off") {
    createTables()
    withoutGlobalBloomFilter("dim") {
      val df = join()
      assert(perPartitionFilters(df).size == 1, "test setup: the rule added it")
      withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
        val (rows, reads, _) = factReadsOf(df)
        assert(rows == expectedRows.toSet)
        assert(storageFilterScans(df.queryExecution.executedPlan).size == 1,
          df.queryExecution.executedPlan)
        assert(reads.map(_._1).toSet == Set("1", "3"), reads)
      }
    }
  }

  test("a plan optimized with per-partition bloom filters on, and planned with them off, reads " +
      "without them under a join that shuffles a key's several splits") {
    // `SizePerPartitionStorageFilters` judges the scan whatever the conf says when it plans.
    createTables()
    withSQLConf(V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false") {
      withoutGlobalBloomFilter("dim") {
        val df = join()
        assert(perPartitionFilters(df).size == 1, "test setup: the rule added it")
        withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedShuffles(plan).nonEmpty, s"test setup: the join must shuffle:\n$plan")
          assert(storageFilterScans(plan).isEmpty, plan)
        }
      }
    }
  }

  Seq(V2_STORAGE_FILTER_PUSHDOWN_ENABLED, V2_BUCKETING_ENABLED).foreach { entry =>
    test(s"a plan optimized with ${entry.key} on gets no per-partition bloom filters when " +
        "planned with it off") {
      // Planning checks again that the scan takes a storage filter on the key, and that the key has
      // its partition transform.
      createTables()
      withoutGlobalBloomFilter("dim") {
        val df = join()
        assert(perPartitionFilters(df).size == 1, "test setup: the rule added it")
        withSQLConf(entry.key -> "false") {
          checkAnswer(df, expectedRows)
          assert(storageFilterScans(df.queryExecution.executedPlan).isEmpty,
            df.queryExecution.executedPlan)
        }
      }
    }
  }

  // The expected numbers of items and the numbers of bits of the per-partition bloom filters that
  // `df` builds.
  private def executedSizes(df: DataFrame): Seq[(Long, Long)] = {
    df.collect()
    def value(e: Expression): Long = e.eval().asInstanceOf[Number].longValue
    storageFilterScans(df.queryExecution.executedPlan)
      .flatMap(_.bloomFilterAggregate.toSeq.flatMap(_.collect { case a: BaseAggregateExec => a }))
      .flatMap(_.aggregateExpressions.flatMap(_.collect { case b: BloomFilterAggregate => b }))
      .map(b => (value(b.estimatedNumItemsExpression), value(b.numBitsExpression)))
      .distinct
  }

  test("each per-partition bloom filter gets an even share of the items and bits over the " +
      "creation side's partition keys") {
    // Without CBO the Filter on the creation side drops its row count. So the creation side, twice
    // the size threshold, gets twice the default expected number of items and number of bits. `dim`
    // has 3 partition keys, fewer than its 4 buckets.
    createTables()
    withSQLConf(
        RUNTIME_BLOOM_FILTER_EXPECTED_NUM_ITEMS.key -> "300",
        RUNTIME_BLOOM_FILTER_NUM_BITS.key -> "2400") {
      withoutGlobalBloomFilter("dim") {
        assert(executedSizes(join()) == Seq((200L, 1600L)),
          "twice the 300 expected items and the 2400 bits over 3 keys")
      }
    }
  }

  test("an even share of the expected items does not overflow") {
    // The expected number of items of the whole creation side is capped at `Long.MaxValue`.
    createTables()
    withSQLConf(RUNTIME_BLOOM_FILTER_EXPECTED_NUM_ITEMS.key -> Long.MaxValue.toString) {
      withoutGlobalBloomFilter("dim") {
        assert(executedSizes(join()).map(_._1) == Seq(Math.ceilDiv(Long.MaxValue, 3L)))
      }
    }
  }

  test("the scan reads without the bloom filters when the creation side has too few partition " +
      "keys for the size threshold") {
    // The creation side is 4 times the threshold, but `dim` has only 3 partition keys. So a share
    // of one partition key would be over the threshold.
    createTables()
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
        (creationSideSize("dim") / 4).toString) {
      val df = join()
      assert(perPartitionFilters(df).size == 1, "test setup: the rule added it")
      checkAnswer(df, expectedRows)
      val plan = df.queryExecution.executedPlan
      assert(storageFilterScans(plan).isEmpty, plan)
      assert(!collectWithSubqueries(plan) { case p => p.expressions }.flatten
        .exists(_.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)), plan)
    }
  }

  Seq(true, false).foreach { adaptive =>
    test(s"a scan that reads without its bloom filters leaves a broadcast that a DPP filter can " +
        s"reuse (AQE $adaptive)") {
      // `fact` can get a DPP filter on the source column of its bucket transform. The broadcast
      // side joins `dim` and `dim2`, which gives `dim` a per-partition bloom filter. `dim2` has 1
      // partition key, under the 2 or more that the threshold needs, so the scan of `dim` reads
      // without it. The DPP filter of `fact` then reuses the broadcast, as without per-partition
      // bloom filters. That holds also with a projection above `dim` that computes a column or
      // renames one.
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        createIdTable("fact", "v", (0 until 64).map(i => s"($i, $i)").mkString(", "),
          runtimeFiltering = true)
        createIdTable("dim", "flag", (0 until 32).map(i => s"($i, ${i % 2})").mkString(", "))
        // All with the partition key 1.
        createIdTable("dim2", "flag", (1 until 64 by 4).map(i => s"($i, 1)").mkString(", "))
        withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
            (creationSideSize("dim2") / 2).toString) {
          val odd = 1 until 32 by 4
          Seq(
            "SELECT /*+ BROADCAST(d) */ f.id, f.v FROM testcat.ns.fact f JOIN (SELECT a.id " +
              "FROM testcat.ns.dim a JOIN testcat.ns.dim2 b ON a.id = b.id " +
              "WHERE a.flag = 1 AND b.flag = 1) d ON f.id = d.id" -> odd.map(i => Row(i, i)),
            "SELECT /*+ BROADCAST(d) */ f.id, f.v, d.id2 FROM testcat.ns.fact f JOIN " +
              "(SELECT a.id, a.id2 FROM (SELECT id, id * 2 AS id2 FROM testcat.ns.dim " +
              "WHERE flag = 1) a JOIN testcat.ns.dim2 b ON a.id = b.id WHERE b.flag = 1) d " +
              "ON f.id = d.id" -> odd.map(i => Row(i, i, i * 2)),
            "SELECT /*+ BROADCAST(d) */ f.id, f.v FROM testcat.ns.fact f JOIN (SELECT a.k " +
              "FROM (SELECT id AS k FROM testcat.ns.dim WHERE flag = 1) a " +
              "JOIN testcat.ns.dim2 b ON a.k = b.id WHERE b.flag = 1) d " +
              "ON f.id = d.k" -> odd.map(i => Row(i, i))
          ).foreach { case (query, expected) =>
            val df = sql(query)
            assert(perPartitionFilters(df).size == 1, s"test setup: the rule added it: $query")
            checkAnswer(df, expected)
            val plan = df.queryExecution.executedPlan
            assert(storageFilterScans(plan).isEmpty, plan)
            val factFilters = executedScans(plan, "fact").flatMap(_.runtimeFilters)
            assert(factFilters.exists {
              case DynamicPruningExpression(Literal.TrueLiteral) => false
              case _: DynamicPruningExpression => true
              case _ => false
            }, s"the DPP filter of `fact` must reuse the broadcast:\n$plan")
          }
        }
      }
    }
  }

  test("for a transform other than bucket, the bloom filters are sized over the partition keys " +
      "too") {
    // The 6 rows of `dim` are over 2 days, so its scan reports 2 partition keys.
    catalog.createFunction(Identifier.of(Array.empty, "days"),
      new UnboundPhysicalDaysFunction(LongDaysFunction))
    createTableWith("fact",
      Array(Column.create("ts", TimestampType), Column.create("v", IntegerType)), Array(days("ts")))
    sql("INSERT INTO testcat.ns.fact VALUES (TIMESTAMP '2026-01-01 01:00:00', 1), " +
      "(TIMESTAMP '2026-01-01 02:00:00', 2), (TIMESTAMP '2026-01-02 01:00:00', 3), " +
      "(TIMESTAMP '2026-01-03 01:00:00', 4)")
    createTableWith("dim", Array(Column.create("ts", TimestampType),
      Column.create("flag", IntegerType)), Array(days("ts")))
    sql("INSERT INTO testcat.ns.dim VALUES (TIMESTAMP '2026-01-01 01:00:00', 1), " +
      "(TIMESTAMP '2026-01-01 02:00:00', 1), (TIMESTAMP '2026-01-01 03:00:00', 0), " +
      "(TIMESTAMP '2026-01-02 01:00:00', 1), (TIMESTAMP '2026-01-02 02:00:00', 0), " +
      "(TIMESTAMP '2026-01-02 03:00:00', 1)")
    def daysJoin(): DataFrame = sql("SELECT f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
      "ON f.ts = d.ts WHERE d.flag = 1")
    // Twice the threshold, over 2 partition keys.
    withSQLConf(RUNTIME_BLOOM_FILTER_EXPECTED_NUM_ITEMS.key -> "600") {
      withoutGlobalBloomFilter("dim", "ts") {
        assert(executedSizes(daysJoin()).map(_._1) == Seq(600L),
          "twice the 600 expected items over 2 keys")
        checkAnswer(daysJoin(), Seq(Row(1), Row(2), Row(3)))
      }
    }
    // Three times the threshold, over 2 partition keys.
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
        (creationSideSize("dim", "ts") / 3).toString) {
      val df = daysJoin()
      assert(perPartitionFilters(df).size == 1, "test setup: the rule added it")
      checkAnswer(df, Seq(Row(1), Row(2), Row(3)))
      assert(storageFilterScans(df.queryExecution.executedPlan).isEmpty,
        df.queryExecution.executedPlan)
    }
  }

  test("no placeholder for a creation side that needs more partition keys than its buckets give") {
    // At this threshold `dim1` needs at least 8 partition keys, more than its 4 buckets and null
    // give. Its bloom filters would always be dropped, and its placeholder would keep the join with
    // `dim2` on the same key from getting one.
    createTables()
    createIdTable("dim1", "flag", (0 until 64).map(i => s"($i, 1)").mkString(", "))
    createIdTable("dim2", "flag", (0 until 16).map(i => s"($i, 1)").mkString(", "))
    val threshold = creationSideSize("dim1") / 8
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> threshold.toString) {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim1 d1 " +
        "ON f.id = d1.id JOIN testcat.ns.dim2 d2 ON f.id = d2.id WHERE d1.flag = 1 AND " +
        "d2.flag = 1")
      checkAnswer(df, (0 until 16).map(i => Row(i, i * 10)))
      val plan = df.queryExecution.executedPlan
      val scans = storageFilterScans(plan)
      assert(scans.size == 1 && executedScans(scans.head.bloomFilterAggregate.get, "dim2").nonEmpty,
        s"`fact` must get the bloom filters of `dim2`:\n$plan")
    }
  }

  test("no per-partition bloom filter on a pair of join keys whose other side gets a bloom " +
      "filter of the whole creation side") {
    // `rt` is too big for one bloom filter of all its keys, so `l` gets no such bloom filter. `rt`
    // gets the bloom filter of `l`. A per-partition one on `l` would read `rt` again, where that
    // bloom filter reads `l` again, so `l` gets none.
    createIdTable("l", "v", (0 until 8).map(i => s"($i, ${i * 10})").mkString(", "))
    createIdTable("rt", "flag", (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
    withoutGlobalBloomFilter("rt") {
      val df = sql("SELECT l.id, l.v FROM testcat.ns.l l JOIN testcat.ns.rt rt ON l.id = rt.id " +
        "WHERE l.v < 30 AND rt.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionFilters(df).isEmpty, plan)
      assert(hasGlobalBloomFilter(plan), plan)
      checkAnswer(df, Seq(Row(1, 10)))
    }
  }

  test("the per-partition bloom filter goes below a join on the application side") {
    // Pushdown moves the placeholder below the join of `fact` and `other`.
    createTables()
    withoutGlobalBloomFilter("dim") {
      val df = joinOverJoin()
      val plan = df.queryExecution.optimizedPlan
      // The join of `fact` and `other` is the inner join that has no other one below it.
      val joinBelow = lowestInnerJoins(plan)
      assert(joinBelow.size == 1 && perPartitionFilters(df).size == 1 &&
        placeholdersIn(joinBelow.head.left).size == 1,
        s"the placeholder must sit below the join of `fact` and `other`:\n$plan")
      checkAnswer(df, expectedRows)
    }
  }

  test("no per-partition bloom filter below a union") {
    // Pushdown puts the placeholder into both branches, so it gives way to `true`.
    createTables()
    createIdTable("fact2", "v", "(1, 11), (2, 21), (3, 31)")
    withoutGlobalBloomFilter("dim") {
      val df = sql("SELECT f.id, f.v FROM (SELECT id, v FROM testcat.ns.fact UNION ALL " +
        "SELECT id, v FROM testcat.ns.fact2) f JOIN testcat.ns.dim d ON f.id = d.id " +
        "WHERE d.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(plan.collect { case union: Union => union }.size == 1, plan)
      assert(replacedPlaceholders(df), s"test setup: the rule must have had placeholders:\n$plan")
      assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY), plan)
      checkAnswer(df, expectedRows ++ Seq(Row(1, 11), Row(3, 31)))
    }
  }

  test("a later join's creation side leaves out the per-partition bloom filters of the joins " +
      "below it") {
    // `dim2` is too big for a bloom filter of all its keys, so the join of `dim` and `dim2` gives
    // `dim` a placeholder. The join with `fact` takes the rows of `dim` as its creation side,
    // without that placeholder. A copy there would be a second one, and `dim` would get no
    // per-partition bloom filter.
    createTwoKeyTables()
    createIdTable("dim2", "flag", "(1, 1), (3, 1), (5, 0)")
    withoutGlobalBloomFilter("dim2") {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN (SELECT dim.id FROM " +
        "testcat.ns.dim JOIN testcat.ns.dim2 ON dim.id = dim2.id WHERE dim.flag = 1 AND " +
        "dim2.flag = 1) d ON f.id = d.id")
      val plan = df.queryExecution.optimizedPlan
      // The join of `dim` and `dim2` is the inner join that has no other one below it.
      val joinBelow = lowestInnerJoins(plan)
      assert(joinBelow.size == 1 && placeholdersIn(joinBelow.head.left).size == 1,
        s"`dim` must keep its placeholder:\n$plan")
      assert(perPartitionFilters(df).forall(!_.plan.containsPattern(
        PER_PARTITION_RUNTIME_FILTER_SUBQUERY)),
        s"no creation side may hold a copy of the placeholder of `dim`:\n$plan")
      checkAnswer(df, Seq(Row(1, 10), Row(3, 30)))
    }
  }

  test("a key with a DPP filter gets its per-partition bloom filter from a creation side that " +
      "held a placeholder") {
    // `dim` and `dim2` are too big for a bloom filter of all their keys, so the join of `dim` and
    // `dim2` gives `dim` a placeholder. The join with `fact` takes the rows of `dim` as its
    // creation side. Its key has a DPP filter, so the per-partition bloom filter needs a creation
    // side it can evaluate again. One that holds a placeholder is not. The creation side leaves
    // the placeholder out.
    createTables(runtimeFiltering = true)
    createIdTable("dim2", "flag", (0 until 20).map(i => s"($i, ${i % 2})").mkString(", "),
      runtimeFiltering = true)
    assert(creationSideSize("dim") < creationSideSize("dim2"),
      "test setup: `dim2` must be the bigger one")
    withSQLConf(
        RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
          ((creationSideSize("dim") + 1) / 2).toString) {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN (SELECT dim.id " +
        "FROM testcat.ns.dim JOIN testcat.ns.dim2 ON dim.id = dim2.id " +
        "WHERE dim.flag = 1 AND dim2.flag = 1) d ON f.id = d.id")
      val plan = df.queryExecution.optimizedPlan
      // The filters right above the scan of `fact`.
      val factFilters = plan.collect {
        case Filter(condition, scan: DataSourceV2ScanRelation)
            if scan.relation.table.name().endsWith("fact") => condition
      }
      assert(factFilters.exists(_.exists(_.isInstanceOf[DynamicPruningSubquery])),
        s"test setup: the key must have a DPP filter:\n$plan")
      // The placeholders of `dim` and of `fact`.
      assert(perPartitionFilters(df).size == 2, s"`fact` must keep its placeholder:\n$plan")
      checkAnswer(df, expectedRows)
    }
  }

  Seq(false, true).foreach { rename =>
    test(s"an IN subquery that becomes a join gets no second per-partition bloom filter on a key " +
        s"(rename $rename)") {
      // The subquery is optimized on its own first, which adds its placeholders. Then it becomes a
      // join, and the optimizer runs on it again. That run adds no second placeholder on a key.
      // `rt` is too big for a bloom filter of all its keys, and `l` has no filter to give `rt` one.
      // With the rename, the placeholder of the first run checks `id`, below the rename, and the
      // second run sees `lid`.
      createIdTable("l", "v", (0 until 8).map(i => s"($i, ${i * 10})").mkString(", "))
      createIdTable("rt", "flag", (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
      createTableWith("t", Array(Column.create("k", LongType)), Array.empty)
      sql("INSERT INTO testcat.ns.t VALUES " + (0 until 20).map(i => s"($i)").mkString(", "))
      withoutGlobalBloomFilter("rt") {
        val subquery = if (rename) {
          "SELECT l.lid FROM (SELECT id AS lid, v FROM testcat.ns.l) l JOIN testcat.ns.rt rt " +
            "ON l.lid = rt.id WHERE rt.flag = 1"
        } else {
          "SELECT l.id FROM testcat.ns.l l JOIN testcat.ns.rt rt ON l.id = rt.id WHERE rt.flag = 1"
        }
        val df = sql(s"SELECT * FROM testcat.ns.t WHERE t.k IN ($subquery)")
        val plan = df.queryExecution.optimizedPlan
        val filters = perPartitionFilters(df)
        assert(filters.nonEmpty && filters.map(_.key).distinct.size == filters.size, plan)
        checkAnswer(df, Seq(Row(1), Row(3), Row(5), Row(7)))
      }
    }
  }

  // Moves a kept per-partition placeholder away from its scan, above a projection of a random
  // column.
  private object PlaceholderAwayFromScan extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
      case Filter(condition, scan: DataSourceV2ScanRelation)
          if condition.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY) =>
        Project(scan.output, Filter(condition,
          Project(scan.output :+ Alias(Rand(Literal(0L)), "r")(), scan)))
    }
  }

  Seq(true, false).foreach { adaptive =>
    test(s"a placeholder that a later optimizer rule moves away from its scan gives way to true " +
        s"(AQE $adaptive)") {
      // A rule a user adds runs after `CleanupPerPartitionRuntimeFilters`. This one puts a
      // projection of a random column between the placeholder's filter and the scan, which the V2
      // scan strategy does not look through. So the filter is planned with the placeholder in it.
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          val extraOptimizations = spark.experimental.extraOptimizations
          spark.experimental.extraOptimizations = Seq(PlaceholderAwayFromScan)
          try {
            val df = join()
            assert(perPartitionFilters(df).size == 1, df.queryExecution.optimizedPlan)
            checkAnswer(df, expectedRows)
            val plan = df.queryExecution.executedPlan
            assert(storageFilterScans(plan).isEmpty, plan)
            assert(!collectWithSubqueries(plan) { case p => p.expressions }.flatten
              .exists(_.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)), plan)
          } finally {
            spark.experimental.extraOptimizations = extraOptimizations
          }
        }
      }
    }
  }

  test("a placeholder that MergeSubplans moves out of its filter gives way to true") {
    // With filter propagation through joins, `MergeSubplans` merges the two aggregates. It moves
    // the filters of `fact` that hold the placeholders into a projection and an `Or`, where no
    // scan can take them.
    createTables()
    withSQLConf(
        MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true",
        MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      withoutGlobalBloomFilter("dim") {
        val df = sql("SELECT * FROM (SELECT sum(f.v) c FROM testcat.ns.fact f " +
          "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1) a, (SELECT max(f.v) s " +
          "FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 0) b")
        val plan = df.queryExecution.optimizedPlan
        assert(replacedPlaceholders(df), s"test setup: the rule must have had placeholders:\n$plan")
        assert(perPartitionFiltersWithSubqueries(df).isEmpty, plan)
        assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY), plan)
        checkAnswer(df, Seq(Row(180, 60)))
      }
    }
  }

  test("a subplan that holds a placeholder merges with one that holds none on the same level") {
    // `MergeSubplans` puts a subplan above the level of the subqueries it reads. The plan of the
    // placeholder of `a` reads one, but is not merged itself. So `a` is on the level of `b`, which
    // gets no placeholder, since the number threshold allows only one. With filter propagation
    // through joins and symmetric filter propagation, the two merge. The placeholder then gives way
    // to `true`, and `fact` is read once.
    createTables()
    createTableWith("t", Array(Column.create("flag", IntegerType)), Array.empty)
    sql("INSERT INTO testcat.ns.t VALUES (1), (0)")
    withSQLConf(
        MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true",
        MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
        RUNTIME_FILTER_NUMBER_THRESHOLD.key -> "1") {
      withoutGlobalBloomFilter("dim") {
        val df = sql("SELECT * FROM (SELECT sum(f.v) s FROM testcat.ns.fact f " +
          "JOIN testcat.ns.dim d ON f.id = d.id " +
          "WHERE d.flag = (SELECT max(flag) FROM testcat.ns.t)) a, (SELECT max(f.v) m " +
          "FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id " +
          "WHERE d.flag = (SELECT max(flag) FROM testcat.ns.t)) b")
        assert(replacedPlaceholders(df),
          s"test setup: the rule must have had placeholders:\n${df.queryExecution.optimizedPlan}")
        checkAnswer(df, Seq(Row(180, 90)))
        val plan = df.queryExecution.executedPlan
        assert(executedScans(plan, "fact").size == 1, plan)
      }
    }
  }

  Seq(true, false).foreach { adaptive =>
    Seq(false, true).foreach { columnar =>
      test(s"the bloom filter of a split holds the keys whose partition transform is null " +
          s"(AQE $adaptive, columnar $columnar)") {
        // This `bucket` returns null for the key 5, which is not null. The aggregate puts the key
        // in a group of its own, in the partition of `dim` that holds it. The split of `fact` with
        // the same partition key gets that group's bloom filter too, so the join still matches its
        // row. Over a columnar scan, the two groups of that partition come in one batch.
        createBucketFunction() { input =>
          if (input.getLong(1) == 5L) null else BucketFunction.produceResult(input)
        }
        createTables(factColumnar = columnar)
        withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          withoutGlobalBloomFilter("dim") {
            val df = join()
            assert(perPartitionFilters(df).size == 1, df.queryExecution.optimizedPlan)
            checkAnswer(df, expectedRows)
            val scans = storageFilterScans(df.queryExecution.executedPlan)
            assert(scans.size == 1 && scans.head.supportsColumnar == columnar,
              df.queryExecution.executedPlan)
            val (_, _, probed) = factReadsOf(join(), probes = Seq(1L, 3L, 5L, 9L))
            assert(probed == Set("1" -> Set(1L, 5L, 9L), "3" -> Set(3L)), probed)
          }
        }
      }
    }

    test(s"a per-partition bloom filter over a creation side whose scalar subqueries " +
        s"MergeSubplans merges (AQE $adaptive)") {
      // The aggregate of the per-partition bloom filter reads the creation side in the plan of its
      // placeholder. `MergeSubplans` merges the two subqueries there as it merges them in the
      // plan. So `t` is read once.
      createTwoKeyTables()
      createTableWith("t", Array(Column.create("flag", IntegerType)), Array.empty)
      sql("INSERT INTO testcat.ns.t VALUES (1), (0)")
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          def query: DataFrame = sql("SELECT f.id, f.v FROM testcat.ns.fact f " +
            "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = (SELECT max(flag) " +
            "FROM testcat.ns.t) AND d.x >= (SELECT min(flag) FROM testcat.ns.t)")
          val df = query
          val plan = df.queryExecution.optimizedPlan
          assert(plan.exists(_.expressions.exists(_.exists {
            case GetStructField(_: ScalarSubquery, _, _) => true
            case _ => false
          })), s"test setup: the two subqueries must be merged:\n$plan")
          assert(perPartitionFilters(df).size == 1, plan)
          checkAnswer(df, expectedRows)
          val executedPlan = df.queryExecution.executedPlan
          assert(storageFilterScans(executedPlan).size == 1, executedPlan)
          assert(executedScans(executedPlan, "t").size == 1, executedPlan)
          assert(factReadKeysOf(query) == Set("1", "3"))
        }
      }
    }
  }

  test("a placeholder takes no place under the number threshold of runtime filters") {
    // The join with `dim` comes first. `dim` is too big for a bloom filter of all its keys, so the
    // join gets only a placeholder. With a threshold of one runtime filter, the join with `s` still
    // gets its bloom filter.
    createTables()
    createTableWith("s", Array(Column.create("v", IntegerType), Column.create("flag", IntegerType)),
      Array.empty)
    sql("INSERT INTO testcat.ns.s VALUES (10, 1)")
    withSQLConf(RUNTIME_FILTER_NUMBER_THRESHOLD.key -> "1") {
      withoutGlobalBloomFilter("dim") {
        val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
          "ON f.id = d.id JOIN testcat.ns.s s ON f.v = s.v WHERE d.flag = 1 AND s.flag = 1")
        val plan = df.queryExecution.optimizedPlan
        assert(perPartitionFilters(df).size == 1, plan)
        assert(plan.exists {
          case Filter(condition, _) => condition.exists {
            case b: BloomFilterMightContain => isGlobalCheck(b) &&
              b.valueExpression.references.exists(_.name == "v")
            case _ => false
          }
          case _ => false
        }, s"`fact` must get the bloom filter of `s`:\n$plan")
        checkAnswer(df, Seq(Row(1, 10)))
      }
    }
  }

  test("per-partition bloom filters have a number threshold of their own") {
    // `dim` and `dim2` are too big for a bloom filter of all their keys, so each of the two joins
    // below could get only a placeholder. With a threshold of one, only the first one gets it.
    createTables()
    createIdTable("fact2", "v", "(1, 10), (3, 30)")
    createIdTable("dim2", "flag", "(1, 1), (3, 1), (5, 0)")
    val size = creationSideSize("dim").min(creationSideSize("dim2"))
    withSQLConf(
        RUNTIME_FILTER_NUMBER_THRESHOLD.key -> "1",
        RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT a.id, b.id FROM (SELECT f.id, f.v FROM testcat.ns.fact f " +
        "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1) a JOIN (SELECT f2.id, f2.v " +
        "FROM testcat.ns.fact2 f2 JOIN testcat.ns.dim2 d2 ON f2.id = d2.id WHERE d2.flag = 1) b " +
        "ON a.v = b.v")
      assert(perPartitionFilters(df).size == 1, df.queryExecution.optimizedPlan)
      checkAnswer(df, Seq(Row(1, 1), Row(3, 3)))
    }
  }

  test("MergeSubplans merges two subplans that hold a placeholder each") {
    // The two aggregates read the same join. `dim` is too big for a bloom filter of all its keys,
    // so each join gets a placeholder. The two placeholders are the same, so the subplans merge.
    // `fact` is read once, as without the placeholders.
    createTables()
    withoutGlobalBloomFilter("dim") {
      val df = sql("SELECT * FROM (SELECT sum(f.v) s FROM testcat.ns.fact f " +
        "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1) a, (SELECT max(f.v) m " +
        "FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1) b")
      // The merged subplan is inlined into each subquery that reads it.
      assert(perPartitionFiltersWithSubqueries(df).map(_.canonicalized).distinct.size == 1,
        df.queryExecution.optimizedPlan)
      checkAnswer(df, Seq(Row(180, 90)))
      val plan = df.queryExecution.executedPlan
      assert(executedScans(plan, "fact").size == 1, plan)
    }
  }

  test("the rule that cleans up the placeholders cannot be excluded") {
    // A placeholder cannot be evaluated.
    createTables()
    withSQLConf(OPTIMIZER_EXCLUDED_RULES.key -> CleanupPerPartitionRuntimeFilters.ruleName) {
      withoutGlobalBloomFilter("dim") {
        val df = join()
        assert(perPartitionFilters(df).size == 1, df.queryExecution.optimizedPlan)
        assert(df.queryExecution.tracker.rules.contains(CleanupPerPartitionRuntimeFilters.ruleName),
          "the rule must run although excluded")
        checkAnswer(df, expectedRows)
      }
    }
  }

  test("a placeholder in a subquery waits for the run on the whole query") {
    // The subquery is optimized on its own first, which adds the placeholder and leaves it there.
    createTables()
    withoutGlobalBloomFilter("dim") {
      def query: DataFrame = sql("SELECT (SELECT sum(f.v) FROM testcat.ns.fact f " +
        "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1)")
      val df = query
      assert(perPartitionFiltersWithSubqueries(df).size == 1, df.queryExecution.optimizedPlan)
      checkAnswer(df, Seq(Row(180)))
      val executedPlan = df.queryExecution.executedPlan
      assert(storageFilterScans(executedPlan).size == 1, executedPlan)
      assert(factReadKeysOf(query) == Set("1", "3"))
    }
  }

  Seq(true, false).foreach { adaptive =>
    test(s"each iteration of a recursive CTE reads the scan with its bloom filters " +
        s"(AQE $adaptive)") {
      // Each iteration analyzes and optimizes the recursion again, with the placeholder that the
      // optimization of the whole query kept.
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          def query: DataFrame = sql(
            """WITH RECURSIVE r(id, n) AS (
              |  SELECT 1L, 0
              |  UNION ALL
              |  SELECT f.id, r.n + 1 FROM r JOIN (SELECT f.id FROM testcat.ns.fact f
              |    JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1) f ON f.id = r.id + 2
              |  WHERE r.n < 3)
              |SELECT * FROM r
              |""".stripMargin)
          assert(perPartitionFilters(query).size == 1, query.queryExecution.optimizedPlan)
          val (rows, reads, _) = factReadsOf(query)
          assert(rows == Set(Row(1L, 0), Row(3L, 1), Row(5L, 2)))
          assert(reads.map(_._1).toSet == Set("1", "3"), reads)
        }
      }
    }
  }

  test("two subqueries merge their subplans that hold a placeholder each") {
    // `dim` is too big for a bloom filter of all its keys, so each join gets a placeholder. Each
    // subquery is optimized on its own first, and its placeholder waits for the run on the whole
    // query. So `MergeSubplans` merges the two subplans, and `fact` is read once, as without the
    // placeholders.
    createTables()
    withoutGlobalBloomFilter("dim") {
      val df = sql("SELECT (SELECT sum(f.v) FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
        "ON f.id = d.id WHERE d.flag = 1), (SELECT max(f.v) FROM testcat.ns.fact f " +
        "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1)")
      // The merged subplan is inlined into each subquery that reads it.
      assert(perPartitionFiltersWithSubqueries(df).map(_.canonicalized).distinct.size == 1,
        df.queryExecution.optimizedPlan)
      checkAnswer(df, Seq(Row(180, 90)))
      val executedPlan = df.queryExecution.executedPlan
      assert(executedScans(executedPlan, "fact").size == 1, executedPlan)
    }
  }

  test("no per-partition bloom filter for a non-deterministic creation side") {
    // Its aggregate evaluates the creation side again, apart from the join. With a random filter
    // there, it can miss keys the join sees, and the scan would drop rows that match them
    // (SPARK-60808). Every key of `nd` is in `nf`, so no row may lose its match. `nd` is too big
    // for a bloom filter of all its keys, so only the per-partition one could be added.
    createIdTable("nd", "flag", (0 until 200).map(i => s"($i, 1)").mkString(", "))
    createIdTable("nf", "v", (0 until 200).map(i => s"($i, $i)").mkString(", "))
    withoutGlobalBloomFilter("nd") {
      val df = sql("SELECT count(*), count_if(f.id IS NULL) FROM (SELECT id FROM testcat.ns.nd " +
        "WHERE flag = 1 AND cast(reflect('java.lang.Math', 'random') AS DOUBLE) < 0.5) d " +
        "LEFT JOIN testcat.ns.nf f ON d.id = f.id")
      assert(perPartitionFilters(df).isEmpty, df.queryExecution.optimizedPlan)
      assert(df.collect().head.getLong(1) == 0L, "a row of `d` lost its match")
    }
  }

  test("no per-partition bloom filter when it is turned off") {
    createTables()
    withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
      checkNoRuntimeFilter(join())
    }
  }

  test("no per-partition bloom filter when the scan takes no storage filters") {
    createTables()
    withSQLConf(V2_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "false") {
      checkNoRuntimeFilter(join())
    }
  }

  test("no per-partition bloom filter for a scan that took a pushed limit, or for the read of a " +
      "group-based DELETE") {
    // Nothing above the scan checks a per-partition bloom filter. So readers that drop rows ahead
    // of a pushed limit would change the rows the scan returns. A group-based DELETE counts the
    // deleted rows from the rows its read returns, so that read is offered no per-partition bloom
    // filter either. An UPDATE counts nothing from the rows its read returns, so its read is
    // offered one. No test table takes both row-level operations and storage filters, so this
    // checks the columns offered to such scans.
    withSQLConf(
        "spark.sql.catalog.rlcat" -> classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      withTable("rlcat.ns.t") {
        sql("CREATE TABLE rlcat.ns.t (id BIGINT) USING foo")
        val ident = Identifier.of(Array("ns"), "t")
        val rowLevelCatalog = spark.sessionState.catalogManager.catalog("rlcat")
          .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
        val table = rowLevelCatalog.loadTable(ident)
          .asInstanceOf[Table with SupportsRowLevelOperations]
        val scan = new Scan with SupportsStorageFilters {
          override def readSchema(): StructType = new StructType().add("id", LongType)
          override def storageFilterAttributes(): Array[NamedReference] =
            Array(FieldReference.column("id"))
        }
        def scanOf(t: Table, onlyRowWise: Boolean = true): DataSourceV2ScanRelation = {
          val relation = DataSourceV2Relation.create(t, Some(rowLevelCatalog), Some(ident))
          DataSourceV2ScanRelation(relation, scan, relation.output,
            onlyRowWisePushdowns = onlyRowWise)
        }
        // The read of `command` over `table`.
        def readOf(command: RowLevelOperation.Command): DataSourceV2ScanRelation =
          scanOf(RowLevelOperationTable(table, table.newRowLevelOperationBuilder(
            RowLevelOperationInfoImpl(command, CaseInsensitiveStringMap.empty())).build()))
        val delete = readOf(RowLevelOperation.Command.DELETE)
        val update = readOf(RowLevelOperation.Command.UPDATE)
        assert(delete.isGroupBasedRead && update.isGroupBasedRead, "test setup")
        assert(PerPartitionRuntimeFilterSubquery.storageFilterColumnsOf(scanOf(table)).nonEmpty,
          "test setup")
        assert(PerPartitionRuntimeFilterSubquery.storageFilterColumnsOf(
          scanOf(table, onlyRowWise = false)).isEmpty)
        assert(PerPartitionRuntimeFilterSubquery.storageFilterColumnsOf(delete).isEmpty)
        assert(PerPartitionRuntimeFilterSubquery.storageFilterColumnsOf(update).nonEmpty)
      }
    }
  }

  test("no per-partition bloom filter when the scan takes no storage filter on its key") {
    createTables(factDeclared = Some("v"))
    checkNoRuntimeFilter(join())
  }

  test("no per-partition bloom filter for an identity partitioning") {
    // Each partition would hold a single key, so its bloom filter would check nothing.
    createTables(factPartitioning = Array(identity("id")), dimPartitioning = Array(identity("id")))
    checkNoRuntimeFilter(join())
  }

  test("no per-partition bloom filter for different transforms") {
    createTables(dimPartitioning = Array(bucket(8, "id")))
    checkNoRuntimeFilter(join())
  }

  test("no per-partition bloom filter for a non-deterministic transform") {
    // The aggregate groups the creation side by the transform.
    createBucketFunction(deterministic = false)(BucketFunction.produceResult(_))
    createTables()
    checkNoRuntimeFilter(join())
  }

  test("a column narrower than the transform's input type is widened to it") {
    // The test catalog's `bucket` takes a BIGINT value, so over an INT column its call casts the
    // value.
    createTables(idType = IntegerType)
    withoutGlobalBloomFilter("dim") {
      val df = join()
      assert(perPartitionFilters(df).size == 1, df.queryExecution.optimizedPlan)
      checkAnswer(df, expectedRows)
      assert(storageFilterScans(df.queryExecution.executedPlan).size == 1,
        df.queryExecution.executedPlan)
      assert(factReadKeysOf(join()) == Set("1", "3"))
    }
  }

  test("the partition key of the aggregate stays in whole-stage codegen when the function of " +
      "the transform has an invoke method") {
    // This `bucket` takes an INT value with a magic `invoke`, as Apache Iceberg's does for an INT
    // column. So the projection of the aggregate's partition key generates the code of its call.
    catalog.clearFunctions()
    catalog.createFunction(Identifier.of(Array.empty, "bucket"), UnboundIntBucketFunction)
    createTables(idType = IntegerType)
    withoutGlobalBloomFilter("dim") {
      val df = join()
      assert(perPartitionFilters(df).size == 1, df.queryExecution.optimizedPlan)
      checkAnswer(df, expectedRows)
      val plan = df.queryExecution.executedPlan
      val partitionKeys = codegenStageNodes(plan)
        .collect { case p: ProjectExec => p }
        .filter(_.projectList.exists(_.exists(_.isInstanceOf[TransformExpression])))
      assert(partitionKeys.size == 1, plan)
    }
  }

  test("no per-partition bloom filter when the column is wider than the transform's input type") {
    // This `bucket` takes an INT value, which the BIGINT column would have to be narrowed to.
    catalog.clearFunctions()
    catalog.createFunction(Identifier.of(Array.empty, "bucket"), UnboundIntBucketFunction)
    createTables()
    checkNoRuntimeFilter(join())
  }
}
