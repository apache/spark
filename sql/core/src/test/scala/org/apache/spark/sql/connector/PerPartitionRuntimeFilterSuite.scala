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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, DynamicPruningSubquery, Expression, GetStructField, PlanExpression, PredicateHelper, ScalarSubquery, TransformExpression}
import org.apache.spark.sql.catalyst.optimizer.{InjectRuntimeFilter, InsertPerPartitionRuntimeFilterJoins, PerPartitionRuntimeFilterPlaceholder}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.plans.physical.REPLICATED_FOR_JOIN
import org.apache.spark.sql.catalyst.trees.TreePattern.PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryBaseTable, InMemoryCatalog, InMemoryTable, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, BucketFunction, ScalarFunction, UnboundBucketFunction, UnboundFunction}
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.{NamedReference, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, identity}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, ScanBuilder, StorageFilterRequest, SupportsStorageFilters}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation, GroupPartitionsExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BaseJoinExec, ShuffledHashJoinExec}
import org.apache.spark.sql.execution.planmerging.MergeSubplans
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PerPartitionRuntimeFilterSuite extends KeyGroupedPartitioningSuiteBase with PredicateHelper {

  // The in-memory scans are small, so the size threshold of the application side is lifted.
  override def sparkConf: SparkConf = super.sparkConf
    .set(V2_BUCKETING_ENABLED, true)
    .set(AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    .set(RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD, 0L)
    .set(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED, true)

  before {
    catalog.createFunction(Identifier.of(Array.empty, UnboundBucketFunction.name()),
      UnboundBucketFunction)
  }

  after {
    catalog.clearTables()
    catalog.clearFunctions()
  }

  private def createTables(
      idType: DataType = LongType,
      factPartitioning: Array[Transform] = Array(bucket(4, "id")),
      dimPartitioning: Array[Transform] = Array(bucket(4, "id")),
      numFactRows: Int = 16,
      catalog: InMemoryTableCatalog = catalog): Unit = {
    createTable("fact", Array(Column.create("id", idType), Column.create("v", IntegerType)),
      factPartitioning, catalog = catalog)
    sql(s"INSERT INTO ${catalog.name()}.ns.fact VALUES " +
      (0 until numFactRows).map(i => s"($i, ${i * 10})").mkString(", "))
    createTable("dim", Array(Column.create("id", idType), Column.create("flag", IntegerType)),
      dimPartitioning, catalog = catalog)
    sql(s"INSERT INTO ${catalog.name()}.ns.dim VALUES " +
      "(1, 1), (2, 0), (3, 1), (5, 1), (6, 0), (9, 1)")
  }

  // Replaces the catalog's `bucket` with a function that computes `produce`.
  private def createBucketFunction(
      deterministic: Boolean = true,
      types: Array[DataType] = BucketFunction.inputTypes())(
      produce: InternalRow => Integer): Unit = {
    catalog.clearFunctions()
    catalog.createFunction(Identifier.of(Array.empty, "bucket"), new UnboundFunction {
      override def bind(inputType: StructType): BoundFunction = new ScalarFunction[Integer] {
        override def inputTypes(): Array[DataType] = types
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

  // Runs `f` with `fact` and `dim` in a `PartitionSourceFilteringCatalog` named `rfcat`.
  private def withPartitionSourceFilteringTables(f: => Unit): Unit = {
    withSQLConf("spark.sql.catalog.rfcat" -> classOf[PartitionSourceFilteringCatalog].getName) {
      val rfcat = spark.sessionState.catalogManager.catalog("rfcat")
        .asInstanceOf[PartitionSourceFilteringCatalog]
      try {
        rfcat.createFunction(Identifier.of(Array.empty, UnboundBucketFunction.name()),
          UnboundBucketFunction)
        createTables(catalog = rfcat)
        f
      } finally {
        rfcat.clearTables()
        rfcat.clearFunctions()
      }
    }
  }

  private def join(catalogName: String = "testcat"): DataFrame = sql(
    s"""
      |SELECT f.id, f.v
      |FROM $catalogName.ns.fact f JOIN $catalogName.ns.dim d ON f.id = d.id
      |WHERE d.flag = 1
      |""".stripMargin)

  // The join with `dim` of the join of `fact` and `other`, on the same key.
  private def joinOverJoin(): DataFrame = {
    createTable("other", Array(Column.create("id", LongType)), Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.other VALUES " + (0 until 16).map(i => s"($i)").mkString(", "))
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

  // The size of the creation side that a join on `id` gets from `table` with `flag = 1`.
  private def creationSideSize(table: String): BigInt =
    spark.table(s"testcat.ns.$table").where("flag = 1").select("id")
      .queryExecution.optimizedPlan.stats.sizeInBytes

  // `fact` and `dim` with a second join key `x`, which the partitioning leaves out.
  private def createTwoKeyTables(): Unit = {
    createTable("fact", Array(Column.create("id", LongType), Column.create("x", IntegerType),
      Column.create("v", IntegerType)), Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.fact VALUES " +
      (0 until 16).map(i => s"($i, ${i % 3}, ${i * 10})").mkString(", "))
    createTable("dim", Array(Column.create("id", LongType), Column.create("x", IntegerType),
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

  // A join on two keys, which gets two bloom filters of the same creation side. `MergeSubplans`
  // merges them into one subquery. Only `id` can get a per-partition one.
  private def twoKeyJoin(): DataFrame = {
    createTwoKeyTables()
    sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
      "ON f.id = d.id AND f.x = d.x WHERE d.flag = 1")
  }

  // The global bloom filters `plan` checks.
  private def executedGlobalBloomFilters(plan: SparkPlan): Seq[BloomFilterMightContain] =
    executedGlobalChecks(plan).flatMap(f => splitConjunctivePredicates(f.condition)).collect {
      case b: BloomFilterMightContain if isGlobalCheck(b) => b
    }

  // The columns the bloom filters check.
  private def columnsOf(blooms: Seq[BloomFilterMightContain]): Set[String] =
    blooms.flatMap(_.valueExpression.references.map(_.name)).toSet

  // A semi join of a per-partition bloom filter.
  private val perPartitionSemiJoin: PartialFunction[LogicalPlan, Join] = {
    case j @ Join(_, _, ExistenceJoin(_), Some(condition), _)
        if InjectRuntimeFilter.perPartitionRuntimeFilter(condition).isDefined => j
  }

  private def perPartitionSemiJoins(df: DataFrame): Seq[Join] =
    df.queryExecution.optimizedPlan.collect(perPartitionSemiJoin)

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

  // `InjectRuntimeFilter` added only the bloom filter of the whole creation side.
  private def checkOnlyGlobalBloomFilter(df: DataFrame): Unit = {
    val plan = df.queryExecution.optimizedPlan
    assert(perPartitionSemiJoins(df).isEmpty, plan)
    assert(hasGlobalBloomFilter(plan), plan)
    checkAnswer(df, expectedRows)
  }

  private def executedSemiJoins(plan: SparkPlan): Seq[ShuffledHashJoinExec] =
    collect(plan) { case j: ShuffledHashJoinExec if j.joinType.isInstanceOf[ExistenceJoin] => j }

  private def executedGlobalChecks(plan: SparkPlan): Seq[FilterExec] =
    collect(plan) { case f: FilterExec if f.condition.exists(isGlobalCheck) => f }

  private def executedShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] =
    collect(plan) { case s: ShuffleExchangeExec => s }

  test("a per-partition bloom filter joins the application side next to the global bloom filter") {
    createTables()
    val df = join()
    val plan = df.queryExecution.optimizedPlan
    assert(perPartitionSemiJoins(df).size == 1, plan)
    assert(hasGlobalBloomFilter(plan), plan)
    checkAnswer(df, expectedRows)
  }

  Seq(true, false).foreach { adaptive =>
    test(s"the semi join stays and the bloom filter goes when it pairs (AQE $adaptive)") {
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = join()
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).size == 1, plan)
        assert(executedGlobalChecks(plan).isEmpty, plan)
        assert(executedShuffles(plan).isEmpty,
          "the semi join pairs the partitions as the join does")
      }
    }

    test(s"the semi join goes and the bloom filter stays when it would shuffle (AQE $adaptive)") {
      // Without pushing the partition values, the two sides' different keys cannot pair.
      createTables()
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false") {
        val df = join()
        assert(perPartitionSemiJoins(df).size == 1, "test setup: the rule added it")
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).isEmpty, plan)
        assert(executedGlobalChecks(plan).size == 1, plan)
        assert(collect(plan) { case p @ ProjectExec(_, _: ProjectExec) => p }.isEmpty,
          s"the semi join's projection must go with it:\n$plan")
      }
    }

    test(s"no semi join when MergeSubplans merges the global bloom filter of its key with others " +
        s"(AQE $adaptive)") {
      // The join runs on the partition key alone, so the semi join would be free. But the merged
      // subquery reads `dim` anyway for the bloom filter of `x`, and the semi join would read it
      // again.
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = twoKeyJoin()
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        val blooms = executedGlobalBloomFilters(plan)
        assert(blooms.exists(_.bloomFilterExpression.isInstanceOf[GetStructField]),
          s"test setup: the bloom filters must come from one merged subquery:\n$plan")
        assert(perPartitionSemiJoins(df).isEmpty, df.queryExecution.optimizedPlan)
        assert(columnsOf(blooms) == Set("id", "x"), s"both bloom filters must stay:\n$plan")
        assert(executedShuffles(plan).isEmpty, s"test setup: the join must not shuffle:\n$plan")
      }
    }

    test(s"the semi join goes when the join shuffles the application side anyway (AQE $adaptive)") {
      // The join's keys are more than the partition key, so it shuffles both sides. The semi join
      // would group the application side's splits ahead of that shuffle. Without `MergeSubplans`,
      // the bloom filter of each key keeps a subquery of its own.
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          OPTIMIZER_EXCLUDED_RULES.key -> MergeSubplans.ruleName) {
        val df = twoKeyJoin()
        assert(perPartitionSemiJoins(df).size == 1, "test setup: the rule added it")
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedShuffles(plan).nonEmpty, s"test setup: the join must shuffle:\n$plan")
        assert(executedSemiJoins(plan).isEmpty, plan)
        assert(columnsOf(executedGlobalBloomFilters(plan)) == Set("id", "x"),
          s"both bloom filters must stay:\n$plan")
      }
    }

    test(s"with partially clustered distribution, the bloom filter stays when the semi join's " +
        s"aggregate would repeat (AQE $adaptive)") {
      // The creation side's aggregate is the smaller side, so it is the one that repeats, once for
      // each split of the application side. Each repeat reads `dim` again, while the bloom filter
      // of the whole creation side reads it once.
      createTables(numFactRows = 400)
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        val df = join()
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).isEmpty, plan)
        assert(executedGlobalChecks(plan).size == 1, plan)
      }
    }

    test(s"with partially clustered distribution, the semi join keeps the application side's " +
        s"splits when it is the only check (AQE $adaptive)") {
      // `dim` is too big for a bloom filter of all its keys, so the semi join is the only check.
      createTables(numFactRows = 400)
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
          RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
            (creationSideSize("dim") / 2).toString) {
        val df = join()
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        val semiJoins = executedSemiJoins(plan)
        assert(semiJoins.size == 1, plan)
        assert(semiJoins.head.left.outputPartitioning.numPartitions > 4,
          s"the application side must keep its splits:\n$plan")
        assert(executedGlobalChecks(plan).isEmpty, plan)
      }
    }

    test(s"with partially clustered distribution, the semi join goes when the join repeats the " +
        s"side it is on (AQE $adaptive)") {
      // `fact` is the smaller side, so the join repeats it and keeps the splits of `dim`. Each
      // repeat would run the semi join again, its aggregate included. The existence join of the
      // semi join is estimated from `fact` (SPARK-61534), so the join still repeats `fact`.
      createTable("fact", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
        Array(bucket(4, "id")))
      sql("INSERT INTO testcat.ns.fact VALUES (1, 10), (2, 20), (3, 30), (5, 50)")
      createTable("dim", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
        Array(bucket(4, "id")))
      sql("INSERT INTO testcat.ns.dim VALUES " +
        (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
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
        val without = withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
          joinPartitions(checkedJoin().queryExecution.executedPlan)
        }
        assert(without.size == 1 && without.head > 4,
          s"test setup: the join must keep the splits of `dim`, got $without")
        val df = checkedJoin()
        assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).isEmpty, plan)
        assert(executedGlobalChecks(plan).size == 1, plan)
        assert(joinPartitions(plan) == without, plan)
        // Without a bloom filter of the whole creation side, the semi join goes too.
        withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
            (creationSideSize("dim") / 2).toString) {
          val df = checkedJoin()
          assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
          val plan = df.queryExecution.executedPlan
          assert(executedSemiJoins(plan).isEmpty, plan)
          assert(executedGlobalChecks(plan).isEmpty, plan)
        }
      }
    }

    test(s"with partially clustered distribution, the semi join stays when the aggregate marked " +
        s"for repeating is read once (AQE $adaptive)") {
      // `fact` has one split per bucket, so the aggregate is read once for each bucket. Partially
      // clustered distribution still marks the aggregate as the side it repeats. `a`, `b` and `c`
      // make `fact` the bigger side.
      createTable("fact", Array(Column.create("id", LongType), Column.create("v", IntegerType),
        Column.create("a", StringType), Column.create("b", StringType),
        Column.create("c", StringType)), Array(bucket(4, "id")))
      sql("INSERT INTO testcat.ns.fact VALUES " +
        (0 until 4).map(i => s"($i, ${i * 10}, 'a', 'b', 'c')").mkString(", "))
      createTable("dim", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
        Array(bucket(4, "id")))
      sql("INSERT INTO testcat.ns.dim VALUES (1, 1), (3, 1)")
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        val df = sql("SELECT f.id, f.v, f.a, f.b, f.c FROM testcat.ns.fact f " +
          "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1")
        checkAnswer(df, Seq(Row(1, 10, "a", "b", "c"), Row(3, 30, "a", "b", "c")))
        val plan = df.queryExecution.executedPlan
        val semiJoins = executedSemiJoins(plan)
        assert(semiJoins.size == 1, plan)
        assert(executedGlobalChecks(plan).isEmpty, plan)
        // Whole-stage codegen wraps the aggregate side in an `InputAdapter`.
        val aggregateSide = semiJoins.head.right.collectFirst { case g: GroupPartitionsExec => g }
        assert(aggregateSide.exists { group =>
          group.ungroupingOrigin.contains(REPLICATED_FOR_JOIN) &&
            group.grouping.numReplicatedPartitionReads == 0
        }, s"test setup: the aggregate must be marked for repeating, and read once:\n$plan")
      }
    }

    test(s"with partially clustered distribution, the semi join goes when it would group the " +
        s"splits the join keeps (AQE $adaptive)") {
      // The application side is the smaller side here, which an existence join cannot repeat. So
      // the semi join would group the splits the join itself keeps apart. A limit, a top-N or a
      // write above the join reports one partition or none, however many tasks read below it.
      createTables()
      createTable("target", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
        Array.empty)
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        Seq(join(), join().limit(100), join().orderBy("id").limit(100)).foreach { df =>
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedSemiJoins(plan).isEmpty, plan)
          assert(executedGlobalChecks(plan).size == 1, plan)
        }
        val write = withQueryExecutionsCaptured(spark) {
          sql("INSERT INTO testcat.ns.target SELECT f.id, f.v FROM testcat.ns.fact f " +
            "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1")
        }.map(_.executedPlan)
        checkAnswer(spark.table("testcat.ns.target"), expectedRows)
        assert(write.flatMap(executedSemiJoins).isEmpty, write)
        assert(write.flatMap(executedGlobalChecks).size == 1, write)
      }
    }

    test(s"the semi join follows the join key through a join below it (AQE $adaptive)") {
      // The application side is the storage-partitioned join of `fact` and `other`. Pushdown puts
      // the placeholder of the semi join on `fact`, below that join, next to the global bloom
      // filter, and the semi join is built there. So the join gets only the rows it keeps.
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = joinOverJoin()
        assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).size == 1, plan)
        assert(executedGlobalChecks(plan).isEmpty, plan)
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

    test(s"the semi join also drops the bloom filter's copy among the runtime filters " +
        s"(AQE $adaptive)") {
      // A scan that offers the column of its bucket transform for runtime filtering, as Iceberg's
      // does, gets the global bloom filter as a runtime filter too. Dynamic partition pruning puts
      // a filter on that column as well. The storage-partitioned join has no broadcast for it to
      // reuse. So physical planning drops it, and the bloom filters stay (SPARK-60702).
      withPartitionSourceFilteringTables {
        // Whether one of the runtime filters of the scan of `fact` is a bloom filter.
        def bloomRuntimeFilter(plan: SparkPlan): Boolean = collectScans(plan).exists { scan =>
          scan.table.name().endsWith("fact") &&
            scan.runtimeFilters.exists(_.exists(_.isInstanceOf[BloomFilterMightContain]))
        }
        withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
            val df = join("rfcat")
            checkAnswer(df, expectedRows)
            val plan = df.queryExecution.executedPlan
            assert(bloomRuntimeFilter(plan),
              s"test setup: the bloom filter must be a runtime filter:\n$plan")
          }
          val df = join("rfcat")
          checkAnswer(df, expectedRows)
          assert(df.queryExecution.optimizedPlan.exists {
            case Filter(condition, _) => condition.exists(_.isInstanceOf[DynamicPruningSubquery])
            case _ => false
          }, s"test setup: the key must have a DPP filter:\n${df.queryExecution.optimizedPlan}")
          val plan = df.queryExecution.executedPlan
          assert(executedSemiJoins(plan).size == 1, plan)
          assert(executedGlobalChecks(plan).isEmpty, plan)
          assert(!bloomRuntimeFilter(plan), s"the runtime filter copy must go too:\n$plan")
        }
      }
    }

    test(s"the semi join goes when the scan applies the global bloom filter (AQE $adaptive)") {
      // Without whole-stage codegen the scan gets no checked column. So the check above the scan
      // stays as it is, and the scan's readers may skip the filter.
      withPartitionSourceFilteringTables {
        withSQLConf(
            ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            V2_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
            WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          val df = join("rfcat")
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(collectScans(plan).exists(_.storageFilters.exists(isGlobalCheck)),
            s"test setup: the scan must take the global bloom filter:\n$plan")
          assert(executedSemiJoins(plan).isEmpty, plan)
          assert(executedGlobalChecks(plan).size == 1, plan)
        }
      }
    }

    test(s"the semi join replaces the bloom filter of a renamed join key (AQE $adaptive)") {
      // The bloom filter is pushed below the rename, so it checks `id` rather than `fid`.
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql("SELECT f.fid, f.v FROM (SELECT id AS fid, v FROM testcat.ns.fact) f " +
          "JOIN testcat.ns.dim d ON f.fid = d.id WHERE d.flag = 1")
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).size == 1, plan)
        assert(executedGlobalChecks(plan).isEmpty, plan)
      }
    }
  }

  test("a later join gets its bloom filter on a key whose semi join has no global one") {
    // The creation side of `dim` is too big for one bloom filter of all its keys, so its join gets
    // only the semi join. The join with `dim2` on the same key still gets its bloom filter, but no
    // second semi join.
    createTables()
    createTable("dim2", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.dim2 VALUES (1, 1), (3, 1)")
    val size = creationSideSize("dim")
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id " +
        "JOIN testcat.ns.dim2 d2 ON f.id = d2.id WHERE d.flag = 1 AND d2.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).size == 1, plan)
      assert(hasGlobalBloomFilter(plan), plan)
      checkAnswer(df, Seq(Row(1, 10), Row(3, 30)))
    }
  }

  test("the right side still gets its bloom filter when the left side gets only a semi join") {
    // The creation side of `l` is too big for one bloom filter of all its keys, so `l` gets only
    // the semi join. `rt` still gets the bloom filter of `l`, but no semi join of its own.
    createTable("l", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.l VALUES " +
      (0 until 8).map(i => s"($i, ${i * 10})").mkString(", "))
    createTable("rt", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.rt VALUES " +
      (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
    val size = creationSideSize("rt")
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT l.id, l.v FROM testcat.ns.l l JOIN testcat.ns.rt rt ON l.id = rt.id " +
        "WHERE l.v < 30 AND rt.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).size == 1, plan)
      assert(hasGlobalBloomFilter(plan), plan)
      checkAnswer(df, Seq(Row(1, 10)))
    }
  }

  test("without a bloom filter of the whole creation side, the semi join still goes below a " +
      "join on the application side") {
    // The creation side of `dim` is too big for one bloom filter of all its keys, so only the
    // placeholder of the per-partition one goes down, below the join of `fact` and `other`.
    createTables()
    val size = creationSideSize("dim")
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = joinOverJoin()
      val plan = df.queryExecution.optimizedPlan
      assert(!hasGlobalBloomFilter(plan), plan)
      // The join of `fact` and `other` is the inner join that has no other one below it.
      val joinBelow = lowestInnerJoins(plan)
      val semiJoins = perPartitionSemiJoins(df)
      assert(joinBelow.size == 1 && semiJoins.size == 1 &&
        joinBelow.head.left.exists(semiJoins.contains(_)),
        s"the semi join must sit below the join of `fact` and `other`:\n$plan")
      checkAnswer(df, expectedRows)
    }
  }

  Seq(true, false).foreach { global =>
    test(s"no per-partition bloom filter below a union (global bloom filter $global)") {
      // Pushdown puts the placeholder into both branches, so it is dropped. Each branch keeps the
      // bloom filter of the whole creation side, when `dim` is not too big for one.
      createTables()
      createTable("fact2", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
        Array(bucket(4, "id")))
      sql("INSERT INTO testcat.ns.fact2 VALUES (1, 11), (2, 21), (3, 31)")
      val threshold = if (global) Nil else Seq(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
        (creationSideSize("dim") / 2).toString)
      withSQLConf(threshold: _*) {
        val df = sql("SELECT f.id, f.v FROM (SELECT id, v FROM testcat.ns.fact UNION ALL " +
          "SELECT id, v FROM testcat.ns.fact2) f JOIN testcat.ns.dim d ON f.id = d.id " +
          "WHERE d.flag = 1")
        val plan = df.queryExecution.optimizedPlan
        assert(perPartitionSemiJoins(df).isEmpty, plan)
        val unions = plan.collect { case union: Union => union }
        assert(unions.size == 1 && unions.head.children.forall(hasGlobalBloomFilter(_) == global),
          s"each branch must keep the bloom filter of the whole creation side, if any:\n$plan")
        checkAnswer(df, expectedRows ++ Seq(Row(1, 11), Row(3, 31)))
      }
    }
  }

  Seq(false, true).foreach { twoKeys =>
    test(s"a later join's creation side leaves out the per-partition bloom filters of the joins " +
        s"below it (two keys $twoKeys)") {
      // The join of `dim` and `dim2` gives `dim` a per-partition bloom filter. The join with `fact`
      // takes that join as its creation side, but without the placeholder of `dim`'s one. So the
      // subqueries of its bloom filters and its own aggregate get no semi join that reads `dim2`
      // again. On two keys, `MergeSubplans` merges the subqueries of both bloom filters of `fact`.
      createTwoKeyTables()
      createTable("dim2", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
        Array(bucket(4, "id")))
      sql("INSERT INTO testcat.ns.dim2 VALUES (1, 1), (3, 1), (5, 0)")
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN (SELECT dim.id, dim.x FROM " +
        "testcat.ns.dim JOIN testcat.ns.dim2 ON dim.id = dim2.id WHERE dim.flag = 1 AND " +
        "dim2.flag = 1) d ON f.id = d.id" + (if (twoKeys) " AND f.x = d.x" else ""))
      val plan = df.queryExecution.optimizedPlan
      assert(!twoKeys || plan.exists(_.expressions.exists(_.exists {
        case BloomFilterMightContain(_: GetStructField, _) => true
        case _ => false
      })), s"test setup: the bloom filters must come from one merged subquery:\n$plan")
      // The join of `dim` and `dim2` is the inner join that has no other one below it.
      val joinBelow = lowestInnerJoins(plan)
      // `dim`'s bloom filter of the whole creation side is checked in the creation side of the join
      // with `fact` too. So its subquery reads `dim2` anyway, and `dim` gets no semi join.
      assert(joinBelow.size == 1 &&
        !joinBelow.head.left.exists(perPartitionSemiJoin.isDefinedAt),
        s"`dim` must get no semi join:\n$plan")
      // `fact` gets a semi join, unless `MergeSubplans` merged its global bloom filter.
      assert(perPartitionSemiJoins(df).size == (if (twoKeys) 0 else 1), plan)
      assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER),
        s"no placeholder may be left, in a subquery or an aggregate either:\n$plan")
      checkAnswer(df, Seq(Row(1, 10), Row(3, 30)))
    }
  }

  test("the aggregate of a placeholder with more than one copy checks no global bloom filter") {
    // The join of `dim` and `dim2` gives `dim` a bloom filter of `dim2` and a placeholder. The join
    // of the union takes that join as its creation side. That side is too big for a bloom filter of
    // all its keys, so the union gets only a placeholder. Pushdown copies it into both branches, so
    // it is dropped, and its aggregate never checks the bloom filter of `dim`. That bloom filter is
    // then checked once, so `dim` keeps its semi join.
    createTables()
    createTable("fact2", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.fact2 VALUES (1, 11), (2, 21), (3, 31)")
    createTable("dim2", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.dim2 VALUES (1, 1), (3, 1), (5, 0)")
    val creationSide = "SELECT dim.id FROM testcat.ns.dim JOIN testcat.ns.dim2 " +
      "ON dim.id = dim2.id WHERE dim.flag = 1 AND dim2.flag = 1"
    // The bloom filter of the union is of the rows of `dim`. The threshold is under their size,
    // and over the size of the rows of `dim2`.
    val size = creationSideSize("dim")
    val threshold = creationSideSize("dim2")
    assert(threshold < size, "test setup: the union must get no bloom filter")
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> threshold.toString) {
      val df = sql("SELECT f.id, f.v FROM (SELECT id, v FROM testcat.ns.fact UNION ALL " +
        s"SELECT id, v FROM testcat.ns.fact2) f JOIN ($creationSide) d ON f.id = d.id")
      val plan = df.queryExecution.optimizedPlan
      val unions = plan.collect { case union: Union => union }
      assert(unions.size == 1 &&
        !unions.head.children.exists(_.exists(perPartitionSemiJoin.isDefinedAt)),
        s"the branches of the union must get no semi join:\n$plan")
      assert(perPartitionSemiJoins(df).size == 1, plan)
      checkAnswer(df, Seq(Row(1, 10), Row(3, 30), Row(1, 11), Row(3, 31)))
    }
  }

  test("a key with a DPP filter gets its bloom filter from a creation side that held a " +
      "placeholder") {
    // `dim2` is too big for a bloom filter of all its keys, so the join of `dim` and `dim2` gives
    // `dim` only a placeholder. The join with `fact` takes that join as its creation side. Its key
    // has a DPP filter, so the bloom filter needs a creation side it can evaluate again. One that
    // holds a placeholder is not. The creation side leaves the placeholder out.
    withPartitionSourceFilteringTables {
      val rfcat = spark.sessionState.catalogManager.catalog("rfcat")
        .asInstanceOf[InMemoryTableCatalog]
      createTable("dim2", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
        Array(bucket(4, "id")), catalog = rfcat)
      sql("INSERT INTO rfcat.ns.dim2 VALUES " +
        (0 until 20).map(i => s"($i, ${i % 2})").mkString(", "))
      // The size of the rows of `table` that the join reads. The threshold is that of `dim`,
      // which is under that of `dim2`.
      def size(table: String): BigInt = spark.table(s"rfcat.ns.$table").where("flag = 1")
        .select("id").queryExecution.optimizedPlan.stats.sizeInBytes
      assert(size("dim") < size("dim2"), "test setup: `dim2` must be the bigger one")
      withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> size("dim").toString) {
        val df = sql("SELECT f.id, f.v FROM rfcat.ns.fact f JOIN (SELECT dim.id " +
          "FROM rfcat.ns.dim JOIN rfcat.ns.dim2 ON dim.id = dim2.id " +
          "WHERE dim.flag = 1 AND dim2.flag = 1) d ON f.id = d.id")
        val plan = df.queryExecution.optimizedPlan
        // The filters right above the scan of `fact`.
        val factFilters = plan.collect {
          case Filter(condition, scan: DataSourceV2ScanRelation)
              if scan.relation.table.name().endsWith("fact") => condition
        }
        assert(factFilters.exists(_.exists(_.isInstanceOf[DynamicPruningSubquery])),
          s"test setup: the key must have a DPP filter:\n$plan")
        assert(factFilters.exists(_.exists(_.isInstanceOf[BloomFilterMightContain])),
          s"`fact` must get a bloom filter:\n$plan")
        checkAnswer(df, expectedRows)
      }
    }
  }

  test("no semi join when another bloom filter of the whole creation side has the same plan") {
    // Without `MergeSubplans`, `fact` and `fact2` get bloom filters of the same rows of `dim`.
    // Their subqueries have the same plan and run once, and a semi join would read `dim` again.
    createTables()
    createTable("fact2", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
      Array.empty)
    sql("INSERT INTO testcat.ns.fact2 VALUES (1, 11), (2, 21), (3, 31)")
    withSQLConf(OPTIMIZER_EXCLUDED_RULES.key -> MergeSubplans.ruleName) {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
        "ON f.id = d.id WHERE d.flag = 1 UNION ALL SELECT f2.id, f2.v FROM testcat.ns.fact2 f2 " +
        "JOIN testcat.ns.dim d2 ON f2.id = d2.id WHERE d2.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      val subqueries = plan.flatMap(_.expressions.flatMap(_.collect {
        case BloomFilterMightContain(subquery: ScalarSubquery, _) => subquery.plan.canonicalized
      }))
      assert(subqueries.size == 2 && subqueries.distinct.size == 1,
        s"test setup: the two bloom filters must have subqueries of the same plan:\n$plan")
      assert(perPartitionSemiJoins(df).isEmpty, plan)
      checkAnswer(df, expectedRows ++ Seq(Row(1, 11), Row(3, 31)))
    }
  }

  Seq(false, true).foreach { rename =>
    test(s"an IN subquery that becomes a join gets no second semi join on a key (rename $rename)") {
      // The subquery is optimized on its own first, which builds its semi joins. Then it becomes a
      // join, and the optimizer runs on it again. That run adds no second semi join on a key.
      // `rt` is too big for a bloom filter of all its keys, and `l` is not. With the rename, the
      // semi join of the first run checks `id`, below the rename, and the second run sees `lid`.
      createTable("l", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
        Array(bucket(4, "id")))
      sql("INSERT INTO testcat.ns.l VALUES " +
        (0 until 8).map(i => s"($i, ${i * 10})").mkString(", "))
      createTable("rt", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
        Array(bucket(4, "id")))
      sql("INSERT INTO testcat.ns.rt VALUES " +
        (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
      createTable("t", Array(Column.create("k", LongType)), Array.empty)
      sql("INSERT INTO testcat.ns.t VALUES " + (0 until 20).map(i => s"($i)").mkString(", "))
      val size = creationSideSize("rt")
      withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
        val subquery = if (rename) {
          "SELECT l.lid FROM (SELECT id AS lid, v FROM testcat.ns.l) l JOIN testcat.ns.rt rt " +
            "ON l.lid = rt.id WHERE rt.flag = 1 AND l.v < 30"
        } else {
          "SELECT l.id FROM testcat.ns.l l JOIN testcat.ns.rt rt ON l.id = rt.id " +
            "WHERE rt.flag = 1 AND l.v < 30"
        }
        val df = sql(s"SELECT * FROM testcat.ns.t WHERE t.k IN ($subquery)")
        val plan = df.queryExecution.optimizedPlan
        val semiJoins = perPartitionSemiJoins(df)
        val keys = semiJoins.flatMap(PerPartitionRuntimeFilterPlaceholder.semiJoinKey)
        assert(semiJoins.nonEmpty && keys.distinct.size == semiJoins.size, plan)
        checkAnswer(df, Seq(Row(1)))
      }
    }
  }

  test("a semi join that an earlier optimizer pass built goes when its global bloom filter is " +
      "checked elsewhere too") {
    // The IN subquery is optimized on its own first, which gives `fact` a semi join that can
    // replace the bloom filter of `dim`. Then the subquery becomes a join, and the bloom filter
    // that `t` gets from it checks that bloom filter of `dim` too. So its subquery reads `dim`
    // anyway.
    createTables()
    createTable("t", Array(Column.create("k", LongType)), Array.empty)
    sql("INSERT INTO testcat.ns.t VALUES " + (0 until 20).map(i => s"($i)").mkString(", "))
    val df = sql("SELECT * FROM testcat.ns.t WHERE t.k IN (SELECT f.id FROM testcat.ns.fact f " +
      "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1 AND f.v < 100)")
    val plan = df.queryExecution.optimizedPlan
    assert(plan.exists(_.expressions.exists(_.exists {
      case b: BloomFilterMightContain => b.bloomFilterExpression.exists {
        case subquery: ScalarSubquery => subquery.plan.exists(_.expressions.exists(
          _.exists(_.isInstanceOf[BloomFilterMightContain])))
        case _ => false
      }
      case _ => false
    })), s"test setup: a bloom filter's subquery must check another one:\n$plan")
    assert(perPartitionSemiJoins(df).isEmpty, plan)
    assert(!plan.exists {
      case node @ (_: Filter | _: Project) =>
        node.expressions.exists(_.exists(_.isInstanceOf[TransformExpression]))
      case _ => false
    }, s"the partition key of the semi join must go too:\n$plan")
    checkAnswer(df, Seq(Row(1), Row(3), Row(5), Row(9)))
  }

  test("a placeholder that MergeSubplans moves out of its filter becomes true") {
    // With filter propagation through joins, `MergeSubplans` merges the two aggregates. It moves
    // the filter of `fact` that holds the placeholder into a projection and an `Or`, where no semi
    // join goes.
    createTables()
    withSQLConf(
        MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true",
        MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      val df = sql("SELECT * FROM (SELECT sum(f.v) c FROM testcat.ns.fact f " +
        "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1) a, (SELECT max(f.v) s " +
        "FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 0) b")
      val plan = df.queryExecution.optimizedPlan
      assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER), plan)
      checkAnswer(df, Seq(Row(180, 60)))
    }
  }

  test("the semi join keeps the rows whose partition key is null") {
    // This `bucket` returns null for the key 5, which is not null. The join matches the row of
    // `fact` with that key, so the semi join must keep it.
    createBucketFunction() { input =>
      if (input.getLong(1) == 5L) null else BucketFunction.produceResult(input)
    }
    createTables()
    Seq(true, false).foreach { adaptive =>
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = join()
        assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
        checkAnswer(df, expectedRows)
        assert(executedSemiJoins(df.queryExecution.executedPlan).size == 1,
          df.queryExecution.executedPlan)
      }
    }
  }

  test("a semi join over a creation side whose scalar subqueries MergeSubplans merges") {
    // The aggregate of the per-partition bloom filter copies the creation side with its two
    // subqueries. `MergeSubplans` merges them in the plan, but not in the copy, which keeps its
    // own.
    createTwoKeyTables()
    createTable("t", Array(Column.create("flag", IntegerType)), Array.empty)
    sql("INSERT INTO testcat.ns.t VALUES (1), (0)")
    Seq(true, false).foreach { adaptive =>
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
          "ON f.id = d.id WHERE d.flag = (SELECT max(flag) FROM testcat.ns.t) " +
          "AND d.x >= (SELECT min(flag) FROM testcat.ns.t)")
        val plan = df.queryExecution.optimizedPlan
        assert(plan.exists(_.expressions.exists(_.exists {
          case GetStructField(_: ScalarSubquery, _, _) => true
          case _ => false
        })), s"test setup: the two subqueries must be merged:\n$plan")
        assert(perPartitionSemiJoins(df).size == 1, plan)
        checkAnswer(df, expectedRows)
        assert(executedSemiJoins(df.queryExecution.executedPlan).size == 1,
          df.queryExecution.executedPlan)
      }
    }
  }

  test("a placeholder alone takes no place under the number threshold of runtime filters") {
    // The join with `dim` comes first. `dim` is too big for a bloom filter of all its keys, so the
    // join gets only a placeholder. With a threshold of one runtime filter, the join with `s` still
    // gets its bloom filter.
    createTables()
    createTable("s", Array(Column.create("v", IntegerType), Column.create("flag", IntegerType)),
      Array.empty)
    sql("INSERT INTO testcat.ns.s VALUES (10, 1)")
    val size = creationSideSize("dim")
    withSQLConf(
        RUNTIME_FILTER_NUMBER_THRESHOLD.key -> "1",
        RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id " +
        "JOIN testcat.ns.s s ON f.v = s.v WHERE d.flag = 1 AND s.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).size == 1, plan)
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

  test("per-partition bloom filters have a number threshold of their own") {
    // `dim` and `dim2` are too big for a bloom filter of all their keys, so each of the two joins
    // below gets only a placeholder. With a threshold of one, only the first one gets it.
    createTables()
    createTable("fact2", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.fact2 VALUES (1, 10), (3, 30)")
    createTable("dim2", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.dim2 VALUES (1, 1), (3, 1), (5, 0)")
    val size = creationSideSize("dim").min(creationSideSize("dim2"))
    withSQLConf(
        RUNTIME_FILTER_NUMBER_THRESHOLD.key -> "1",
        RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT a.id, b.id FROM (SELECT f.id, f.v FROM testcat.ns.fact f " +
        "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1) a JOIN (SELECT f2.id, f2.v " +
        "FROM testcat.ns.fact2 f2 JOIN testcat.ns.dim2 d2 ON f2.id = d2.id WHERE d2.flag = 1) b " +
        "ON a.v = b.v")
      assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
      checkAnswer(df, Seq(Row(1, 1), Row(3, 3)))
    }
  }

  test("MergeSubplans merges two subplans that hold a placeholder each") {
    // The two aggregates read the same join. `dim` is too big for a bloom filter of all its keys,
    // so each join gets only a placeholder. The two are the same check, so the subplans merge.
    // `fact` is read once, as without the placeholders.
    createTables()
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
        (creationSideSize("dim") / 2).toString) {
      val df = sql("SELECT * FROM (SELECT sum(f.v) s FROM testcat.ns.fact f " +
        "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1) a, (SELECT max(f.v) m " +
        "FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1) b")
      checkAnswer(df, Seq(Row(180, 90)))
      val plan = df.queryExecution.executedPlan
      val scans = collectWithSubqueries(plan) { case scan: BatchScanExec => scan }
      assert(scans.count(_.table.name().endsWith("fact")) == 1, plan)
    }
  }

  test("the rule that builds the semi joins cannot be excluded") {
    // Nothing can evaluate a placeholder.
    createTables()
    withSQLConf(
        OPTIMIZER_EXCLUDED_RULES.key -> InsertPerPartitionRuntimeFilterJoins.ruleName) {
      val df = join()
      assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
      checkAnswer(df, expectedRows)
    }
  }

  test("no per-partition bloom filter for a non-deterministic creation side") {
    // Its aggregate evaluates the creation side again, apart from the join. With a random filter
    // there, it can miss keys the join sees, and the semi join would drop rows that match them
    // (SPARK-60808). Every key of `nd` is in `nf`, so no row may lose its match. `nd` is too big
    // for a bloom filter of all its keys, so only the per-partition one could be added.
    createTable("nd", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.nd VALUES " + (0 until 200).map(i => s"($i, 1)").mkString(", "))
    createTable("nf", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.nf VALUES " + (0 until 200).map(i => s"($i, $i)").mkString(", "))
    val size = creationSideSize("nd")
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT count(*), count_if(f.id IS NULL) FROM (SELECT id FROM testcat.ns.nd " +
        "WHERE flag = 1 AND cast(reflect('java.lang.Math', 'random') AS DOUBLE) < 0.5) d " +
        "LEFT JOIN testcat.ns.nf f ON d.id = f.id")
      assert(perPartitionSemiJoins(df).isEmpty, df.queryExecution.optimizedPlan)
      assert(df.collect().head.getLong(1) == 0L, "a row of `d` lost its match")
    }
  }

  test("no per-partition bloom filter when it is turned off") {
    createTables()
    withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
      checkOnlyGlobalBloomFilter(join())
    }
  }

  test("no per-partition bloom filter for an identity partitioning") {
    // Each partition would hold a single key, so its bloom filter would check nothing.
    createTables(factPartitioning = Array(identity("id")), dimPartitioning = Array(identity("id")))
    checkOnlyGlobalBloomFilter(join())
  }

  test("no per-partition bloom filter for different transforms") {
    createTables(dimPartitioning = Array(bucket(8, "id")))
    checkOnlyGlobalBloomFilter(join())
  }

  test("no per-partition bloom filter for a non-deterministic transform") {
    // Both sides of the semi join evaluate the transform, so they could disagree on a row's key.
    createBucketFunction(deterministic = false)(BucketFunction.produceResult(_))
    createTables()
    checkOnlyGlobalBloomFilter(join())
  }

  test("a column narrower than the transform's input type is widened to it") {
    // The test catalog's `bucket` takes a BIGINT value, so over an INT column its call casts the
    // value. Both sides of the semi join evaluate it the same way.
    createTables(idType = IntegerType)
    val df = join()
    assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
    checkAnswer(df, expectedRows)
  }

  test("no per-partition bloom filter when the column is wider than the transform's input type") {
    // This `bucket` takes an INT value, which the BIGINT column would have to be narrowed to.
    createBucketFunction(types = Array(IntegerType, IntegerType)) { input =>
      Math.floorMod(input.getInt(1), input.getInt(0))
    }
    createTables()
    checkOnlyGlobalBloomFilter(join())
  }
}

/**
 * A catalog whose scans offer the source column of every partition transform for runtime
 * filtering, as Iceberg's do, rather than only the identity ones. They also take a storage filter
 * on those columns, which their readers skip.
 */
class PartitionSourceFilteringCatalog extends InMemoryCatalog {
  // scalastyle:off argcount
  override protected def newInMemoryTable(
      name: String,
      columns: Array[Column],
      transforms: Array[Transform],
      properties: java.util.Map[String, String],
      constraints: Array[Constraint],
      distribution: Distribution,
      ordering: Array[SortOrder],
      requiredNumPartitions: Option[Int],
      advisoryPartitionSize: Option[Long],
      distributionStrictlyRequired: Boolean,
      numRowsPerSplit: Int,
      id: String,
      schemaAlignmentConfig: SchemaAlignmentConfig): InMemoryBaseTable = {
    // scalastyle:on argcount
    new InMemoryTable(name, columns, transforms, properties, constraints, distribution,
      ordering, requiredNumPartitions, advisoryPartitionSize, distributionStrictlyRequired,
      numRowsPerSplit, id, schemaAlignmentConfig) {
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
        new InMemoryScanBuilder(schema, options) {
          override protected def createScan(
              partitions: Seq[InputPartition],
              scanSchema: StructType,
              fullSchema: StructType,
              scanOptions: CaseInsensitiveStringMap): BatchScanBaseClass =
            new InMemoryBatchScan(partitions, scanSchema, fullSchema, scanOptions)
                with SupportsStorageFilters {
              override def filterAttributes(): Array[NamedReference] =
                transforms.flatMap(_.references).filter(ref => scanSchema.fieldNames.contains(
                  ref.fieldNames.head))
              override def storageFilterAttributes(): Array[NamedReference] = filterAttributes()
              override def createReaderFactory(): PartitionReaderFactory =
                StorageFilterSkippingReaderFactory(super.createReaderFactory())
            }
        }
    }
  }
}

/**
 * Reads as `factory` does, without applying a storage filter. A reader may skip it when Spark asks
 * for no checked column.
 */
case class StorageFilterSkippingReaderFactory(factory: PartitionReaderFactory)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    factory.createReader(partition)

  override def createReader(
      partition: InputPartition,
      request: StorageFilterRequest): PartitionReader[InternalRow] = {
    assert(!request.appendsCheckedColumn())
    factory.createReader(partition)
  }
}
