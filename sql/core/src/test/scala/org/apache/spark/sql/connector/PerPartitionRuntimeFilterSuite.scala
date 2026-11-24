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
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, DynamicPruningExpression, DynamicPruningSubquery, Expression, GetStructField, Literal, PlanExpression, ScalarSubquery, TransformExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.{InsertPerPartitionRuntimeFilterJoins, PerPartitionRuntimeFilterSubquery}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.catalyst.trees.TreePattern.PER_PARTITION_RUNTIME_FILTER_SUBQUERY
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryCatalystRuntimeFilterCatalog, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, BucketFunction, LongDaysFunction, ScalarFunction, UnboundBucketFunction, UnboundFunction, UnboundIntBucketFunction, UnboundPhysicalDaysFunction}
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, days, identity}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec, ShuffledHashJoinExec}
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType, TimestampType}
import org.apache.spark.tags.DataSourceV2Test

@DataSourceV2Test
class PerPartitionRuntimeFilterSuite extends KeyGroupedPartitioningSuiteBase {

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

  // `fact` and `dim`, both bucketed by `id` into 4 by default. Then the 6 rows of `dim` have 3
  // partition keys, since the test catalog's `bucket` takes the value modulo the number of buckets.
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

  // A table `name` of `id BIGINT` and an INT column `column`, bucketed by `id` into 4, with the
  // rows of the VALUES list `values`.
  private def createIdTable(
      name: String,
      column: String,
      values: String,
      catalog: InMemoryTableCatalog = catalog): Unit = {
    createTable(name, Array(Column.create("id", LongType), Column.create(column, IntegerType)),
      Array(bucket(4, "id")), catalog = catalog)
    sql(s"INSERT INTO ${catalog.name()}.ns.$name VALUES $values")
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

  // Runs `f` with `fact` and `dim` in a catalog named `rfcat`, whose scans offer the source column
  // of every partition transform for runtime filtering, as Iceberg's do, rather than only the
  // identity ones.
  private def withPartitionSourceFilteringTables(f: InMemoryTableCatalog => Unit): Unit = {
    withSQLConf(
        "spark.sql.catalog.rfcat" -> classOf[InMemoryCatalystRuntimeFilterCatalog].getName) {
      val rfcat = spark.sessionState.catalogManager.catalog("rfcat")
        .asInstanceOf[InMemoryCatalystRuntimeFilterCatalog]
      try {
        rfcat.createFunction(Identifier.of(Array.empty, UnboundBucketFunction.name()),
          UnboundBucketFunction)
        createTables(catalog = rfcat)
        f(rfcat)
      } finally {
        rfcat.clearTables()
        rfcat.clearFunctions()
      }
    }
  }

  private def join(): DataFrame = sql(
    """
      |SELECT f.id, f.v
      |FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id
      |WHERE d.flag = 1
      |""".stripMargin)

  // A table `other` of `id BIGINT`, with the ids 0 to 15 bucketed into 4.
  private def createOtherTable(): Unit = {
    createTable("other", Array(Column.create("id", LongType)), Array(bucket(4, "id")))
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
  private def creationSideSize(
      table: String,
      column: String = "id",
      catalogName: String = "testcat"): BigInt =
    spark.table(s"$catalogName.ns.$table").where("flag = 1").select(column)
      .queryExecution.optimizedPlan.stats.sizeInBytes

  // Runs `f` with the creation side threshold at about half the size of the creation side that a
  // join gets from `table`. That side is then too big for one bloom filter of all its keys. With 2
  // partition keys or more, each share is within the threshold.
  private def withoutGlobalBloomFilter(table: String, column: String = "id")(f: => Unit): Unit =
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
      ((creationSideSize(table, column) + 1) / 2).toString)(f)

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

  // A semi join of a per-partition bloom filter.
  private val perPartitionSemiJoin: PartialFunction[LogicalPlan, Join] = {
    case j: Join if PerPartitionRuntimeFilterSubquery.semiJoinKey(j).isDefined => j
  }

  private def perPartitionSemiJoins(df: DataFrame): Seq[Join] =
    df.queryExecution.optimizedPlan.collect(perPartitionSemiJoin)

  // Whether `InsertPerPartitionRuntimeFilterJoins` changed the plan of `df`. It does when the plan
  // has placeholders, even if none gets a semi join.
  private def replacedPlaceholders(df: DataFrame): Boolean = {
    df.queryExecution.optimizedPlan
    df.queryExecution.tracker.rules.get(InsertPerPartitionRuntimeFilterJoins.ruleName)
      .exists(_.numEffectiveInvocations > 0)
  }

  // The semi joins of per-partition bloom filters of `df`, in its subqueries too.
  private def perPartitionSemiJoinsWithSubqueries(df: DataFrame): Seq[Join] =
    df.queryExecution.optimizedPlan.collectWithSubqueries(perPartitionSemiJoin)

  // The scans of `table` that `plan` runs, in its subqueries too. A reused subquery counts once.
  private def executedScans(plan: SparkPlan, table: String): Seq[BatchScanExec] =
    collectWithSubqueries(plan) { case scan: BatchScanExec => scan }
      .filter(_.table.name().split('.').last == table)

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

  // With `dim` too big for a bloom filter of all its keys, `InjectRuntimeFilter` added no runtime
  // filter to `df`.
  private def checkNoRuntimeFilter(df: => DataFrame): Unit = withoutGlobalBloomFilter("dim") {
    val plan = df.queryExecution.optimizedPlan
    assert(perPartitionSemiJoins(df).isEmpty && !hasGlobalBloomFilter(plan), plan)
    checkAnswer(df, expectedRows)
  }

  private def executedSemiJoins(plan: SparkPlan): Seq[ShuffledHashJoinExec] =
    collect(plan) { case j: ShuffledHashJoinExec if j.joinType.isInstanceOf[ExistenceJoin] => j }

  private def executedShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] =
    collect(plan) { case s: ShuffleExchangeExec => s }

  test("a creation side within the size threshold gets only a bloom filter of all its keys") {
    createTables()
    val df = join()
    val plan = df.queryExecution.optimizedPlan
    assert(perPartitionSemiJoins(df).isEmpty && hasGlobalBloomFilter(plan), plan)
    assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY), plan)
    checkAnswer(df, expectedRows)
  }

  test("a creation side too big for a bloom filter of all its keys gets a per-partition one") {
    createTables()
    withoutGlobalBloomFilter("dim") {
      val df = join()
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).size == 1 && !hasGlobalBloomFilter(plan), plan)
      checkAnswer(df, expectedRows)
    }
  }

  Seq(true, false).foreach { adaptive =>
    test(s"the semi join stays when it pairs (AQE $adaptive)") {
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          val df = join()
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedSemiJoins(plan).size == 1, plan)
          assert(executedShuffles(plan).isEmpty,
            "the semi join pairs the partitions as the join does")
        }
      }
    }

    test(s"the semi join goes when it would shuffle (AQE $adaptive)") {
      // Without pushing the partition values, the two sides' different keys cannot pair.
      createTables()
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false") {
        withoutGlobalBloomFilter("dim") {
          val df = join()
          assert(perPartitionSemiJoins(df).size == 1, "test setup: the rule added it")
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedSemiJoins(plan).isEmpty, plan)
          assert(collect(plan) { case p @ ProjectExec(_, _: ProjectExec) => p }.isEmpty,
            s"the semi join's projection must go with it:\n$plan")
        }
      }
    }

    test(s"the semi join goes when the join shuffles the application side anyway (AQE $adaptive)") {
      // The join's keys are more than the partition key, so it shuffles both sides. The semi join
      // would group the application side's splits ahead of that shuffle.
      createTwoKeyTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
            "ON f.id = d.id AND f.x = d.x WHERE d.flag = 1")
          assert(perPartitionSemiJoins(df).size == 1, "test setup: the rule added it")
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedShuffles(plan).nonEmpty, s"test setup: the join must shuffle:\n$plan")
          assert(executedSemiJoins(plan).isEmpty, plan)
        }
      }
    }

    test(s"a semi join that goes takes no free one with it (AQE $adaptive)") {
      // Without pushed partition values, the semi join of `fact2` cannot pair with `dim2`, which
      // has one partition key only, and shuffles. The semi join of `fact1` pairs, as the joins
      // above do. Judged next to the other one, it would look as if it cut tasks.
      Seq("fact1" -> "v", "fact2" -> "w").foreach { case (name, column) =>
        createTable(name, Array(Column.create("id", LongType), Column.create(column, IntegerType)),
          Array(bucket(4, "id")))
        // Two inserts, so that each partition key has more than one split.
        Seq(0 until 8, 8 until 16).foreach { ids =>
          sql(s"INSERT INTO testcat.ns.$name VALUES " + ids.map(i => s"($i, $i)").mkString(", "))
        }
      }
      createIdTable("dim", "flag", (0 until 16).map(i => s"($i, ${i % 2})").mkString(", "))
      // As many rows as `dim`, all with the partition key 1.
      createIdTable("dim2", "flag", (1 until 64 by 4).map(i => s"($i, 1)").mkString(", "))
      val threshold = (creationSideSize("dim").min(creationSideSize("dim2")) + 1) / 2
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false",
          RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> threshold.toString) {
        val df = sql("SELECT f1.id, f1.v, f2.w FROM testcat.ns.fact1 f1 JOIN testcat.ns.fact2 f2 " +
          "ON f1.id = f2.id JOIN testcat.ns.dim d1 ON f1.id = d1.id JOIN testcat.ns.dim2 d2 " +
          "ON f2.id = d2.id WHERE d1.flag = 1 AND d2.flag = 1")
        assert(perPartitionSemiJoins(df).size == 2, "test setup: the rule added both")
        checkAnswer(df, Seq(1, 5, 9, 13).map(i => Row(i, i, i)))
        val plan = df.queryExecution.executedPlan
        val semiJoins = executedSemiJoins(plan)
        assert(semiJoins.size == 1 && semiJoins.head.left.collectLeaves().exists {
          case scan: BatchScanExec => scan.table.name().split('.').last == "fact1"
          case _ => false
        }, s"the semi join of `fact1` must stay:\n$plan")
      }
    }

    test(s"a semi join over another one is judged by its own application side (AQE $adaptive)") {
      // `rand()` keeps the placeholders of `dim1` and `dim2` above the projection, in one filter.
      // So the semi join of `dim2` is built over that of `dim1`. Both pair with the joins above.
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
        val semiJoins = perPartitionSemiJoins(df)
        assert(semiJoins.size == 2 &&
          semiJoins.exists(_.left.exists(perPartitionSemiJoin.isDefinedAt)),
          s"test setup: one semi join must be over the other:\n${df.queryExecution.optimizedPlan}")
        checkAnswer(df, Seq(1, 3, 5, 9).map(i => Row(i, i * 10, i * 10, true)))
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).size == 2, plan)
      }
    }

    test(s"with partially clustered distribution, the semi join keeps the application side's " +
        s"splits (AQE $adaptive)") {
      // The creation side's aggregate is the smaller side, so it is the one that repeats, once for
      // each split of the application side.
      createTables(numFactRows = 400)
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        withoutGlobalBloomFilter("dim") {
          val df = join()
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          val semiJoins = executedSemiJoins(plan)
          assert(semiJoins.size == 1, plan)
          assert(semiJoins.head.left.outputPartitioning.numPartitions > 4,
            s"the application side must keep its splits:\n$plan")
        }
      }
    }

    test(s"the semi join stays when the side it is on is read more than once (AQE $adaptive)") {
      // A cartesian product reads each side once for each partition of the other. A broadcast
      // nested loop join that also returns the broadcast rows with no match reads its streamed side
      // twice. A range shuffle reads its child once to sample it, and once more to shuffle it.
      // Each read runs the semi join again, together with the scan of `fact` below it.
      createTables()
      createOtherTable()
      val semiJoinSide = "SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
        "ON f.id = d.id WHERE d.flag = 1"
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          Seq(
            s"SELECT count(*), sum(x.v) FROM ($semiJoinSide) x CROSS JOIN testcat.ns.other o" ->
              Seq(Row(64, 2880)),
            s"SELECT count(*), sum(x.v) FROM ($semiJoinSide) x FULL OUTER JOIN " +
              "testcat.ns.other o ON x.v < o.id" -> Seq(Row(19, 220)),
            s"$semiJoinSide ORDER BY f.v" -> expectedRows).foreach { case (query, answer) =>
            val df = sql(query)
            assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
            checkAnswer(df, answer)
            val plan = df.queryExecution.executedPlan
            assert(collect(plan) {
              case j: CartesianProductExec => j
              case j: BroadcastNestedLoopJoinExec => j
              case s: ShuffleExchangeExec if s.outputPartitioning.isInstanceOf[RangePartitioning] =>
                s
            }.size == 1, s"test setup: the semi join's side must be read again:\n$plan")
            assert(executedSemiJoins(plan).size == 1, plan)
          }
        }
      }
    }

    test(s"with partially clustered distribution, the semi join stays when the join repeats the " +
        s"side it is on (AQE $adaptive)") {
      // `fact` is the smaller side, so the join repeats it and keeps the splits of `dim`. Each
      // repeat runs the semi join again, together with the scan of `fact` below it. The existence
      // join of the semi join is estimated from `fact` (SPARK-61534), so the join still repeats
      // `fact`.
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
          assert(executedSemiJoins(plan).size == 1, plan)
          assert(joinPartitions(plan) == without, plan)
        }
      }
    }

    test(s"with partially clustered distribution, the semi join goes when it would group the " +
        s"splits the join keeps (AQE $adaptive)") {
      // The application side is the smaller side of the semi join here, which an existence join
      // cannot repeat. So the semi join would group the splits the join itself keeps apart. A
      // limit, a top-N or a write above the join reports one partition or none, however many
      // tasks read below it.
      createTables()
      createTable("target", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
        Array.empty)
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        withoutGlobalBloomFilter("dim") {
          Seq(join(), join().limit(100), join().orderBy("id").limit(100)).foreach { df =>
            assert(perPartitionSemiJoins(df).size == 1, "test setup: the rule added it")
            checkAnswer(df, expectedRows)
            val plan = df.queryExecution.executedPlan
            assert(executedSemiJoins(plan).isEmpty, plan)
          }
          val writes = withQueryExecutionsCaptured(spark) {
            sql("INSERT INTO testcat.ns.target SELECT f.id, f.v FROM testcat.ns.fact f " +
              "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1")
          }
          assert(writes.flatMap(_.optimizedPlan.collect(perPartitionSemiJoin)).size == 1,
            "test setup: the rule added it")
          val write = writes.map(_.executedPlan)
          checkAnswer(spark.table("testcat.ns.target"), expectedRows)
          assert(write.flatMap(executedSemiJoins).isEmpty, write)
        }
      }
    }

    test(s"the semi join follows the join key through a join below it (AQE $adaptive)") {
      // The application side is the storage-partitioned join of `fact` and `other`. Pushdown puts
      // the placeholder of the semi join on `fact`, below that join, and the semi join is built
      // there. So the join gets only the rows it keeps.
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          val df = joinOverJoin()
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedSemiJoins(plan).size == 1, plan)
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

    test(s"the semi join follows a renamed join key (AQE $adaptive)") {
      // The placeholder is pushed below the rename, so it checks `id` rather than `fid`.
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          val df = sql("SELECT f.fid, f.v FROM (SELECT id AS fid, v FROM testcat.ns.fact) f " +
            "JOIN testcat.ns.dim d ON f.fid = d.id WHERE d.flag = 1")
          val keys =
            perPartitionSemiJoins(df).flatMap(PerPartitionRuntimeFilterSubquery.semiJoinKey)
          assert(keys.map(_.references.map(_.name).toSet) == Seq(Set("id")),
            df.queryExecution.optimizedPlan)
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedSemiJoins(plan).size == 1, plan)
        }
      }
    }
  }

  test("the semi joins of a plan optimized with the conf on are judged with it off") {
    // The semi join would shuffle, as in "the semi join goes when it would shuffle".
    createTables()
    withSQLConf(V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false") {
      withoutGlobalBloomFilter("dim") {
        val df = join()
        assert(perPartitionSemiJoins(df).size == 1, "test setup: the rule added it")
        withSQLConf(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
          checkAnswer(df, expectedRows)
          assert(executedSemiJoins(df.queryExecution.executedPlan).isEmpty,
            df.queryExecution.executedPlan)
        }
      }
    }
  }

  // The expected numbers of items and the numbers of bits of the bloom filters that the semi joins
  // of `df` build.
  private def executedSizes(df: DataFrame): Seq[(Long, Long)] = {
    df.collect()
    def value(e: Expression): Long = e.eval().asInstanceOf[Number].longValue
    executedSemiJoins(df.queryExecution.executedPlan)
      .flatMap(_.right.collect { case a: BaseAggregateExec => a })
      .flatMap(_.aggregateExpressions.flatMap(_.collect { case b: BloomFilterAggregate => b }))
      .map(b => (value(b.estimatedNumItemsExpression), value(b.numBitsExpression)))
      .distinct
  }

  test("each per-partition bloom filter gets an even share of the items and bits over the " +
      "creation side's partition keys") {
    // Without CBO the Filter on the creation side drops its row count. So the creation side, twice
    // the size threshold, gets the default expected number of items and number of bits twice. `dim`
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

  test("the semi join goes when the creation side has too few partition keys for the size " +
      "threshold") {
    // The creation side is 4 times the threshold, but `dim` has only 3 partition keys. So a share
    // of one partition key would be over the threshold.
    createTables()
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
        (creationSideSize("dim") / 4).toString) {
      val df = join()
      assert(perPartitionSemiJoins(df).size == 1, "test setup: the rule added it")
      checkAnswer(df, expectedRows)
      assert(executedSemiJoins(df.queryExecution.executedPlan).isEmpty,
        df.queryExecution.executedPlan)
    }
  }

  Seq(true, false).foreach { adaptive =>
    test(s"a semi join that goes leaves a broadcast that a DPP filter can reuse (AQE $adaptive)") {
      // `fact` can get a DPP filter on the source column of its bucket transform. The broadcast
      // side joins `dim` and `dim2`, which gives `dim` a semi join. `dim2` has 1 partition key,
      // under the 2 or more that the threshold needs, so the semi join goes. The DPP filter of
      // `fact` then reuses the broadcast, as without per-partition bloom filters. That holds also
      // with a projection above `dim` that computes a column or renames one.
      withSQLConf(
          ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          "spark.sql.catalog.rfcat" -> classOf[InMemoryCatalystRuntimeFilterCatalog].getName) {
        val rfcat = spark.sessionState.catalogManager.catalog("rfcat")
          .asInstanceOf[InMemoryCatalystRuntimeFilterCatalog]
        try {
          rfcat.createFunction(Identifier.of(Array.empty, UnboundBucketFunction.name()),
            UnboundBucketFunction)
          createIdTable("fact", "v", (0 until 64).map(i => s"($i, $i)").mkString(", "), rfcat)
          createIdTable("dim", "flag", (0 until 32).map(i => s"($i, ${i % 2})").mkString(", "))
          // All with the partition key 1.
          createIdTable("dim2", "flag", (1 until 64 by 4).map(i => s"($i, 1)").mkString(", "))
          withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
              (creationSideSize("dim2") / 2).toString) {
            val odd = 1 until 32 by 4
            Seq(
              "SELECT /*+ BROADCAST(d) */ f.id, f.v FROM rfcat.ns.fact f JOIN (SELECT a.id " +
                "FROM testcat.ns.dim a JOIN testcat.ns.dim2 b ON a.id = b.id " +
                "WHERE a.flag = 1 AND b.flag = 1) d ON f.id = d.id" -> odd.map(i => Row(i, i)),
              "SELECT /*+ BROADCAST(d) */ f.id, f.v, d.id2 FROM rfcat.ns.fact f JOIN " +
                "(SELECT a.id, a.id2 FROM (SELECT id, id * 2 AS id2 FROM testcat.ns.dim " +
                "WHERE flag = 1) a JOIN testcat.ns.dim2 b ON a.id = b.id WHERE b.flag = 1) d " +
                "ON f.id = d.id" -> odd.map(i => Row(i, i, i * 2)),
              "SELECT /*+ BROADCAST(d) */ f.id, f.v FROM rfcat.ns.fact f JOIN (SELECT a.k " +
                "FROM (SELECT id AS k FROM testcat.ns.dim WHERE flag = 1) a " +
                "JOIN testcat.ns.dim2 b ON a.k = b.id WHERE b.flag = 1) d " +
                "ON f.id = d.k" -> odd.map(i => Row(i, i))
            ).foreach { case (query, expected) =>
              val df = sql(query)
              assert(perPartitionSemiJoins(df).size == 1, s"test setup: the rule added it: $query")
              checkAnswer(df, expected)
              val plan = df.queryExecution.executedPlan
              assert(executedSemiJoins(plan).isEmpty, plan)
              val factFilters = executedScans(plan, "fact").flatMap(_.runtimeFilters)
              assert(factFilters.exists {
                case DynamicPruningExpression(Literal.TrueLiteral) => false
                case _: DynamicPruningExpression => true
                case _ => false
              }, s"the DPP filter of `fact` must reuse the broadcast:\n$plan")
            }
          }
        } finally {
          rfcat.clearTables()
          rfcat.clearFunctions()
        }
      }
    }
  }

  test("for a transform other than bucket, the bloom filters are sized over the partition keys " +
      "too") {
    // The 6 rows of `dim` are over 2 days, so its scan reports 2 partition keys.
    catalog.createFunction(Identifier.of(Array.empty, "days"),
      new UnboundPhysicalDaysFunction(LongDaysFunction))
    createTable("fact", Array(Column.create("ts", TimestampType), Column.create("v", IntegerType)),
      Array(days("ts")))
    sql("INSERT INTO testcat.ns.fact VALUES (TIMESTAMP '2026-01-01 01:00:00', 1), " +
      "(TIMESTAMP '2026-01-01 02:00:00', 2), (TIMESTAMP '2026-01-02 01:00:00', 3), " +
      "(TIMESTAMP '2026-01-03 01:00:00', 4)")
    createTable("dim", Array(Column.create("ts", TimestampType),
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
      assert(perPartitionSemiJoins(df).size == 1, "test setup: the rule added it")
      checkAnswer(df, Seq(Row(1), Row(2), Row(3)))
      assert(executedSemiJoins(df.queryExecution.executedPlan).isEmpty,
        df.queryExecution.executedPlan)
    }
  }

  test("no placeholder for a creation side that needs more partition keys than its buckets give") {
    // At this threshold `dim1` needs at least 8 partition keys, more than its 4 buckets and null
    // give. Its semi join would always go, and its placeholder would keep the join with `dim2` on
    // the same key from getting one.
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
      val semiJoins = executedSemiJoins(plan)
      assert(semiJoins.size == 1 && executedScans(semiJoins.head.right, "dim2").nonEmpty,
        s"`fact` must get the semi join of `dim2`:\n$plan")
    }
  }

  test("a later join on the key of a semi join still gets its bloom filter") {
    // The creation side of `dim` is too big for one bloom filter of all its keys, so its join gets
    // only the semi join. The join with `dim2` on the same key still gets its bloom filter.
    createTables()
    createIdTable("dim2", "flag", "(1, 1), (3, 1)")
    withoutGlobalBloomFilter("dim") {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id " +
        "JOIN testcat.ns.dim2 d2 ON f.id = d2.id WHERE d.flag = 1 AND d2.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).size == 1, plan)
      assert(hasGlobalBloomFilter(plan), plan)
      checkAnswer(df, Seq(Row(1, 10), Row(3, 30)))
    }
  }

  test("no semi join on a pair of join keys whose other side gets a bloom filter of the whole " +
      "creation side") {
    // `rt` is too big for one bloom filter of all its keys, so `l` gets no such bloom filter. `rt`
    // gets the bloom filter of `l`. A semi join on `l` would read `rt` again, where that bloom
    // filter reads `l` again, so `l` gets none.
    createIdTable("l", "v", (0 until 8).map(i => s"($i, ${i * 10})").mkString(", "))
    createIdTable("rt", "flag", (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
    withoutGlobalBloomFilter("rt") {
      val df = sql("SELECT l.id, l.v FROM testcat.ns.l l JOIN testcat.ns.rt rt ON l.id = rt.id " +
        "WHERE l.v < 30 AND rt.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).isEmpty, plan)
      assert(hasGlobalBloomFilter(plan), plan)
      checkAnswer(df, Seq(Row(1, 10)))
    }
  }

  test("the semi join goes below a join on the application side") {
    // Pushdown moves the placeholder below the join of `fact` and `other`.
    createTables()
    withoutGlobalBloomFilter("dim") {
      val df = joinOverJoin()
      val plan = df.queryExecution.optimizedPlan
      // The join of `fact` and `other` is the inner join that has no other one below it.
      val joinBelow = lowestInnerJoins(plan)
      val semiJoins = perPartitionSemiJoins(df)
      assert(joinBelow.size == 1 && semiJoins.size == 1 &&
        joinBelow.head.left.exists(semiJoins.contains(_)),
        s"the semi join must sit below the join of `fact` and `other`:\n$plan")
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
      assert(perPartitionSemiJoins(df).isEmpty, plan)
      assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY), plan)
      checkAnswer(df, expectedRows ++ Seq(Row(1, 11), Row(3, 31)))
    }
  }

  test("a later join's creation side leaves out the per-partition bloom filters of the joins " +
      "below it") {
    // `dim2` is too big for a bloom filter of all its keys, so the join of `dim` and `dim2` gives
    // `dim` a placeholder. The join with `fact` takes the rows of `dim` as its creation side,
    // without that placeholder. A copy there would be a second one, and `dim` would get no semi
    // join.
    createTwoKeyTables()
    createIdTable("dim2", "flag", "(1, 1), (3, 1), (5, 0)")
    withoutGlobalBloomFilter("dim2") {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN (SELECT dim.id FROM " +
        "testcat.ns.dim JOIN testcat.ns.dim2 ON dim.id = dim2.id WHERE dim.flag = 1 AND " +
        "dim2.flag = 1) d ON f.id = d.id")
      val plan = df.queryExecution.optimizedPlan
      // The join of `dim` and `dim2` is the inner join that has no other one below it.
      val joinBelow = lowestInnerJoins(plan)
      assert(joinBelow.size == 1 && joinBelow.head.left.exists(perPartitionSemiJoin.isDefinedAt),
        s"`dim` must get its semi join:\n$plan")
      assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY),
        s"no placeholder may be left, in a subquery either:\n$plan")
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
    withPartitionSourceFilteringTables { rfcat =>
      createIdTable("dim2", "flag", (0 until 20).map(i => s"($i, ${i % 2})").mkString(", "), rfcat)
      // The size of the rows of `table` that the join reads. The threshold is under that of `dim`,
      // which is under that of `dim2`.
      def size(table: String): BigInt = creationSideSize(table, catalogName = "rfcat")
      assert(size("dim") < size("dim2"), "test setup: `dim2` must be the bigger one")
      withSQLConf(
          RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> ((size("dim") + 1) / 2).toString) {
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
        // The semi joins of `dim` and of `fact`.
        assert(perPartitionSemiJoins(df).size == 2, s"`fact` must get a semi join:\n$plan")
        checkAnswer(df, expectedRows)
      }
    }
  }

  Seq(false, true).foreach { rename =>
    test(s"an IN subquery that becomes a join gets no second semi join on a key (rename $rename)") {
      // The subquery is optimized on its own first, which adds its placeholders. Then it becomes a
      // join, and the optimizer runs on it again. That run adds no second placeholder on a key.
      // `rt` is too big for a bloom filter of all its keys, and `l` has no filter to give `rt` one.
      // With the rename, the placeholder of the first run checks `id`, below the rename, and the
      // second run sees `lid`.
      createIdTable("l", "v", (0 until 8).map(i => s"($i, ${i * 10})").mkString(", "))
      createIdTable("rt", "flag", (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
      createTable("t", Array(Column.create("k", LongType)), Array.empty)
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
        val semiJoins = perPartitionSemiJoins(df)
        val keys = semiJoins.flatMap(PerPartitionRuntimeFilterSubquery.semiJoinKey)
        assert(semiJoins.nonEmpty && keys.distinct.size == semiJoins.size, plan)
        checkAnswer(df, Seq(Row(1), Row(3), Row(5), Row(7)))
      }
    }
  }

  test("a placeholder that MergeSubplans moves out of its filter gives way to true") {
    // With filter propagation through joins, `MergeSubplans` merges the two aggregates. It moves
    // the filters of `fact` that hold the placeholders into a projection and an `Or`, where no
    // semi join goes.
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
        assert(perPartitionSemiJoinsWithSubqueries(df).isEmpty, plan)
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
    createTable("t", Array(Column.create("flag", IntegerType)), Array.empty)
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
    test(s"the semi join keeps the rows whose partition key is null (AQE $adaptive)") {
      // This `bucket` returns null for the key 5, which is not null. The join matches the row of
      // `fact` with that key, so the semi join must keep it.
      createBucketFunction() { input =>
        if (input.getLong(1) == 5L) null else BucketFunction.produceResult(input)
      }
      createTables()
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
          val df = join()
          assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
          checkAnswer(df, expectedRows)
          assert(executedSemiJoins(df.queryExecution.executedPlan).size == 1,
            df.queryExecution.executedPlan)
        }
      }
    }

    test(s"a semi join over a creation side whose scalar subqueries MergeSubplans merges " +
        s"(AQE $adaptive)") {
      // The aggregate of the per-partition bloom filter reads the creation side in the plan of its
      // placeholder. `MergeSubplans` merges the two subqueries there as it merges them in the
      // plan. So `t` is read once.
      createTwoKeyTables()
      createTable("t", Array(Column.create("flag", IntegerType)), Array.empty)
      sql("INSERT INTO testcat.ns.t VALUES (1), (0)")
      withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withoutGlobalBloomFilter("dim") {
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
          val executedPlan = df.queryExecution.executedPlan
          assert(executedSemiJoins(executedPlan).size == 1, executedPlan)
          assert(executedScans(executedPlan, "t").size == 1, executedPlan)
        }
      }
    }
  }

  test("a placeholder takes no place under the number threshold of runtime filters") {
    // The join with `dim` comes first. `dim` is too big for a bloom filter of all its keys, so the
    // join gets only a placeholder. With a threshold of one runtime filter, the join with `s` still
    // gets its bloom filter.
    createTables()
    createTable("s", Array(Column.create("v", IntegerType), Column.create("flag", IntegerType)),
      Array.empty)
    sql("INSERT INTO testcat.ns.s VALUES (10, 1)")
    withSQLConf(RUNTIME_FILTER_NUMBER_THRESHOLD.key -> "1") {
      withoutGlobalBloomFilter("dim") {
        val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
          "ON f.id = d.id JOIN testcat.ns.s s ON f.v = s.v WHERE d.flag = 1 AND s.flag = 1")
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
  }

  test("per-partition bloom filters have a number threshold of their own") {
    // `dim` and `dim2` are too big for a bloom filter of all their keys, so each of the two joins
    // below gets only a placeholder. With a threshold of one, only the first one gets it.
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
      assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
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
      assert(perPartitionSemiJoinsWithSubqueries(df).distinct.size == 1,
        df.queryExecution.optimizedPlan)
      checkAnswer(df, Seq(Row(180, 90)))
      val plan = df.queryExecution.executedPlan
      assert(executedScans(plan, "fact").size == 1, plan)
    }
  }

  test("the rule that builds the semi joins cannot be excluded") {
    // A placeholder cannot be evaluated.
    createTables()
    withSQLConf(
        OPTIMIZER_EXCLUDED_RULES.key -> InsertPerPartitionRuntimeFilterJoins.ruleName) {
      withoutGlobalBloomFilter("dim") {
        val df = join()
        assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
        checkAnswer(df, expectedRows)
      }
    }
  }

  test("the semi join of a placeholder in a subquery is built in the run on the whole query") {
    // The subquery is optimized on its own first, which adds the placeholder and leaves it there.
    createTables()
    withoutGlobalBloomFilter("dim") {
      val df = sql("SELECT (SELECT sum(f.v) FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
        "ON f.id = d.id WHERE d.flag = 1)")
      assert(perPartitionSemiJoinsWithSubqueries(df).size == 1, df.queryExecution.optimizedPlan)
      checkAnswer(df, Seq(Row(180)))
      val executedPlan = df.queryExecution.executedPlan
      assert(collectWithSubqueries(executedPlan) {
        case j: ShuffledHashJoinExec if j.joinType.isInstanceOf[ExistenceJoin] => j
      }.size == 1, executedPlan)
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
      assert(perPartitionSemiJoinsWithSubqueries(df).distinct.size == 1,
        df.queryExecution.optimizedPlan)
      checkAnswer(df, Seq(Row(180, 90)))
      val executedPlan = df.queryExecution.executedPlan
      assert(executedScans(executedPlan, "fact").size == 1, executedPlan)
    }
  }

  test("no per-partition bloom filter for a non-deterministic creation side") {
    // Its aggregate evaluates the creation side again, apart from the join. With a random filter
    // there, it can miss keys the join sees, and the semi join would drop rows that match them
    // (SPARK-60808). Every key of `nd` is in `nf`, so no row may lose its match. `nd` is too big
    // for a bloom filter of all its keys, so only the per-partition one could be added.
    createIdTable("nd", "flag", (0 until 200).map(i => s"($i, 1)").mkString(", "))
    createIdTable("nf", "v", (0 until 200).map(i => s"($i, $i)").mkString(", "))
    withoutGlobalBloomFilter("nd") {
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
      checkNoRuntimeFilter(join())
    }
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
    // Both sides of the semi join evaluate the transform, so they could disagree on a row's key.
    createBucketFunction(deterministic = false)(BucketFunction.produceResult(_))
    createTables()
    checkNoRuntimeFilter(join())
  }

  test("a column narrower than the transform's input type is widened to it") {
    // The test catalog's `bucket` takes a BIGINT value, so over an INT column its call casts the
    // value. Both sides of the semi join evaluate it the same way.
    createTables(idType = IntegerType)
    withoutGlobalBloomFilter("dim") {
      val df = join()
      assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
      checkAnswer(df, expectedRows)
    }
  }

  test("the partition keys of the semi join stay in whole-stage codegen when the function of " +
      "the transform has an invoke method") {
    // This `bucket` takes an INT value with a magic `invoke`, as Apache Iceberg's does for an INT
    // column. So both projections of a partition key, of the application side and of the
    // aggregate, generate the code of its call.
    catalog.clearFunctions()
    catalog.createFunction(Identifier.of(Array.empty, "bucket"), UnboundIntBucketFunction)
    createTables(idType = IntegerType)
    withoutGlobalBloomFilter("dim") {
      val df = join()
      assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
      checkAnswer(df, expectedRows)
      val plan = df.queryExecution.executedPlan
      val partitionKeys = codegenStageNodes(plan)
        .collect { case p: ProjectExec => p }
        .filter(_.projectList.exists(_.exists(_.isInstanceOf[TransformExpression])))
      assert(partitionKeys.size == 2, plan)
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
