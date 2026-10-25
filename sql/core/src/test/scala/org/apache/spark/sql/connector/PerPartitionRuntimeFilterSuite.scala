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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, DynamicPruningSubquery, Expression, GetStructField, PlanExpression, ScalarSubquery, TransformExpression}
import org.apache.spark.sql.catalyst.optimizer.{InsertPerPartitionRuntimeFilterJoins, PerPartitionRuntimeFilterSubquery}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.trees.TreePattern.PER_PARTITION_RUNTIME_FILTER_SUBQUERY
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryCatalystRuntimeFilterCatalog, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, BucketFunction, ScalarFunction, UnboundBucketFunction, UnboundFunction, UnboundIntBucketFunction}
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, identity}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{InputAdapter, ProjectExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}
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

  // `fact` and `dim`, both bucketed by `id` into 4 by default.
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
  // join gets from `table`. That side is then too big for one bloom filter of all its keys.
  private def withoutGlobalBloomFilter(table: String)(f: => Unit): Unit =
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key ->
      ((creationSideSize(table) + 1) / 2).toString)(f)

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
        assert(perPartitionSemiJoinsWithSubqueries(df).isEmpty, plan)
        assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY), plan)
        checkAnswer(df, Seq(Row(180, 60)))
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
      // The nodes of the codegen stage of `plan`, down to its inputs.
      def inStage(plan: SparkPlan): Seq[SparkPlan] = plan match {
        case _: InputAdapter => Nil
        case _ => plan +: plan.children.flatMap(inStage)
      }
      val plan = df.queryExecution.executedPlan
      val partitionKeys = collect(plan) { case stage: WholeStageCodegenExec => stage }
        .flatMap(stage => inStage(stage.child))
        .collect { case p: ProjectExec => p }
        .filter(_.projectList.exists(_.exists(_.isInstanceOf[TransformExpression])))
      assert(partitionKeys.size == 2, plan)
    }
  }

  test("no per-partition bloom filter when the column is wider than the transform's input type") {
    // This `bucket` takes an INT value, which the BIGINT column would have to be narrowed to.
    createBucketFunction(types = Array(IntegerType, IntegerType)) { input =>
      Math.floorMod(input.getInt(1), input.getInt(0))
    }
    createTables()
    checkNoRuntimeFilter(join())
  }
}
