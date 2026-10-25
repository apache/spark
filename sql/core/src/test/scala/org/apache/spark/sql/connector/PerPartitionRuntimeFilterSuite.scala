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
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, GetStructField, PlanExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.{InsertPerPartitionRuntimeFilterJoins, PerPartitionRuntimeFilterPlaceholder}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.trees.TreePattern.PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER
import org.apache.spark.sql.connector.catalog.{Column, Identifier}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, BucketFunction, ScalarFunction, UnboundBucketFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, identity}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}

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

  private def createTables(
      idType: DataType = LongType,
      factPartitioning: Array[Transform] = Array(bucket(4, "id")),
      dimPartitioning: Array[Transform] = Array(bucket(4, "id"))): Unit = {
    createTable("fact", Array(Column.create("id", idType), Column.create("v", IntegerType)),
      factPartitioning)
    sql("INSERT INTO testcat.ns.fact VALUES " +
      (0 until 16).map(i => s"($i, ${i * 10})").mkString(", "))
    createTable("dim", Array(Column.create("id", idType), Column.create("flag", IntegerType)),
      dimPartitioning)
    sql("INSERT INTO testcat.ns.dim VALUES (1, 1), (2, 0), (3, 1), (5, 1), (6, 0), (9, 1)")
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

  private def join(): DataFrame = sql(
    """
      |SELECT f.id, f.v
      |FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id
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

  // A semi join of a per-partition bloom filter.
  private val perPartitionSemiJoin: PartialFunction[LogicalPlan, Join] = {
    case j @ Join(_, _, ExistenceJoin(_), Some(condition), _)
        if condition.exists(_.isInstanceOf[BloomFilterMightContain]) => j
  }

  private def perPartitionSemiJoins(df: DataFrame): Seq[Join] =
    df.queryExecution.optimizedPlan.collect(perPartitionSemiJoin)

  // A bloom filter of the whole creation side, which a subquery computes.
  private def isGlobalCheck(e: Expression): Boolean = e match {
    case BloomFilterMightContain(_: PlanExpression[_], _) => true
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

  test("a per-partition bloom filter joins the application side next to the global bloom filter") {
    createTables()
    val df = join()
    val plan = df.queryExecution.optimizedPlan
    assert(perPartitionSemiJoins(df).size == 1, plan)
    assert(hasGlobalBloomFilter(plan), plan)
    checkAnswer(df, expectedRows)
    assert(collect(df.queryExecution.executedPlan) { case s: ShuffleExchangeExec => s }.isEmpty,
      "the semi join pairs the partitions as the join does")
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
      assert(joinBelow.size == 1 && joinBelow.head.left.exists(perPartitionSemiJoin.isDefinedAt),
        s"`dim` must keep its semi join:\n$plan")
      assert(!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER),
        s"no placeholder may be left, in a subquery or an aggregate either:\n$plan")
      checkAnswer(df, Seq(Row(1, 10), Row(3, 30)))
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
