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
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, DynamicPruning, DynamicPruningExpression, EqualTo, Expression, GreaterThan, Literal}
import org.apache.spark.sql.connector.catalog.{InMemoryCatalystRuntimeFilterTable, InMemoryTableCatalystRuntimeFilterCatalog}
import org.apache.spark.sql.execution.{FilterExec, ScalarSubquery => ExecScalarSubquery}
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * Tests for scans that implement
 * [[org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering]],
 * where runtime filters are pushed once as Catalyst expressions instead of connector
 * predicates.
 */
class DataSourceV2CatalystRuntimeFilterSuite extends SharedSparkSession {

  protected val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
  protected val catalogName = "testcatalystruntimefilter"

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalogName",
      classOf[InMemoryTableCatalystRuntimeFilterCatalog].getName)

  private def withDPPConf(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10")(f)
  }

  test("scalar subquery on partition column -> pushed as Catalyst expression") {
    val tbl = s"$catalogName.tbl1"
    val dim = s"$catalogName.dim1"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3))

      assertScalarSubqueryRuntimeFilters(df)
      val part = AttributeReference("part", IntegerType, nullable = false)()
      assertPushedCatalystPredicatesEqual(df, EqualTo(part, Literal(3)))
      // `part` is not declared fully pushed, so Spark still evaluates the filter after the scan.
      assertScalarSubqueryEvaluatedAfterScan(df, expected = true)
    }
  }

  test("predicate on fully pushed filter attributes -> not evaluated after the scan") {
    val tbl = s"$catalogName.tbl_fully_pushed"
    val dim = s"$catalogName.dim_fully_pushed"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part) " +
        "TBLPROPERTIES('fully-pushed-filter-attributes' = 'part')")
      // Matching and nonmatching partitions: the scan must prune nonmatching ones itself
      // because Spark drops the post-scan FilterExec for fully pushed attributes.
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3))

      assertScalarSubqueryRuntimeFilters(df)
      val part = AttributeReference("part", IntegerType, nullable = false)()
      assertPushedCatalystPredicatesEqual(df, EqualTo(part, Literal(3)))
      assertScalarSubqueryEvaluatedAfterScan(df, expected = false)
    }
  }

  test("non-deterministic predicate on fully pushed attributes -> evaluated after the scan") {
    val tbl = s"$catalogName.tbl_nondeterministic"
    val dim = s"$catalogName.dim_nondeterministic"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part) " +
        "TBLPROPERTIES('fully-pushed-filter-attributes' = 'part')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      // A non-deterministic filter is never pushed, so it must keep its post-scan FilterExec even
      // though it only references a fully pushed attribute. Dropping it there would leave nothing
      // to evaluate it and the scan would return the nonmatching partitions too.
      val df = sql(
        s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim) OR rand() < 0.5")
      // The row in the matching partition always qualifies, the others qualify at random.
      assert(df.collect().contains(Row(3, 3)))

      assertScalarSubqueryEvaluatedAfterScan(df, expected = true)
      assertScalarSubqueryRuntimeFilters(df, expectedCount = 0)
      assertPushedCatalystPredicates(df, 0)
    }
  }

  test("predicate on partly fully pushed filter attributes -> evaluated after the scan") {
    val tbl = s"$catalogName.tbl_partly_pushed"
    val dim = s"$catalogName.dim_partly_pushed"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, p1 INT, p2 INT) USING $v2Source " +
        "PARTITIONED BY (p1, p2) " +
        "TBLPROPERTIES('fully-pushed-filter-attributes' = 'p1')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, 1, 2)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      // The predicate also references p2, which is not declared fully pushed, so it is not
      // considered fully pushed and Spark keeps evaluating it after the scan.
      val df = sql(s"SELECT * FROM $tbl WHERE p1 + p2 = (SELECT max(val) FROM $dim)")
      checkAnswer(df, (0 until 5).map(i => Row(i, 1, 2)))

      assertScalarSubqueryRuntimeFilters(df)
      val p1 = AttributeReference("p1", IntegerType, nullable = false)()
      val p2 = AttributeReference("p2", IntegerType, nullable = false)()
      assertPushedCatalystPredicatesEqual(df, EqualTo(Add(p1, p2), Literal(3)))
      assertScalarSubqueryEvaluatedAfterScan(df, expected = true)
    }
  }

  test("untranslatable filter -> pushed instead of dropped") {
    val tbl = s"$catalogName.tbl2"
    val dim = s"$catalogName.dim2"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (2)")

      // `part > sub + 1` has no data source V2 translation, so the V2 interfaces would never
      // see it. The scalar subquery is literalized but the surrounding expression is kept.
      val df = sql(s"SELECT * FROM $tbl WHERE part > (SELECT max(val) FROM $dim) + 1")
      checkAnswer(df, Row(4, 4))

      assertScalarSubqueryRuntimeFilters(df)
      val part = AttributeReference("part", IntegerType, nullable = false)()
      assertPushedCatalystPredicatesEqual(
        df, GreaterThan(part, Add(Literal(2), Literal(1))))
    }
  }

  test("DPP filter -> pushed as InSubqueryExec expression") {
    val fact = s"$catalogName.fact3"
    val dim = s"$catalogName.dim3"
    withTable(fact, dim) {
      sql(s"CREATE TABLE $fact (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $fact VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (dim_id INT, dim_val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (2, 'two')")

      withDPPConf {
        val df = sql(
          s"""SELECT f.id, f.part FROM $fact f JOIN $dim d
             |ON f.part = d.dim_id WHERE d.dim_val = 'two'""".stripMargin)
        checkAnswer(df, Row(2, 2))

        assertDPPRuntimeFilters(df)
        val dppPredicate = collectBatchScan(df).runtimeFilters.collectFirst {
          case DynamicPruningExpression(e) => e
        }.get
        assertPushedCatalystPredicatesEqual(df, dppPredicate)
      }
    }
  }

  test("filter on column outside filterAttributes -> not pushed") {
    val tbl = s"$catalogName.tbl4"
    val dim = s"$catalogName.dim4"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, p1 INT, p2 INT) USING $v2Source " +
        "PARTITIONED BY (p1, p2) " +
        "TBLPROPERTIES('filter-attributes' = 'p1')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i, 10)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (10)")

      // p2 is a partition column but is not declared filterable, so no runtime filter is derived.
      val df = sql(s"SELECT * FROM $tbl WHERE p2 = (SELECT max(val) FROM $dim)")
      checkAnswer(df, (0 until 5).map(i => Row(i, i, 10)))

      assert(collectBatchScan(df).runtimeFilters.isEmpty,
        "Expected no runtime filters for a column outside filterAttributes")
      assertPushedCatalystPredicates(df, 0)
    }
  }

  test("no runtime filter -> filter() is never called") {
    val tbl = s"$catalogName.tbl5"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }

      val df = sql(s"SELECT * FROM $tbl WHERE part = 3")
      checkAnswer(df, Row(3, 3))

      assert(collectBatchScan(df).runtimeFilters.isEmpty)
      assertPushedCatalystPredicates(df, 0)
    }
  }

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  private def assertDPPRuntimeFilters(
      df: DataFrame, expectedCount: Int = 1): Unit = {
    val batchScan = collectBatchScan(df)
    val dppFilters = batchScan.runtimeFilters.collect {
      case d: DynamicPruningExpression => d
    }
    assert(dppFilters.size === expectedCount,
      s"Expected $expectedCount DynamicPruningExpression(s) " +
        s"in runtimeFilters, got ${dppFilters.size}")
  }

  private def assertScalarSubqueryRuntimeFilters(
      df: DataFrame, expectedCount: Int = 1): Unit = {
    val batchScan = collectBatchScan(df)
    val scalarFilters = batchScan.runtimeFilters.collect {
      case f if !f.isInstanceOf[DynamicPruning] => f
    }
    val dppFilters = batchScan.runtimeFilters.collect {
      case d: DynamicPruning => d
    }
    assert(scalarFilters.size === expectedCount,
      s"Expected $expectedCount scalar subquery runtime filter(s), " +
        s"got ${scalarFilters.size}")
    assert(dppFilters.isEmpty,
      "Expected non-DPP runtime filters (scalar subquery)")
  }

  /**
   * Checks whether a scalar subquery runtime filter is still evaluated by a [[FilterExec]] above
   * the scan. Filters that only reference `fullyPushedFilterAttributes` are dropped from it.
   */
  private def assertScalarSubqueryEvaluatedAfterScan(
      df: DataFrame,
      expected: Boolean): Unit = {
    val postScanConditions = stripAQEPlan(df.queryExecution.executedPlan).collect {
      case f: FilterExec => f.condition
    }
    val evaluated = postScanConditions.exists(_.exists(_.isInstanceOf[ExecScalarSubquery]))
    assert(evaluated === expected,
      s"Expected scalar subquery evaluated after scan to be $expected, " +
        s"post-scan filter conditions: $postScanConditions")
  }

  private def collectBatchScan(df: DataFrame): BatchScanExec = {
    stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
      case b: BatchScanExec => b
    }.getOrElse(fail("Expected BatchScanExec in plan"))
  }

  private def getPushedCatalystPredicates(df: DataFrame): Seq[Expression] = {
    collectBatchScan(df).scan match {
      case s: InMemoryCatalystRuntimeFilterTable#InMemoryCatalystRuntimeFilterBatchScan =>
        s.pushedCatalystPredicates
      case other =>
        fail(s"Expected InMemoryCatalystRuntimeFilterBatchScan, got $other")
    }
  }

  private def assertPushedCatalystPredicates(df: DataFrame, expected: Int): Unit = {
    val preds = getPushedCatalystPredicates(df)
    assert(preds.size === expected,
      s"Expected $expected pushed Catalyst runtime predicate(s), got ${preds.size}: $preds")
  }

  /**
   * Binds [[AttributeReference]]s in `expected` to the scan output (by name) and checks that the
   * pushed Catalyst runtime predicates match exactly via [[Expression.semanticEquals]].
   */
  private def assertPushedCatalystPredicatesEqual(
      df: DataFrame,
      expected: Expression*): Unit = {
    val batchScan = collectBatchScan(df)
    val actual = getPushedCatalystPredicates(df)
    val normalizedExpected = expected.map(bindToScanOutput(_, batchScan.output))
    assert(actual.size === normalizedExpected.size,
      s"Expected ${normalizedExpected.size} pushed Catalyst predicate(s), " +
        s"got ${actual.size}: $actual")
    actual.zip(normalizedExpected).foreach { case (a, e) =>
      assert(a.semanticEquals(e),
        s"Pushed Catalyst predicate mismatch.\nExpected: $e\nActual:   $a")
    }
  }

  private def bindToScanOutput(
      expr: Expression,
      output: Seq[AttributeReference]): Expression = {
    val resolver = SQLConf.get.resolver
    expr.transformUp {
      case a: AttributeReference =>
        output.find(o => resolver(o.name, a.name))
          .map(_.withNullability(a.nullable).withQualifier(a.qualifier))
          .getOrElse(a)
    }
  }
}
