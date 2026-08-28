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
package org.apache.spark.sql.execution

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Correctness tests for [[RewriteSelfJoinInequalityToAggregate]].
 *
 * Positive A' / A2 cases assert both result equivalence and that the rewrite actually fired.
 *
 * `assert(!ruleFired(plan))` on its own only proves the rewrite did not happen -- not that it was
 * the guard under test that stopped it. A fixture whose two self-join sides are not structurally
 * identical is rejected by `isSameBaseRelation` before any predicate is even parsed, and such a
 * test passes while covering nothing. So six important rejection paths -- the predicate parser, the
 * single-inequality requirement, output-position identity, the nondeterminism guard, the
 * leaf-source allowlist (LogicalRDD vs Parquet), and the expression-type allowlist (`abs(v)` vs
 * `v + 1`) -- are tested as single-variable pairs: the same fixture and the same query shape, one
 * control query that must fire and one variant that changes only the feature under test and must
 * not. A firing control does not pin the rejection to a particular line, but it does rule out an
 * unrelated fixture mismatch as the reason its partner was rejected. The row-bag whitelist
 * (Aggregate, Window) stays a plain negative: dropping the operator would change the query shape
 * rather than one feature.
 *
 * Self-joined fixtures are real tables, not temp views over VALUES. Spark deduplicates a self-join
 * over a [[org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation]] via `newInstance()`,
 * which refreshes one side's ExprIds without inserting a rename-only Project, so both sides stay
 * structurally identical. A temp view over VALUES cannot, and Spark renames one side with a Project
 * instead, which would make `isSameBaseRelation` false for every self-join below. `range()` needs
 * no such treatment -- Range is a MultiInstanceRelation already.
 */
class RewriteSelfJoinInequalityToAggregateSuite extends QueryTest with SharedSparkSession {

  private val rewriteConf = SQLConf.REWRITE_SELF_JOIN_INEQUALITY_TO_AGGREGATE_ENABLED.key

  /** Signature alias produced by the rewrite; presence => rule definitely fired. */
  private val CountDistinctAlias = "_rewrite_selfjoin_inequality_cnt_distinct"

  private def ruleFired(plan: LogicalPlan): Boolean =
    plan.exists {
      p =>
        p.expressions.exists(_.exists {
          case a: Alias if a.name == CountDistinctAlias => true
          case _ => false
        })
    }

  private def assertRuleFired(sql: String): Unit = {
    withSQLConf(rewriteConf -> "true") {
      val plan = spark.sql(sql).queryExecution.optimizedPlan
      assert(ruleFired(plan), s"self-join inequality rewrite should fire:\n$plan")
    }
  }

  private def assertRuleNotFired(sql: String): Unit = {
    withSQLConf(rewriteConf -> "true") {
      val plan = spark.sql(sql).queryExecution.optimizedPlan
      assert(!ruleFired(plan), s"self-join inequality rewrite must not fire:\n$plan")
    }
  }

  /**
   * A real table, so that a self-join of it dedups into two structurally identical sides. See the
   * class comment for why a temp view over VALUES cannot be used for a self-joined fixture.
   */
  private def createTable(name: String, schema: String, values: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(s"CREATE TABLE $name($schema) USING parquet")
    spark.sql(s"INSERT INTO $name SELECT * FROM VALUES $values")
  }

  /** Run `sql` twice, first with rewrite ON then OFF, and return the two result row sets. */
  private def runBoth(sql: String): (Set[Row], Set[Row]) = {
    var on: Set[Row] = null
    var off: Set[Row] = null
    withSQLConf(rewriteConf -> "true") {
      on = spark.sql(sql).collect().toSet
    }
    withSQLConf(rewriteConf -> "false") {
      off = spark.sql(sql).collect().toSet
    }
    (on, off)
  }

  private def setupTable(): Unit = {
    // k=1: distinct v={10,20}      -> matches (has 2 non-null distinct)
    // k=2: distinct v={30}         -> no match (only 1)
    // k=3: distinct v={40,50,60}   -> matches
    // k=4: v={70, NULL}            -> no match (only 1 non-null)
    // k=5: v={NULL, NULL}          -> no match (0 non-null)
    // k=6: v={80, 90, NULL}        -> matches
    // k=7: v={100,100}             -> no match: duplicate-only. Proves DISTINCT is required;
    //                                a plain COUNT(v) > 1 would wrongly match this group.
    createTable(
      "T",
      "k INT, v INT",
      """  (1, 10), (1, 10), (1, 20),
        |  (2, 30),
        |  (3, 40), (3, 50), (3, 60),
        |  (4, 70), (4, CAST(NULL AS INT)),
        |  (5, CAST(NULL AS INT)), (5, CAST(NULL AS INT)),
        |  (6, 80), (6, 90), (6, CAST(NULL AS INT)),
        |  (7, 100), (7, 100)""".stripMargin
    )
  }

  // ==================== Positive: rewrite fires and is semantically equivalent ===============

  test("Pattern A': direct InSubquery self-join is rewritten") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin

    assertRuleFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
  }

  test("Pattern A2: nested self-join is rewritten") {
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT s1.k FROM T s1 JOIN T s2
        |             ON s1.k = s2.k AND s1.v <> s2.v) sj
        |  WHERE d.k = sj.k)""".stripMargin

    assertRuleFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"Pattern A2 rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)))
  }

  test("Pattern A2: self-join on the LEFT of the outer join is rewritten") {
    // Mirror of the Pattern A2 test above. There the self-join is the RIGHT child of the outer join
    // (`selfJoinOnRight = true`); here it is the LEFT child (`selfJoinOnRight = false`). The rule
    // has an explicit branch for each side, so both are covered.
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM (SELECT s1.k FROM T s1 JOIN T s2
        |        ON s1.k = s2.k AND s1.v <> s2.v) sj, D d
        |  WHERE sj.k = d.k)""".stripMargin

    assertRuleFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"Pattern A2 (self-join on left) rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)))
  }

  test("Pattern A2: nondeterminism in the outer join condition must not be rewritten") {
    // Both self-join sides are still repeatable here, so the per-side `isSameBaseRelation` check
    // would pass; the `rand()` conjunct lives on the outer join ABOVE the self-join. Only the
    // candidate-level `isRepeatablePlan` walk over the whole subquery catches it, so the rule must
    // fail closed. This is the case the candidate-level guard exists for.
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT s1.k FROM T s1 JOIN T s2
        |             ON s1.k = s2.k AND s1.v <> s2.v) sj
        |  WHERE d.k = sj.k AND rand() < 0.5)""".stripMargin

    assertRuleNotFired(sql)
  }

  test("Pattern A': multi-equi tuple IN with sjRight key remap is rewritten") {
    // Exercises the multi-equi-key path: two equi keys (k1, k2) drive the GROUP BY, and the tuple
    // IN projects `s1.k1, s2.k2` -- so the second output column comes from the RIGHT self-join side
    // and must be remapped to its sjLeft counterpart by `canonicalizeWrapper`. This one case covers
    // multiple equi keys, tuple IN output arity, the two injected IsNotNull(equiKey) filters, and
    // the sjRight-attribute remap at once.
    createTable(
      "TM",
      "k1 INT, k2 INT, v INT",
      """  (1, 1, 10), (1, 1, 20),
        |  (1, 2, 30), (1, 2, 30),
        |  (2, 1, 40), (2, 1, 50),
        |  (CAST(NULL AS INT), 1, 60), (CAST(NULL AS INT), 1, 70),
        |  (3, CAST(NULL AS INT), 80), (3, CAST(NULL AS INT), 90)""".stripMargin)
    val sql =
      """SELECT k1, k2 FROM TM outer_t WHERE (k1, k2) IN (
        |  SELECT s1.k1, s2.k2 FROM TM s1 JOIN TM s2
        |    ON s1.k1 = s2.k1 AND s1.k2 = s2.k2 AND s1.v <> s2.v)""".stripMargin

    assertRuleFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"multi-equi tuple IN rewrite ON $on != OFF $off")
    // (1,1): distinct v={10,20} -> matches; (1,2): v={30} -> no; (2,1): v={40,50} -> matches;
    // (NULL,1) and (3,NULL): NULL equi key filtered out by the injected IsNotNull. -> {(1,1),(2,1)}
    assert(on == Set(Row(1, 1), Row(2, 1)), s"expected {(1,1),(2,1)}, got $on")
  }

  test("NULL / 3VL on inequality column is preserved") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin

    assertRuleFired(sql)
    val (on, off) = runBoth(sql)
    // k=4 (v={70,NULL}) and k=5 (v={NULL,NULL}) do not satisfy plain SQL <>.
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
    assert(off == on, s"NULL/3VL semantics diverge between rewrite ON and OFF: $on vs $off")
  }

  test("NULL equi-key is filtered before aggregation for NOT IN") {
    createTable(
      "TN",
      "k INT, v INT",
      """  (CAST(NULL AS INT), 10),
        |  (CAST(NULL AS INT), 20),
        |  (1, 10), (1, 20),
        |  (2, 30)""".stripMargin
    )
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW OuterKeys AS SELECT * FROM VALUES
        |  (1), (2), (3) AS OuterKeys(k)""".stripMargin)
    val sql =
      """SELECT k FROM OuterKeys o WHERE k NOT IN (
        |  SELECT s1.k FROM TN s1 JOIN TN s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin

    assertRuleFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"NULL equi-key NOT IN semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(2), Row(3)), s"expected {2,3}, got $on")
  }

  test("Swapped aliases must not be treated as the same self-join columns") {
    // The guard under test is `sameOutputPosition`. Both queries alias the same two base columns
    // to the names `k` and `v` on both sides, so a rule that compares attribute names would fire
    // on both; only the output ordinal tells them apart. The control fires, which is what makes
    // the negative case evidence that the ordinal check -- not a structural mismatch -- rejected
    // the swapped one.
    createTable("AliasBase", "a INT, b INT", "  (1, 10), (1, 20), (2, 30)")

    val alignedSql =
      """SELECT a FROM AliasBase outer_t WHERE a IN (
        |  SELECT s1.k
        |  FROM (SELECT a AS k, b AS v FROM AliasBase) s1
        |  JOIN (SELECT a AS k, b AS v FROM AliasBase) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(alignedSql)
    val (alignedOn, alignedOff) = runBoth(alignedSql)
    assert(
      alignedOn == alignedOff,
      s"aligned-alias control diverges: ON=$alignedOn OFF=$alignedOff")
    assert(alignedOn == Set(Row(1)), s"aligned-alias control expected {1}, got $alignedOn")

    // s1.k is `a` (output position 0) but s2.k is `b` (output position 1): same name, different
    // column. Rewriting this would count distinct `b` per `a`, which is a different query.
    val swappedSql =
      """SELECT a FROM AliasBase outer_t WHERE a IN (
        |  SELECT s1.k
        |  FROM (SELECT a AS k, b AS v FROM AliasBase) s1
        |  JOIN (SELECT a AS v, b AS k FROM AliasBase) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(swappedSql)
    val (on, off) = runBoth(swappedSql)
    assert(on == off, s"swapped-alias semantics diverge: ON=$on OFF=$off")
    assert(on.isEmpty, s"swapped-alias baseline should be empty, got $on")
  }

  test("Two different relations with the same schema must not be treated as a self-join") {
    // The guard under test is `isSameBaseRelation`: it must reject a join between two DIFFERENT
    // base tables even when they share a schema and column names. Distinct Parquet tables
    // canonicalize to distinct `rootPaths`, so `left.canonicalized == right.canonicalized` is
    // false and the rewrite must not fire. This pins a real correctness boundary, not just a
    // missed optimization: rewriting `TLeft JOIN TRight` as COUNT(DISTINCT) over TLeft alone would
    // drop TRight's rows and change the answer, so removing the guard would make ON diverge from
    // OFF here.
    createTable("TLeft", "k INT, v INT", "  (1, 10), (1, 10), (2, 30)")
    createTable("TRight", "k INT, v INT", "  (1, 20), (1, 20), (2, 30)")
    val sql =
      """SELECT k FROM TLeft outer_t WHERE k IN (
        |  SELECT s1.k FROM TLeft s1 JOIN TRight s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"different-relation join semantics diverge: ON=$on OFF=$off")
    // k=1: TLeft v={10} vs TRight v={20} -> 10<>20 true -> qualifies; k=2: 30<>30 false -> no.
    assert(on == Set(Row(1)), s"expected {1}, got $on")
  }

  // ==================== Positive: rewritten plan is structurally the aggregate ================
  //
  // Result parity (ON == OFF) proves the two queries return the same rows; it does not prove the
  // rewrite produced the specific GROUP BY + HAVING COUNT(DISTINCT) > 1 shape rather than, say,
  // leaving the self-join and happening to agree. These controls compare the rewritten plan against
  // a hand-written aggregate SQL that spells out the intended shape, INCLUDING two `IS NOT NULL`
  // filters: on the equi-key (which the rewrite injects to preserve equi-join NULL semantics) and
  // on the neq column. `v IS NOT NULL` is semantically redundant for COUNT(DISTINCT v), which
  // already ignores NULL, but the original `s1.v <> s2.v` lets InferFiltersFromConstraints derive
  // `isnotnull(v)` and push it below the aggregate, so the equivalent SQL must include it to match
  // the shape the optimizer actually produces. The comparison itself is `compareCanonicalizedPlans`
  // (see its doc for why canonicalizing first, and disabling checkAnalysis, is required here).

  private def optimizedPlanWith(sql: String, rewrite: Boolean): LogicalPlan =
    withSQLConf(rewriteConf -> rewrite.toString) {
      spark.sql(sql).queryExecution.optimizedPlan
    }

  /**
   * Assert two optimized plans are structurally equal. Canonicalize first: the rewrite creates
   * fresh aliases, whose names and exprIds are cosmetic for this structural check, while the
   * Join-vs-Aggregate shape difference this asserts on survives canonicalization. `checkAnalysis`
   * is disabled because both plans are already analyzed and optimized, and a canonicalized plan is
   * not re-analyzable (its HAVING references a zeroed exprId), which the default would reject
   * before any comparison.
   */
  private def compareCanonicalizedPlans(actual: LogicalPlan, expected: LogicalPlan): Unit =
    comparePlans(actual.canonicalized, expected.canonicalized, checkAnalysis = false)

  test("Pattern A' rewritten plan is structurally the equivalent aggregate") {
    setupTable()
    val selfJoinSql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    val aggregateSql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT k FROM T WHERE k IS NOT NULL AND v IS NOT NULL
        |  GROUP BY k HAVING count(DISTINCT v) > 1)""".stripMargin

    val actual = optimizedPlanWith(selfJoinSql, rewrite = true)
    val expected = optimizedPlanWith(aggregateSql, rewrite = false)
    assert(ruleFired(actual), s"precondition: rewrite should fire:\n$actual")
    assert(!ruleFired(expected), s"precondition: expected plan is the hand-written aggregate")
    compareCanonicalizedPlans(actual, expected)
  }

  test("Pattern A2 rewritten plan is structurally the equivalent aggregate") {
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val selfJoinSql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT s1.k FROM T s1 JOIN T s2
        |             ON s1.k = s2.k AND s1.v <> s2.v) sj
        |  WHERE d.k = sj.k)""".stripMargin
    val aggregateSql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT k FROM T WHERE k IS NOT NULL AND v IS NOT NULL
        |             GROUP BY k HAVING count(DISTINCT v) > 1) sj
        |  WHERE d.k = sj.k)""".stripMargin

    val actual = optimizedPlanWith(selfJoinSql, rewrite = true)
    val expected = optimizedPlanWith(aggregateSql, rewrite = false)
    assert(ruleFired(actual), s"precondition: rewrite should fire:\n$actual")
    assert(!ruleFired(expected), s"precondition: expected plan is the hand-written aggregate")
    compareCanonicalizedPlans(actual, expected)
  }

  // ==================== Negative: rewrite must produce equivalent results (or bail) ==========

  test("Plain InnerJoin at top level: results unchanged (rewrite must not touch it)") {
    setupTable()
    val sql =
      """SELECT ws1.k FROM T ws1 JOIN T ws2
        |ON ws1.k = ws2.k AND ws1.v <> ws2.v""".stripMargin
    // Row-multiplicity matters here; using count() to catch any drop or dup.
    var onCount: Long = -1L
    var offCount: Long = -1L
    withSQLConf(rewriteConf -> "true") {
      onCount = spark.sql(sql).count()
    }
    withSQLConf(rewriteConf -> "false") {
      offCount = spark.sql(sql).count()
    }
    assert(
      onCount == offCount,
      s"plain InnerJoin row-count differs: rewrite=$onCount vs baseline=$offCount")
    assertRuleNotFired(sql)
  }

  test("IS DISTINCT FROM is rejected by the self-join condition parser") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v IS DISTINCT FROM s2.v)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"IS DISTINCT FROM semantics diverge: ON=$on OFF=$off")
    // Assert the full result, not just contains(4): unlike `<>`, `IS DISTINCT FROM` treats NULL as
    // a value, so k=4 (v={70,NULL}) qualifies alongside k=1, k=3 and k=6.
    assert(on == Set(Row(1), Row(3), Row(4), Row(6)), s"expected {1,3,4,6}, got $on")
    assertRuleNotFired(sql)
  }

  test("IsNotNull on a non-join column is rejected") {
    // The guard under test is the predicate parser: it accepts IsNotNull only on a column the
    // join condition already references, because such a predicate is implied by the equi-key or
    // the inequality and can be dropped, while IsNotNull(w) filters rows the aggregate would
    // otherwise count. The control is the same query without that one conjunct.
    createTable(
      "T3",
      "k INT, v INT, w INT",
      """  (1, 10, 100), (1, 20, 200),
        |  (2, 30, CAST(NULL AS INT)), (2, 40, CAST(NULL AS INT))""".stripMargin)

    val controlSql =
      """SELECT k FROM T3 outer_t WHERE k IN (
        |  SELECT s1.k FROM T3 s1 JOIN T3 s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"T3 control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1), Row(2)), s"T3 control expected {1,2}, got $controlOn")

    val sql =
      """SELECT k FROM T3 outer_t WHERE k IN (
        |  SELECT s1.k FROM T3 s1 JOIN T3 s2
        |    ON s1.k = s2.k AND s1.v <> s2.v AND s1.w IS NOT NULL)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"IsNotNull(non-join-col) semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(1)), s"expected {1}, got $on")
  }

  test("Multiple inequality columns are rejected") {
    // The guard under test is `neqPairs.size != 1`. Two inequalities need "at least two rows
    // differing in v AND in w", which no count-distinct over a single column can express. The
    // control is the same query with only the first inequality.
    createTable(
      "T2",
      "k INT, v INT, w INT",
      """  (1, 10, 100), (1, 20, 200),
        |  (2, 30, 300),
        |  (3, 40, 100), (3, 50, 100)""".stripMargin)

    val controlSql =
      """SELECT k FROM T2 outer_t WHERE k IN (
        |  SELECT s1.k FROM T2 s1 JOIN T2 s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"T2 control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1), Row(3)), s"T2 control expected {1,3}, got $controlOn")

    val sql =
      """SELECT k FROM T2 outer_t WHERE k IN (
        |  SELECT s1.k FROM T2 s1 JOIN T2 s2
        |    ON s1.k = s2.k AND s1.v <> s2.v AND s1.w <> s2.w)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"multi-column neq semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(1)), s"expected {1}, got $on")
  }

  test("LeftOuter join is outside existence context: results unchanged") {
    setupTable()
    val sql =
      """SELECT ws1.k FROM T ws1 LEFT OUTER JOIN T ws2
        |ON ws1.k = ws2.k AND ws1.v <> ws2.v""".stripMargin
    // Row multiplicity matters here.
    var onCount: Long = -1L
    var offCount: Long = -1L
    withSQLConf(rewriteConf -> "true") {
      onCount = spark.sql(sql).count()
    }
    withSQLConf(rewriteConf -> "false") {
      offCount = spark.sql(sql).count()
    }
    assert(onCount == offCount, s"LeftOuter row-count differs: $onCount vs $offCount")
    assertRuleNotFired(sql)
  }

  test("Inequality column overlapping an equi-key is rejected") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.k <> s2.k)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"unsatisfiable predicate diverges: ON=$on OFF=$off")
    assert(on.isEmpty, s"unsatisfiable predicate should produce empty set, got $on")
    assertRuleNotFired(sql)
  }

  test("Config gate: rewrite disabled leaves a valid A' candidate untouched") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    withSQLConf(rewriteConf -> "false") {
      val plan = spark.sql(sql).queryExecution.optimizedPlan
      assert(!ruleFired(plan), s"config off must not fire rewrite:\n$plan")
      val res = spark.sql(sql).collect().toSet
      assert(res == Set(Row(1), Row(3), Row(6)), s"config off correctness broken: $res")
    }
  }

  // ==================== Correlated subquery: rule must fail-closed ====================

  private def setupOuterT(): Unit = {
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW OuterT AS SELECT * FROM VALUES
        |  (1), (3), (6) AS OuterT(k)""".stripMargin)
  }

  test("Correlated InSubquery is fail-closed") {
    setupTable()
    setupOuterT()
    val sql =
      """SELECT o.k FROM OuterT o WHERE o.k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v
        |  WHERE s2.k = o.k)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"correlated IN parity: ON=$on OFF=$off")
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
    assertRuleNotFired(sql)
  }

  // ==================== Repeatability whitelist: unknown operators fail-closed ==============

  test("Aggregate (first) inside subquery breaks row-bag repeatability: rule bails out") {
    // FIRST() is order-dependent and its aggregate result is not row-bag repeatable across
    // two evaluations, yet Catalyst's Expression.deterministic returns true. The whitelist
    // in `isRowBagRepeatable` must reject any Aggregate node inside the subquery plan.
    // range(...) avoids ConvertToLocalRelation folding the Aggregate away.
    val sql =
      """SELECT k FROM (SELECT CAST(id AS INT) AS k, CAST(id AS INT) AS v FROM range(100)) t
        |WHERE k IN (
        |  SELECT s1.k FROM (
        |      SELECT CAST(id % 10 AS INT) AS k, first(CAST(id AS INT)) AS v
        |      FROM range(200) GROUP BY id % 10
        |    ) s1
        |  JOIN (
        |      SELECT CAST(id % 10 AS INT) AS k, first(CAST(id AS INT)) AS v
        |      FROM range(200) GROUP BY id % 10
        |    ) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
  }

  test("Window (row_number) inside subquery breaks row-bag repeatability: rule bails out") {
    // ROW_NUMBER over non-total order breaks ties nondeterministically. Whitelist rejects
    // any Window node inside the subquery plan.
    val sql =
      """SELECT k FROM (SELECT CAST(id AS INT) AS k, CAST(id AS INT) AS v FROM range(100)) t
        |WHERE k IN (
        |  SELECT s1.k FROM (
        |      SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY grp) AS v
        |      FROM (SELECT CAST(id % 10 AS INT) AS k, CAST(id % 3 AS INT) AS grp FROM range(200))
        |    ) s1
        |  JOIN (
        |      SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY grp) AS v
        |      FROM (SELECT CAST(id % 10 AS INT) AS k, CAST(id % 3 AS INT) AS grp FROM range(200))
        |    ) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
  }

  test("Nondeterministic self-join input is rejected") {
    // The guard under test is `plan.deterministic` inside `isRepeatablePlan`. Both sides use the
    // same explicit seed, so the two subplans do have the same canonical shape and the rejection
    // cannot come from `isSameBaseRelation`. The control replaces `rand(41) < 0.5` with a
    // deterministic filter and nothing else, proving this Range/Filter/Project shape does reach
    // the rewrite.
    val controlSql =
      """SELECT k FROM (SELECT CAST(id AS INT) AS k, CAST(id AS INT) AS v FROM range(100)) t
        |WHERE k IN (
        |  SELECT s1.k FROM (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE id % 2 = 0
        |    ) s1
        |  JOIN (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE id % 2 = 0
        |    ) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"range control diverges: ON=$controlOn OFF=$controlOff")
    assert(
      controlOn == Set(Row(0), Row(2), Row(4), Row(6), Row(8)),
      s"range control expected the even keys, got $controlOn")

    val sql =
      """SELECT k FROM (SELECT CAST(id AS INT) AS k, CAST(id AS INT) AS v FROM range(100)) t
        |WHERE k IN (
        |  SELECT s1.k FROM (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE rand(41) < 0.5
        |    ) s1
        |  JOIN (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE rand(41) < 0.5
        |    ) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
  }

  test("LogicalRDD leaf is not a trusted repeatable source: rule bails out") {
    // The guard under test is the leaf allowlist in `isRowBagRepeatable`: a Parquet
    // `LogicalRelation` is trusted, but a `LogicalRDD` (createDataFrame over an RDD) wraps an
    // arbitrary RDD lineage whose runtime row bag Catalyst cannot prove repeatable, so it must
    // fail closed even though `plan.deterministic` is true. Both fixtures are MultiInstanceRelation
    // leaves, so each self-join dedups into two structurally identical sides without a rename-only
    // Project -- the rejection therefore comes from the leaf allowlist, not `isSameBaseRelation`.
    // The Parquet control uses the same schema, data and query shape and fires, which is what makes
    // the LogicalRDD negative evidence that the leaf allowlist -- not a structural mismatch --
    // rejected it.
    createTable("RddCtl", "k INT, v INT", "  (1, 10), (1, 20), (2, 30)")
    val controlSql =
      """SELECT k FROM RddCtl outer_t WHERE k IN (
        |  SELECT s1.k FROM RddCtl s1 JOIN RddCtl s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"Parquet control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1)), s"Parquet control expected {1}, got $controlOn")

    val schema = StructType(Seq(StructField("k", IntegerType), StructField("v", IntegerType)))
    val rows = spark.sparkContext.parallelize(Seq(Row(1, 10), Row(1, 20), Row(2, 30)))
    spark.createDataFrame(rows, schema).createOrReplaceTempView("RddT")
    val sql =
      """SELECT k FROM RddT outer_t WHERE k IN (
        |  SELECT s1.k FROM RddT s1 JOIN RddT s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"LogicalRDD semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(1)), s"expected {1}, got $on")
  }

  test("Non-allowlisted deterministic expression (Abs) fails closed") {
    // The guard under test is the expression allowlist in `isRepeatableExpression`: it trusts
    // expression TYPES, not merely `deterministic`. Abs is deterministic, but is intentionally not
    // yet part of the expression allowlist; until its repeatability contract is explicitly admitted
    // there, a self-join whose side projects abs(v) fails closed -- a missed optimization, not a
    // correctness bug. This test verifies that an unknown-but-deterministic expression is not
    // silently let through by `plan.deterministic`.
    //
    // Control and negative are a single-variable pair: both project one derived column and differ
    // ONLY in its expression. The control uses `v + 1` (Add over Attribute + Literal, all
    // allowlisted) and fires; wrapping the same column in abs() -- the sole change -- makes it not
    // fire, so the rejection is attributable to the expression allowlist rather than a structural
    // mismatch. `+ 1` (not `+ 0`) is used so the Add survives arithmetic simplification and the
    // control genuinely exercises a compound allowlisted expression. v is INT here, so the Add is a
    // plain `Add(v, 1)` with no decimal PromotePrecision / CheckOverflow wrappers.
    setupTable()

    val controlSql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k
        |  FROM (SELECT k, v + 1 AS x FROM T) s1
        |  JOIN (SELECT k, v + 1 AS x FROM T) s2
        |    ON s1.k = s2.k AND s1.x <> s2.x)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"Add control diverges: ON=$controlOn OFF=$controlOff")
    // v+1 is injective over the (non-null) v values, so distinctness per k is unchanged: {1,3,6}.
    assert(
      controlOn == Set(Row(1), Row(3), Row(6)),
      s"Add control expected {1,3,6}, got $controlOn")

    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k
        |  FROM (SELECT k, abs(v) AS x FROM T) s1
        |  JOIN (SELECT k, abs(v) AS x FROM T) s2
        |    ON s1.k = s2.k AND s1.x <> s2.x)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"Abs-projected self-join semantics diverge: ON=$on OFF=$off")
  }

  // ==================== Data-type safety: comparison vs grouping/DISTINCT equality ============
  //
  // The rewrite turns `<>` into COUNT(DISTINCT) and `=` into GROUP BY, so it is only sound on
  // types where SQL comparison equality coincides with grouping/DISTINCT equality.
  // `parseSelfJoinCondition` gates BOTH the neq column and every equi-key through
  // `isSafeComparisonGroupingType` (a positive allowlist, not `RowOrdering.isOrderable`). These
  // tests pin the boundary for the risky types.

  test("Float/Double neq column is rejected (comparison-vs-DISTINCT contract, defensive)") {
    // The guard under test is `isSafeComparisonGroupingType` on the NEQ column. Control and
    // negative differ only in which column feeds the inequality: `vi` (Int, allowlisted) fires,
    // `vd` (Double) does not. Double fails closed defensively: the rewrite depends on comparison
    // equality and grouping/DISTINCT equality agreeing, and for floating point that agreement on
    // signed zero and NaN rests on normalization details (NormalizeFloatingNumbers) that need not
    // match across Spark versions or native backends. On current Spark, comparison semantics and
    // aggregation normalization are aligned for these cases, so the OFF baseline of the negative
    // query is {1}; the rule does not fire, so ON matches it. The test pins fail-closed behavior,
    // not a divergence in current Spark.
    createTable(
      "TFloat",
      "k INT, vi INT, vd DOUBLE",
      """  (1, 10, 1.0), (1, 20, 2.0),
        |  (2, 30, 0.0), (2, 30, -0.0),
        |  (3, 40, CAST('NaN' AS DOUBLE)), (3, 50, CAST('NaN' AS DOUBLE))""".stripMargin)

    val controlSql =
      """SELECT k FROM TFloat outer_t WHERE k IN (
        |  SELECT s1.k FROM TFloat s1 JOIN TFloat s2
        |    ON s1.k = s2.k AND s1.vi <> s2.vi)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"Int-neq control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1), Row(3)), s"Int-neq control expected {1,3}, got $controlOn")

    val sql =
      """SELECT k FROM TFloat outer_t WHERE k IN (
        |  SELECT s1.k FROM TFloat s1 JOIN TFloat s2
        |    ON s1.k = s2.k AND s1.vd <> s2.vd)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"Double-neq semantics diverge: ON=$on OFF=$off")
    // k=1 matches (1.0 <> 2.0); k=2 does not (0.0 = -0.0); k=3 does not (Spark NaN = NaN).
    assert(on == Set(Row(1)), s"Double-neq baseline expected {1}, got $on")
  }

  test("Float/Double equi-key is rejected (defensive fail-closed)") {
    // The guard under test is `isSafeComparisonGroupingType` on the EQUI key. Current Spark aligns
    // floating-point comparison semantics with grouping normalization here, but the rule does not
    // depend on that implementation contract, so it fails closed. Control and negative differ only
    // in the equi-key column: `ki` (Int) fires, `kd` (Double) does not.
    createTable(
      "TFloatKey",
      "kd DOUBLE, ki INT, v INT",
      """  (1.0, 1, 10), (1.0, 1, 20),
        |  (2.0, 2, 30),
        |  (0.0, 3, 40), (-0.0, 3, 50)""".stripMargin)

    val controlSql =
      """SELECT ki FROM TFloatKey outer_t WHERE ki IN (
        |  SELECT s1.ki FROM TFloatKey s1 JOIN TFloatKey s2
        |    ON s1.ki = s2.ki AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"Int-key control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1), Row(3)), s"Int-key control expected {1,3}, got $controlOn")

    val sql =
      """SELECT kd FROM TFloatKey outer_t WHERE kd IN (
        |  SELECT s1.kd FROM TFloatKey s1 JOIN TFloatKey s2
        |    ON s1.kd = s2.kd AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"Double-key semantics diverge: ON=$on OFF=$off")
  }

  test("Complex-type neq column (array/struct) is rejected wholesale") {
    // The guard under test is the wholesale rejection of complex types by
    // `isSafeComparisonGroupingType`, which also covers any Float/Double nested inside them.
    // Control (`v` Int) fires; the ARRAY<DOUBLE> and STRUCT<..DOUBLE> variants -- identical query
    // shape, only the neq column changed -- do not.
    createTable(
      "TCplx",
      "k INT, v INT, a ARRAY<DOUBLE>, s STRUCT<x: INT, y: DOUBLE>",
      """  (1, 10, ARRAY(1.0), NAMED_STRUCT('x', 1, 'y', 1.0)),
        |  (1, 20, ARRAY(2.0), NAMED_STRUCT('x', 2, 'y', 2.0)),
        |  (2, 30, ARRAY(1.0), NAMED_STRUCT('x', 1, 'y', 1.0))""".stripMargin)

    val controlSql =
      """SELECT k FROM TCplx outer_t WHERE k IN (
        |  SELECT s1.k FROM TCplx s1 JOIN TCplx s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"Int-neq control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1)), s"Int-neq control expected {1}, got $controlOn")

    val arraySql =
      """SELECT k FROM TCplx outer_t WHERE k IN (
        |  SELECT s1.k FROM TCplx s1 JOIN TCplx s2
        |    ON s1.k = s2.k AND s1.a <> s2.a)""".stripMargin
    assertRuleNotFired(arraySql)
    val (arrayOn, arrayOff) = runBoth(arraySql)
    assert(arrayOn == arrayOff, s"array-neq semantics diverge: ON=$arrayOn OFF=$arrayOff")

    val structSql =
      """SELECT k FROM TCplx outer_t WHERE k IN (
        |  SELECT s1.k FROM TCplx s1 JOIN TCplx s2
        |    ON s1.k = s2.k AND s1.s <> s2.s)""".stripMargin
    assertRuleNotFired(structSql)
    val (structOn, structOff) = runBoth(structSql)
    assert(structOn == structOff, s"struct-neq semantics diverge: ON=$structOn OFF=$structOff")
  }

  test("String neq/equi key: default (UTF8_BINARY) fires, non-binary collation fails closed") {
    // The guard under test is the StringType branch of `isSafeComparisonGroupingType`: only
    // `supportsBinaryEquality` (byte-wise) strings are admitted, because a non-binary collation
    // (Spark 4.0+) routes comparison and grouping through different code paths. The control uses a
    // default-collation table for BOTH the equi key and the neq column and fires -- proving plain
    // strings are not rejected wholesale.
    //
    // The two negatives collate exactly ONE column each, so each pins the rejection to a specific
    // gate: a UTF8_LCASE NEQ column exercises the neq-side check, a UTF8_LCASE EQUI key exercises
    // the equi-side check. Collating both at once would leave it ambiguous which gate fired and
    // stay green if either were deleted -- mirroring the split Float neq / Float equi coverage.
    createTable(
      "TStrBin",
      "k STRING, v STRING",
      """  ('a', 'x'), ('a', 'y'),
        |  ('b', 'z')""".stripMargin)
    val binSql =
      """SELECT k FROM TStrBin outer_t WHERE k IN (
        |  SELECT s1.k FROM TStrBin s1 JOIN TStrBin s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(binSql)
    val (binOn, binOff) = runBoth(binSql)
    assert(binOn == binOff, s"binary-string control diverges: ON=$binOn OFF=$binOff")
    assert(binOn == Set(Row("a")), s"binary-string control expected {a}, got $binOn")

    // Negative 1: only the NEQ column is non-binary collated -> neq-side type gate rejects.
    createTable(
      "TStrCiNeq",
      "k STRING, v STRING COLLATE UTF8_LCASE",
      """  ('a', 'x'), ('a', 'y'),
        |  ('b', 'z')""".stripMargin)
    val ciNeqSql =
      """SELECT k FROM TStrCiNeq outer_t WHERE k IN (
        |  SELECT s1.k FROM TStrCiNeq s1 JOIN TStrCiNeq s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(ciNeqSql)
    val (neqOn, neqOff) = runBoth(ciNeqSql)
    assert(neqOn == neqOff, s"collated-neq semantics diverge: ON=$neqOn OFF=$neqOff")

    // Negative 2: only the EQUI key is non-binary collated -> equi-side type gate rejects.
    createTable(
      "TStrCiEqui",
      "k STRING COLLATE UTF8_LCASE, v STRING",
      """  ('a', 'x'), ('a', 'y'),
        |  ('b', 'z')""".stripMargin)
    val ciEquiSql =
      """SELECT k FROM TStrCiEqui outer_t WHERE k IN (
        |  SELECT s1.k FROM TStrCiEqui s1 JOIN TStrCiEqui s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(ciEquiSql)
    val (equiOn, equiOff) = runBoth(ciEquiSql)
    assert(equiOn == equiOff, s"collated-equi semantics diverge: ON=$equiOn OFF=$equiOff")
  }

  test("CHAR/VARCHAR join keys are rejected via declared-type metadata") {
    // The guard under test is isSafeComparisonGroupingAttribute: CHAR/VARCHAR table columns reach
    // the optimizer as annotated StringType (CharVarcharUtils records the declared type in the
    // attribute metadata), so a dataType-only check would admit them through the StringType branch.
    // The rule recovers the declared raw type from the metadata and fails closed. Control: a plain
    // STRING table -- identical query shape and data -- fires, proving strings are not rejected
    // wholesale; the CHAR(5) and VARCHAR(5) variants, differing only in the declared column type,
    // do not. Both the equi key `k` and the neq column `v` are CHAR/VARCHAR, so both gate paths are
    // pinned.
    createTable(
      "TStr",
      "k STRING, v STRING",
      """  ('a', 'x'), ('a', 'y'),
        |  ('b', 'z')""".stripMargin)
    val stringSql =
      """SELECT k FROM TStr outer_t WHERE k IN (
        |  SELECT s1.k FROM TStr s1 JOIN TStr s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(stringSql)
    val (strOn, strOff) = runBoth(stringSql)
    assert(strOn == strOff, s"string control diverges: ON=$strOn OFF=$strOff")
    assert(strOn == Set(Row("a")), s"string control expected {a}, got $strOn")

    Seq("CHAR(5)", "VARCHAR(5)").foreach { keyType =>
      createTable(
        "TCharVarchar",
        s"k $keyType, v $keyType",
        """  ('a', 'x'), ('a', 'y'),
          |  ('b', 'z')""".stripMargin)
      val sql =
        """SELECT k FROM TCharVarchar outer_t WHERE k IN (
          |  SELECT s1.k FROM TCharVarchar s1 JOIN TCharVarchar s2
          |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
      assertRuleNotFired(sql)
      val (on, off) = runBoth(sql)
      assert(on == off, s"$keyType not-fired query diverges: ON=$on OFF=$off")
    }
  }

  test("ANSI: rewrite preserves observable error behavior (throw-or-succeed parity)") {
    // The guard under test is not a rejection but a parity property: the expression allowlist
    // admits Cast, which can throw under ANSI. The rewrite evaluates the projected `CAST(s AS INT)`
    // once per row inside the Aggregate, while the baseline self-join evaluates it per row on each
    // side -- the same set of rows either way -- so a malformed value must make BOTH forms behave
    // the same. k=2 holds a non-numeric 's'.
    //
    // Parity is checked as observable behavior, not just "an exception happened": both succeed with
    // equal rows, or both throw with the same error class. A one-sided throw is a blocker.
    createTable(
      "TAnsi",
      "k INT, s STRING",
      """  (1, '10'), (1, '20'),
        |  (2, '30'), (2, 'xyz')""".stripMargin)
    val sql =
      """SELECT k FROM TAnsi outer_t WHERE k IN (
        |  SELECT s1.k
        |  FROM (SELECT k, CAST(s AS INT) AS x FROM TAnsi) s1
        |  JOIN (SELECT k, CAST(s AS INT) AS x FROM TAnsi) s2
        |    ON s1.k = s2.k AND s1.x <> s2.x)""".stripMargin

    Seq("false", "true").foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        // The rule still fires at plan level regardless of ANSI (the cast throws only at runtime).
        assertRuleFired(sql)
        val on = runOutcome(sql, rewrite = true)
        val off = runOutcome(sql, rewrite = false)
        (on, off) match {
          case (Right(onRows), Right(offRows)) =>
            assert(onRows == offRows,
              s"ANSI=$ansi both succeeded but diverged: ON=$onRows OFF=$offRows")
          case (Left(onErr), Left(offErr)) =>
            assert(onErr == offErr,
              s"ANSI=$ansi both threw but different error: ON=$onErr OFF=$offErr")
          case _ =>
            fail(s"ANSI=$ansi one-sided error behavior: ON=$on OFF=$off")
        }
      }
    }
  }

  test("ANSI: Remainder neq column preserves error behavior; Divide is type-gated") {
    // Cast is not the only allowlisted expression that can throw under ANSI. This pins the two
    // arithmetic ops the review asked about:
    //   - Remainder (`%`) on INT operands stays INT, an allowlisted type, so the rule fires. It can
    //     raise DIVIDE_BY_ZERO under ANSI, so it exercises the throw-parity property directly, not
    //     resting it on the Cast test alone: the rewrite evaluates `v % w` once per row inside
    //     the Aggregate and the baseline self-join evaluates it per row on each side -- the same
    //     rows -- so a `% 0` must make BOTH forms behave identically.
    //   - Divide (`/`) is admitted by the expression allowlist (`isRepeatableExpression` trusts it
    //     wherever it appears in the scanned plan), but a Divide-derived NEQ KEY is stopped one
    //     layer later by the type guard: `/` always widens INT operands to a Double result, which
    //     `isSafeComparisonGroupingType` rejects, so a `v / w` neq column never fires. There is
    //     thus no runtime Divide path to check for parity; the type gate stops it first. (Decimal
    //     operands would keep a Decimal result but wrap the Divide in CheckOverflow, which the
    //     expression allowlist rejects, so a Decimal `/` neq key has no firing path.) The two
    //     layers are independent: allowlisting Divide as repeatable is not the same as admitting a
    //     Divide result as a safe comparison/grouping key.
    // Row (2, 30, 0) forces `30 % 0`, which throws under ANSI and yields NULL otherwise.
    createTable(
      "TAnsiDiv",
      "k INT, v INT, w INT",
      """  (1, 10, 3), (1, 20, 7),
        |  (2, 30, 0), (2, 40, 4)""".stripMargin)

    // Divide: Double result -> rejected by the data-type allowlist regardless of ANSI.
    val divSql =
      """SELECT k FROM TAnsiDiv outer_t WHERE k IN (
        |  SELECT s1.k
        |  FROM (SELECT k, v / w AS x FROM TAnsiDiv) s1
        |  JOIN (SELECT k, v / w AS x FROM TAnsiDiv) s2
        |    ON s1.k = s2.k AND s1.x <> s2.x)""".stripMargin
    assertRuleNotFired(divSql)

    // Remainder: INT result -> fires; assert throw-or-succeed parity under ANSI off and on.
    val remSql =
      """SELECT k FROM TAnsiDiv outer_t WHERE k IN (
        |  SELECT s1.k
        |  FROM (SELECT k, v % w AS x FROM TAnsiDiv) s1
        |  JOIN (SELECT k, v % w AS x FROM TAnsiDiv) s2
        |    ON s1.k = s2.k AND s1.x <> s2.x)""".stripMargin
    Seq("false", "true").foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        // The rule fires at plan level regardless of ANSI (the remainder throws only at runtime).
        assertRuleFired(remSql)
        val on = runOutcome(remSql, rewrite = true)
        val off = runOutcome(remSql, rewrite = false)
        (on, off) match {
          case (Right(onRows), Right(offRows)) =>
            assert(onRows == offRows,
              s"Remainder ANSI=$ansi both succeeded but diverged: ON=$onRows OFF=$offRows")
            // Positive signal (ANSI off): the remainders are defined, so the rewrite must return
            // the real membership. k=1: 10%3=1, 20%7=6 -> {1,6} matches; k=2: 30%0=NULL, 40%4=0
            // -> {0} no match. Result is exactly {1}.
            if (ansi == "false") {
              assert(onRows == Set(Row(1)), s"Remainder ANSI=false expected {1}, got $onRows")
            }
          case (Left(onErr), Left(offErr)) =>
            assert(onErr == offErr,
              s"Remainder ANSI=$ansi both threw but different error: ON=$onErr OFF=$offErr")
          case _ =>
            fail(s"Remainder ANSI=$ansi one-sided error behavior: ON=$on OFF=$off")
        }
      }
    }
  }

  /**
   * Run `sql` with the rewrite on/off and capture observable behavior: `Right(rows)` on success or
   * `Left(errorClass)` if execution throws. Used to assert the rewrite does not change whether, or
   * with what error, a query fails (e.g. under ANSI).
   */
  private def runOutcome(sql: String, rewrite: Boolean): Either[String, Set[Row]] = {
    withSQLConf(rewriteConf -> rewrite.toString) {
      try {
        Right(spark.sql(sql).collect().toSet)
      } catch {
        case e: SparkThrowable => Left(Option(e.getCondition).getOrElse(e.getClass.getName))
        case e: Throwable => Left(e.getClass.getName)
      }
    }
  }
}
