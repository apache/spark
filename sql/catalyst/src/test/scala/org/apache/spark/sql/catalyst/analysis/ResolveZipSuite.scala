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
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class ResolveZipSuite extends AnalysisTest {

  private val base = LocalRelation($"a".int, $"b".int, $"c".int)

  object Resolve extends RuleExecutor[LogicalPlan] {
    override val batches: Seq[Batch] = Seq(
      Batch("ResolveZip", Once, ResolveZip))
  }

  private def reachesBase(plan: LogicalPlan, expectedBase: LogicalPlan): Boolean = plan match {
    case Project(_, child) => reachesBase(child, expectedBase)
    case other => other eq expectedBase
  }

  test("resolve Zip: both sides have Project over same base") {
    val left = Project(Seq(base.output(0)), base)
    val right = Project(Seq(base.output(1)), base)
    val zip = Zip(left, right)

    val resolved = Resolve.execute(zip)
    val expected = Project(Seq(base.output(0), base.output(1)), base)
    comparePlans(resolved, expected)
  }

  test("resolve Zip: left is bare plan, right has Project") {
    val right = Project(Seq(base.output(0)), base)
    val zip = Zip(base, right)

    val resolved = Resolve.execute(zip)
    val expected = Project(base.output ++ Seq(base.output(0)), base)
    comparePlans(resolved, expected)
  }

  test("resolve Zip: both sides are bare same plan") {
    val zip = Zip(base, base)

    val resolved = Resolve.execute(zip)
    val expected = Project(base.output ++ base.output, base)
    comparePlans(resolved, expected)
  }

  test("resolve Zip: both sides have expressions over same base") {
    val left = base.select(($"a" + 1).as("a_plus_1"))
    val right = base.select(($"b" * 2).as("b_times_2"))
    val zip = Zip(left.analyze, right.analyze)

    val resolved = Resolve.execute(zip)
    assert(!resolved.isInstanceOf[Zip], "Zip should have been resolved to a Project")
    assert(resolved.isInstanceOf[Project])
    assert(resolved.output.length == 2)
    assert(resolved.output(0).name == "a_plus_1")
    assert(resolved.output(1).name == "b_times_2")
  }

  test("resolve Zip: different base plans - Zip remains unresolved") {
    val base2 = LocalRelation($"x".int, $"y".int, $"z".int, $"w".int)
    val left = Project(Seq(base.output(0)), base)
    val right = Project(Seq(base2.output(0)), base2)
    val zip = Zip(left, right)

    val resolved = Resolve.execute(zip)
    // ResolveZip cannot merge, so Zip stays
    assert(resolved.isInstanceOf[Zip])
  }

  test("resolve Zip: skipped when children are unresolved") {
    val unresolvedChild = Project(
      Seq(UnresolvedAttribute("a")),
      UnresolvedRelation(Seq("t")))
    val zip = Zip(unresolvedChild, unresolvedChild)

    val result = Resolve.execute(zip)
    // Zip should remain unchanged because children are not resolved
    assert(result.isInstanceOf[Zip])
  }

  test("CheckAnalysis: different base plans throws ZIP_PLANS_NOT_MERGEABLE") {
    val base2 = LocalRelation($"x".int, $"y".int, $"z".int, $"w".int)
    val left = Project(Seq(base.output(0)), base)
    val right = Project(Seq(base2.output(0)), base2)
    val zip = Zip(left, right)

    assertAnalysisErrorCondition(
      zip,
      expectedErrorCondition = "ZIP_PLANS_NOT_MERGEABLE",
      expectedMessageParameters = Map.empty
    )
  }

  test("resolve Zip: longer chain of selects on both sides") {
    // Left has 3 nested Projects, right has 1 Project. Both reach the same base.
    val left = Project(Seq(base.output(0)),
      Project(Seq(base.output(0), base.output(1)),
        Project(base.output, base)))
    val right = Project(Seq(base.output(1)), base)
    val zip = Zip(left, right)

    val resolved = Resolve.execute(zip)
    assert(resolved.isInstanceOf[Project], "Asymmetric chain should still merge to a Project")
    assert(resolved.output.map(_.name) == Seq("a", "b"))
  }

  test("resolve Zip: chained Project with aliases composes substitutions") {
    // Build df.select(a + 1 AS x).select(x * 2 AS y) -- outer references the inner alias.
    val inner = base.select(($"a" + 1).as("x"))
    val outer = inner.select(($"x" * 2).as("y")).analyze
    val right = base.select(($"b" * 3).as("z")).analyze
    val zip = Zip(outer, right)

    val resolved = Resolve.execute(zip)
    assert(resolved.isInstanceOf[Project], "Aliased chain should still merge to a Project")
    assert(reachesBase(resolved, base),
      "Resolved plan should be a Project chain rooted at the shared base")
    assert(resolved.output.map(_.name) == Seq("y", "z"))
  }

  test("resolve Zip: different-instance bases with same canonical plan") {
    // Two LocalRelations with the same schema but distinct exprIds. `sameResult` matches
    // (canonicalized plans are equal), so this is the only path where `attrMapping` actually
    // remaps right-side references.
    val baseB = LocalRelation($"a".int, $"b".int, $"c".int)
    val left = Project(Seq(base.output(0)), base)
    val right = baseB.select(($"a" + 1).as("a_plus_1")).analyze
    val zip = Zip(left, right)

    val resolved = Resolve.execute(zip)
    assert(resolved.isInstanceOf[Project])
    assert(reachesBase(resolved, base),
      "Resolved plan should be rooted at the left base, not the right base")
    assert(!resolved.exists(_ eq baseB), "Right base should be discarded after merge")
    assert(resolved.output.map(_.name) == Seq("a", "a_plus_1"))
  }

  test("CheckAnalysis: non-scalar Python UDF throws ZIP_PLANS_NOT_MERGEABLE") {
    // A GROUPED_MAP Python UDF in either side's projection breaks the 1:1 row mapping, so
    // ResolveZip refuses to merge and the surviving Zip must surface ZIP_PLANS_NOT_MERGEABLE
    // (rather than fall through to the generic unresolved-operator INTERNAL_ERROR).
    val groupedMapUdf = PythonUDF(
      "pyUDF",
      null,
      IntegerType,
      Seq(base.output(0)),
      PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
      udfDeterministic = true)
    val left = base.select(groupedMapUdf.as("x"))
    val right = base.select($"b".as("y"))
    val zip = Zip(left.analyze, right.analyze)

    assertAnalysisErrorCondition(
      zip,
      expectedErrorCondition = "ZIP_PLANS_NOT_MERGEABLE",
      expectedMessageParameters = Map.empty
    )
  }

  test("resolve Zip: stacked withColumn-style projections (multiple Project layers)") {
    // Emulate df.withColumn("d", a + 1).withColumn("e", b * 2) on left:
    // two passthrough-plus-alias Projects stacked, while right has a single layer.
    val left = base
      .select($"a", $"b", $"c", ($"a" + 1).as("d"))
      .select($"a", $"b", $"c", $"d", ($"b" * 2).as("e"))
      .analyze
    val right = base.select($"a", $"b", $"c", ($"c" + 100).as("f")).analyze
    val zip = Zip(left, right)

    val resolved = Resolve.execute(zip)
    assert(resolved.isInstanceOf[Project], "Stacked withColumn chain should merge to a Project")
    assert(reachesBase(resolved, base),
      "Resolved plan should be a Project chain rooted at the shared base")
    assert(resolved.output.map(_.name) == Seq("a", "b", "c", "d", "e", "a", "b", "c", "f"))
  }

  test("resolve Zip: shared-producer dedup preserves each side's output column name") {
    // Both sides project the same deterministic expression over the shared base, but under
    // different user-given names. The dedup must merge the producer but keep each side's name.
    val left = base.select($"a".as("x")).analyze
    val right = base.select($"a".as("y")).analyze
    val zip = Zip(left, right)

    val resolved = Resolve.execute(zip)
    assert(resolved.isInstanceOf[Project])
    assert(resolved.output.map(_.name) == Seq("x", "y"),
      s"schema should be [x, y] but got ${resolved.output.map(_.name)}")
  }
}
