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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, BindReferences, Collate,
  CreateArray, CustomNondetUnaryTestExpr, Expression, GetArrayItem, LambdaFunction, Literal,
  PythonUDF, Rand, ResolvedCollation, RowOrderIndepTestExpr, Substring,
  UnresolvedNamedLambdaVariable, UpdateFields}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, IntegerType, MetadataBuilder}

class CollapseProjectSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", FixedPoint(10), EliminateSubqueryAliases) ::
      Batch("CollapseProject", Once, CollapseProject) ::
      Batch("SimplifyExtractValueOps", Once, SimplifyExtractValueOps) ::
      Batch("ReplaceUpdateFieldsExpression", Once, ReplaceUpdateFieldsExpression) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int)

  test("collapse two deterministic, independent projects into one") {
    val query = testRelation
      .select(($"a" + 1).as("a_plus_1"), $"b")
      .select($"a_plus_1", ($"b" + 1).as("b_plus_1"))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.select(($"a" + 1).as("a_plus_1"),
      ($"b" + 1).as("b_plus_1")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two deterministic, dependent projects into one") {
    val query = testRelation
      .select(($"a" + 1).as("a_plus_1"), $"b")
      .select(($"a_plus_1" + 1).as("a_plus_2"), $"b")

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation.select(
      (($"a" + 1).as("a_plus_1") + 1).as("a_plus_2"),
      $"b").analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse nondeterministic projects") {
    val query = testRelation
      .select(Rand(10).as("rand"))
      .select(($"rand" + 1).as("rand1"), ($"rand" + 2).as("rand2"))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two nondeterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as("rand"))
      .select(Rand(20).as("rand2"))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(Rand(20).as("rand2")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse one nondeterministic, one deterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as("rand"), $"a")
      .select(($"a" + 1).as("a_plus_1"))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(($"a" + 1).as("a_plus_1")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse project into aggregate") {
    val query = testRelation
      .groupBy($"a", $"b")(($"a" + 1).as("a_plus_1"), $"b")
      .select($"a_plus_1", ($"b" + 1).as("b_plus_1"))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .groupBy($"a", $"b")(($"a" + 1).as("a_plus_1"), ($"b" + 1).as("b_plus_1"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse common nondeterministic project and aggregate") {
    val query = testRelation
      .groupBy($"a")($"a", Rand(10).as("rand"))
      .select(($"rand" + 1).as("rand1"), ($"rand" + 2).as("rand2"))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-36718: do not collapse project if non-cheap expressions will be repeated") {
    val query = testRelation
      .select(($"a" + 1).as("a_plus_1"))
      .select(($"a_plus_1" + $"a_plus_1").as("a_2_plus_2"))
      .analyze

    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("SPARK-39699: collapse project with collection creation expressions") {
    val struct = namedStruct(
      "a", $"a",
      "a_plus_1", $"a" + 1,
      "a_plus_2", $"a" + 2,
      "nested", namedStruct("inner1", $"a" + 3, "inner2", $"a" + 4)
    ).as("struct")
    val baseQuery = testRelation.select(struct)

    // Can collapse as there is only one non-cheap access: `struct.a_plus_1`
    val query1 = baseQuery
      .select(($"struct".getField("a") + $"struct".getField("a_plus_1")).as("add"))
      .analyze
    val optimized1 = Optimize.execute(query1)
    val expected1 = testRelation
      .select(($"a" + ($"a" + 1)).as("add"))
      .analyze
    comparePlans(optimized1, expected1)

    // Cannot collapse as there are two non-cheap accesses: `struct.a_plus_1` and `struct.a_plus_1`
    val query2 = baseQuery
      .select(($"struct".getField("a_plus_1") + $"struct".getField("a_plus_1")).as("add"))
      .analyze
    val optimized2 = Optimize.execute(query2)
    comparePlans(optimized2, query2)

    // Cannot collapse as there are two non-cheap accesses: `struct.a_plus_1` and `struct`
    val query3 = baseQuery
      .select($"struct".getField("a_plus_1"), $"struct")
      .analyze
    val optimized3 = Optimize.execute(query3)
    comparePlans(optimized3, query3)

    // Can collapse as there is only one non-cheap access: `struct`
    val query4 = baseQuery
      .select($"struct".getField("a"), $"struct")
      .analyze
    val optimized4 = Optimize.execute(query4)
    val expected4 = testRelation
      .select($"a".as("struct.a"), struct)
      .analyze
    comparePlans(optimized4, expected4)

    // Referenced by WithFields.
    val query5 = testRelation.select(namedStruct("a", $"a", "b", $"a" + 1).as("struct"))
      .select(UpdateFields($"struct", "c", $"struct".getField("a")).as("u"))
      .analyze
    val optimized5 = Optimize.execute(query5)
    val expected5 = testRelation
      .select(namedStruct("a", $"a", "b", $"a" + 1, "c", $"a").as("struct").as("u"))
      .analyze
    comparePlans(optimized5, expected5)

    // TODO: should collapse as the non-cheap accesses are distinct:
    //  `struct.a_plus_1` and `struct.a_plus_2`
    val query6 = baseQuery
      .select(($"struct".getField("a_plus_1") + $"struct".getField("a_plus_2")).as("add"))
      .analyze
    val optimized6 = Optimize.execute(query6)
    comparePlans(optimized6, query6)

    // Cannot collapse as the two non-cheap accesses have a lineage:
    // `struct.nested` and `struct.nested.inner1`
    val query7 = baseQuery
      .select($"struct".getField("nested"), $"struct".getField("nested").getField("inner1"))
      .analyze
    val optimized7 = Optimize.execute(query7)
    comparePlans(optimized7, query7)
  }

  test("preserve top-level alias metadata while collapsing projects") {
    def hasMetadata(logicalPlan: LogicalPlan): Boolean = {
      logicalPlan.asInstanceOf[Project].projectList.exists(_.metadata.contains("key"))
    }

    val metadata = new MetadataBuilder().putLong("key", 1).build()
    val analyzed =
      Project(Seq(Alias($"a_with_metadata", "b")()),
        Project(Seq(Alias($"a", "a_with_metadata")(explicitMetadata = Some(metadata))),
          testRelation.logicalPlan)).analyze
    require(hasMetadata(analyzed))

    val optimized = Optimize.execute(analyzed)
    val projects = optimized.collect { case p: Project => p }
    assert(projects.size === 1)
    assert(hasMetadata(optimized))
  }

  test("collapse redundant alias through limit") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as "b").limit(1).select($"b" as "c").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"a" as "c").limit(1).analyze
    comparePlans(optimized, expected)
  }

  test("collapse redundant alias through local limit") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = LocalLimit(1, relation.select($"a" as "b"))
      .select($"b" as "c").analyze
    val optimized = Optimize.execute(query)
    val expected = LocalLimit(1, relation.select($"a" as "c")).analyze
    comparePlans(optimized, expected)
  }

  test("collapse redundant alias through repartition") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as "b").repartition(1)
      .select($"b" as "c").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"a" as "c").repartition(1).analyze
    comparePlans(optimized, expected)
  }

  test("collapse redundant alias through sample") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = Sample(0.0, 0.6, false, 11L, relation.select($"a" as "b"))
      .select($"b" as "c").analyze
    val optimized = Optimize.execute(query)
    val expected = Sample(0.0, 0.6, false, 11L, relation.select($"a" as "c")).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-36086: CollapseProject should keep output schema name") {
    val relation = LocalRelation($"a".int, $"b".int)
    val select = relation.select(($"a" + $"b").as("c")).analyze
    val query = Project(Seq(select.output.head.withName("C")), select)
    val optimized = Optimize.execute(query)
    val expected = relation.select(($"a" + $"b").as("C")).analyze
    comparePlans(optimized, expected)
  }

  test("ES-1102888: collapse project duplicating aggregate expressions in UDF") {
    withSQLConf(SQLConf.AVOID_COLLAPSE_UDF_WITH_EXPENSIVE_EXPR.key -> "true") {
      val pythonUdf = (e: Expression) => {
        PythonUDF("udf", null, ArrayType(IntegerType), Seq(e), 0, udfDeterministic = true)
      }

      val query = testRelation
        .groupBy($"a")(collectList($"b").as("l1"))
        .select(pythonUdf($"l1").as("l2"))
        .select(CreateArray(Seq(
          GetArrayItem($"l2", 0),
          GetArrayItem($"l2", 1),
          GetArrayItem($"l2", 2),
          GetArrayItem($"l2", 3)
        )))
        .analyze

      val optimized = Optimize.execute(query)
      val expected = testRelation
        .groupBy($"a")(pythonUdf(collectList($"b")).as("l2"))
        .select(CreateArray(Seq(
          GetArrayItem($"l2", 0),
          GetArrayItem($"l2", 1),
          GetArrayItem($"l2", 2),
          GetArrayItem($"l2", 3)
        )))
        .analyze
      comparePlans(optimized, expected)
    }
  }

  test("SPARK-53399: Merge Python UDFs with same evalType") {
    val pythonUdf = (e: Expression) => {
      PythonUDF("udf", null, IntegerType, Seq(e), PythonEvalType.SQL_BATCHED_UDF, true)
    }

    val query = testRelation
      .select(
        pythonUdf($"a") as "udf_a", // Always inline
        $"b",
        $"b" + 1 as "b_plus_1", // Never inline
        $"b" + 2 as "b_plus_2") // Maybe inline
      .select(
        $"udf_a",
        pythonUdf($"b") as "udf_b",
        $"b_plus_1" + $"b_plus_1" as "2b_plus_2",
        $"b_plus_2")
      .analyze

    val optimized = Optimize.execute(query)

    val expected = testRelation
      .select(
        $"a", // New passthrough attribute is added due to always inline
        $"b",
        $"b" + 1 as "b_plus_1", // Never inline is kept in lower
        $"b" + 2 as "b_plus_2") // Maybe inline is kept in lower for now
      .select(
        pythonUdf($"a") as "udf_a", // Always inline is moved to upper
        pythonUdf($"b") as "udf_b",
        $"b_plus_1" + $"b_plus_1" as "2b_plus_2",
        $"b_plus_2")
      .analyze

    comparePlans(optimized, expected)
  }

  test("SPARK-53399: Don't merge Python UDFs with different evalType") {
    val pythonUdfA = (e: Expression) => {
      PythonUDF("udf", null, IntegerType, Seq(e), PythonEvalType.SQL_BATCHED_UDF, true)
    }

    val pythonUdfB = (e: Expression) => {
      PythonUDF("udf", null, IntegerType, Seq(e), PythonEvalType.SQL_ARROW_BATCHED_UDF, true)
    }

    val query = testRelation
      .select(
        pythonUdfA($"a") as "udf_a", // Maybe inline because `evalType` doesn't match to `udf_b`'s
        $"b",
        $"b" + 1 as "b_plus_1", // Never inline
        $"b" + 2 as "b_plus_2") // Maybe inline
      .select(
        $"udf_a",
        pythonUdfB($"b") as "udf_b",
        $"b_plus_1" + $"b_plus_1" as "2b_plus_2",
        "b_plus_2")
      .analyze

    val optimized = Optimize.execute(query)

    val expected = query // No always inlines so keep both nodes intact

    comparePlans(optimized, expected)
  }

  test("SPARK-59413: Collate is cheap when its value child is cheap") {
    // `Collate` is a pure passthrough: `eval` delegates to the child and the collation argument
    // is never evaluated, so `isCheap` mirrors the value child's cheapness.
    val col = $"s".string
    assert(CollapseProject.isCheap(col))
    assert(CollapseProject.isCheap(Collate(col, ResolvedCollation("UTF8_LCASE"))))

    // A non-cheap value child stays non-cheap through `Collate` (the SPARK-40228 guard is intact).
    val nonCheap = Substring(col, Literal(1), Literal(5))
    assert(!CollapseProject.isCheap(nonCheap))
    assert(!CollapseProject.isCheap(Collate(nonCheap, ResolvedCollation("UTF8_LCASE"))))
  }

  test("collapse single-ref rowOrderIndependent nondeterministic project " +
    "and prune unused") {
    val attrA = testRelation.output(0)
    val attrB = testRelation.output(1)
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    val roiExpr = RowOrderIndepTestExpr(attrA)(counter)
    val singleRefQuery = testRelation
      .select(roiExpr.as("c"))
      .select(($"c" + 1).as("d"))
      .analyze
    val optimizedSingleRef = Optimize.execute(singleRefQuery)
    val expectedSingleRef = testRelation
      .select((roiExpr + 1).as("d"))
      .analyze
    comparePlans(optimizedSingleRef, expectedSingleRef)

    // Verify evaluation count is 1 per row after collapse.
    val bound = BindReferences.bindReference(
      optimizedSingleRef.asInstanceOf[Project].projectList.head, testRelation.output)
    bound.eval(org.apache.spark.sql.catalyst.InternalRow(42, 0))
    assert(counter.get() == 1)

    // count == 0 unreferenced column is pruned.
    val unusedQuery = testRelation
      .select($"a", RowOrderIndepTestExpr(attrB)().as("c"))
      .select($"a")
      .analyze
    val optimizedUnused = Optimize.execute(unusedQuery)
    val expectedUnused = testRelation.select($"a").analyze
    comparePlans(optimizedUnused, expectedUnused)
  }

  test("do not collapse multi-ref rowOrderIndependent expr " +
    "even with alwaysInline = true") {
    val attrA = testRelation.output(0)
    val roiExpr = RowOrderIndepTestExpr(attrA)()
    val multiConsumerQuery = testRelation
      .select(roiExpr.as("c"))
      .select(($"c" + 1).as("d"), ($"c" + 2).as("e"))
      .analyze
    val sameConsumerMultiRefQuery = testRelation
      .select(roiExpr.as("c"))
      .select(($"c" + $"c").as("d"))
      .analyze

    withSQLConf(SQLConf.COLLAPSE_PROJECT_ALWAYS_INLINE.key -> "true") {
      comparePlans(Optimize.execute(multiConsumerQuery), multiConsumerQuery)
      comparePlans(Optimize.execute(sameConsumerMultiRefQuery), sameConsumerMultiRefQuery)
    }

    // Legacy !rowOrderIndependent nondeterministic expression is never collapsed even for count==1.
    val legacyNondetQuery = testRelation
      .select(CustomNondetUnaryTestExpr(attrA).as("c"))
      .select(($"c" + 1).as("d"))
      .analyze
    comparePlans(Optimize.execute(legacyNondetQuery), legacyNondetQuery)

    // Mixed lower Project containing both rowOrderIndependent and stateful Rand(0) stays blocked.
    val mixedLowerQuery = testRelation
      .select(roiExpr.as("c"), Rand(0).as("r"))
      .select(($"c" + 1).as("d"), ($"r" + 1).as("r2"))
      .analyze
    comparePlans(Optimize.execute(mixedLowerQuery), mixedLowerQuery)

    // Kill-switch disables single-ref rowOrderIndependent project collapse.
    withSQLConf(SQLConf.ROW_ORDER_INDEPENDENT_NONDETERMINISTIC_PUSHDOWN_ENABLED.key -> "false") {
      val singleRefQuery = testRelation
        .select(RowOrderIndepTestExpr(attrA)().as("c"))
        .select(($"c" + 1).as("d"))
        .analyze
      comparePlans(Optimize.execute(singleRefQuery), singleRefQuery)
    }
  }

  test("do not collapse rowOrderIndependent expr referenced inside lambda") {
    val arrRelation = LocalRelation($"arr".array(IntegerType), $"b".int)
    val attrArr = arrRelation.output(0)
    val attrB = arrRelation.output(1)
    val x = UnresolvedNamedLambdaVariable("x" :: Nil)

    val insideLambdaQuery = arrRelation
      .select(attrArr, RowOrderIndepTestExpr(attrB)().as("c"))
      .select(ArrayTransform(attrArr, LambdaFunction(x + $"c", Seq(x))).as("res"))
      .analyze
    comparePlans(Optimize.execute(insideLambdaQuery), insideLambdaQuery)

    // Positive control: single reference as the HigherOrderFunction input argument (not inside
    // the lambda body) evaluates once per row and can be collapsed.
    val roiArrExpr = RowOrderIndepTestExpr(attrArr)()
    val hofArgQuery = arrRelation
      .select(roiArrExpr.as("c_arr"))
      .select(ArrayTransform($"c_arr", LambdaFunction(x + 1, Seq(x))).as("res"))
      .analyze
    val expectedHofArg = arrRelation
      .select(ArrayTransform(roiArrExpr, LambdaFunction(x + 1, Seq(x))).as("res"))
      .analyze
    comparePlans(Optimize.execute(hofArgQuery), expectedHofArg)
  }

  test("Project over Aggregate and canCollapseExpressions keep deterministic-only behavior") {
    val attrA = testRelation.output(0)
    val roiExpr = RowOrderIndepTestExpr(attrA)()
    val singleRefAgg = testRelation
      .groupBy(attrA)(attrA, roiExpr.as("c"))
      .select(attrA, ($"c" + 1).as("d"))
      .analyze
    comparePlans(Optimize.execute(singleRefAgg), singleRefAgg)

    val multiRefAgg = testRelation
      .groupBy(attrA)(attrA, roiExpr.as("c"))
      .select(($"c" + 1).as("d"), ($"c" + 2).as("e"))
      .analyze
    comparePlans(Optimize.execute(multiRefAgg), multiRefAgg)

    // canCollapseExpressions stays deterministic-only (even with alwaysInline = true),
    // isolating CombineUnions, patterns.scala, and V2 pushdown gates.
    val lowerProj = testRelation.select(roiExpr.as("c")).analyze.asInstanceOf[Project]
    val upperProj = lowerProj.select(($"c" + 1).as("d")).analyze.asInstanceOf[Project]
    assert(!CollapseProject.canCollapseExpressions(
      upperProj.projectList, lowerProj.projectList, alwaysInline = false))
    assert(!CollapseProject.canCollapseExpressions(
      upperProj.projectList, lowerProj.projectList, alwaysInline = true))
  }
}
