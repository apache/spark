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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

class AnalysisSuite extends AnalysisTest {
  import org.apache.spark.sql.catalyst.analysis.TestRelations._

  test("union project *") {
    val plan = (1 to 100)
      .map(_ => testRelation)
      .fold[LogicalPlan](testRelation) { (a, b) =>
        a.select(UnresolvedStar(None)).select('a).unionAll(b.select(UnresolvedStar(None)))
      }

    assertAnalysisSuccess(plan)
  }

  test("check project's resolved") {
    assert(Project(testRelation.output, testRelation).resolved)

    assert(!Project(Seq(UnresolvedAttribute("a")), testRelation).resolved)

    val explode = Explode(AttributeReference("a", IntegerType, nullable = true)())
    assert(!Project(Seq(Alias(explode, "explode")()), testRelation).resolved)

    assert(!Project(Seq(Alias(Count(Literal(1)), "count")()), testRelation).resolved)
  }

  test("analyze project") {
    checkAnalysis(
      Project(Seq(UnresolvedAttribute("a")), testRelation),
      Project(testRelation.output, testRelation))

    checkAnalysis(
      Project(Seq(UnresolvedAttribute("TbL.a")), UnresolvedRelation(Seq("TaBlE"), Some("TbL"))),
      Project(testRelation.output, testRelation))

    assertAnalysisError(
      Project(Seq(UnresolvedAttribute("tBl.a")), UnresolvedRelation(Seq("TaBlE"), Some("TbL"))),
      Seq("cannot resolve"))

    checkAnalysis(
      Project(Seq(UnresolvedAttribute("TbL.a")), UnresolvedRelation(Seq("TaBlE"), Some("TbL"))),
      Project(testRelation.output, testRelation),
      caseSensitive = false)

    checkAnalysis(
      Project(Seq(UnresolvedAttribute("tBl.a")), UnresolvedRelation(Seq("TaBlE"), Some("TbL"))),
      Project(testRelation.output, testRelation),
      caseSensitive = false)
  }

  test("resolve relations") {
    assertAnalysisError(UnresolvedRelation(Seq("tAbLe"), None), Seq("Table Not Found: tAbLe"))

    checkAnalysis(UnresolvedRelation(Seq("TaBlE"), None), testRelation)

    checkAnalysis(UnresolvedRelation(Seq("tAbLe"), None), testRelation, caseSensitive = false)

    checkAnalysis(UnresolvedRelation(Seq("TaBlE"), None), testRelation, caseSensitive = false)
  }

  test("divide should be casted into fractional types") {
    val plan = caseInsensitiveAnalyzer.execute(
      testRelation2.select(
        'a / Literal(2) as 'div1,
        'a / 'b as 'div2,
        'a / 'c as 'div3,
        'a / 'd as 'div4,
        'e / 'e as 'div5))
    val pl = plan.asInstanceOf[Project].projectList

    assert(pl(0).dataType == DoubleType)
    assert(pl(1).dataType == DoubleType)
    assert(pl(2).dataType == DoubleType)
    // StringType will be promoted into Decimal(38, 18)
    assert(pl(3).dataType == DecimalType(38, 22))
    assert(pl(4).dataType == DoubleType)
  }

  test("pull out nondeterministic expressions from RepartitionByExpression") {
    val plan = RepartitionByExpression(Seq(Rand(33)), testRelation)
    val projected = Alias(Rand(33), "_nondeterministic")()
    val expected =
      Project(testRelation.output,
        RepartitionByExpression(Seq(projected.toAttribute),
          Project(testRelation.output :+ projected, testRelation)))
    checkAnalysis(plan, expected)
  }

  test("pull out nondeterministic expressions from Sort") {
    val plan = Sort(Seq(SortOrder(Rand(33), Ascending)), false, testRelation)
    val projected = Alias(Rand(33), "_nondeterministic")()
    val expected =
      Project(testRelation.output,
        Sort(Seq(SortOrder(projected.toAttribute, Ascending)), false,
          Project(testRelation.output :+ projected, testRelation)))
    checkAnalysis(plan, expected)
  }

  test("SPARK-9634: cleanup unnecessary Aliases in LogicalPlan") {
    val a = testRelation.output.head
    var plan = testRelation.select(((a + 1).as("a+1") + 2).as("col"))
    var expected = testRelation.select((a + 1 + 2).as("col"))
    checkAnalysis(plan, expected)

    plan = testRelation.groupBy(a.as("a1").as("a2"))((min(a).as("min_a") + 1).as("col"))
    expected = testRelation.groupBy(a)((min(a) + 1).as("col"))
    checkAnalysis(plan, expected)

    // CreateStruct is a special case that we should not trim Alias for it.
    plan = testRelation.select(CreateStruct(Seq(a, (a + 1).as("a+1"))).as("col"))
    checkAnalysis(plan, plan)
    plan = testRelation.select(CreateStructUnsafe(Seq(a, (a + 1).as("a+1"))).as("col"))
    checkAnalysis(plan, plan)
  }
}
