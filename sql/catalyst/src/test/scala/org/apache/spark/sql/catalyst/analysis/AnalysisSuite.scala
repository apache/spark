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

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._

class AnalysisSuite extends FunSuite with BeforeAndAfter {
  val caseSensitiveCatalog = new SimpleCatalog(true)
  val caseInsensitiveCatalog = new SimpleCatalog(false)
  val caseSensitiveAnalyze =
    new Analyzer(caseSensitiveCatalog, EmptyFunctionRegistry, caseSensitive = true)
  val caseInsensitiveAnalyze =
    new Analyzer(caseInsensitiveCatalog, EmptyFunctionRegistry, caseSensitive = false)

  val testRelation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())
  val testRelation2 = LocalRelation(
    AttributeReference("a", StringType)(),
    AttributeReference("b", StringType)(),
    AttributeReference("c", DoubleType)(),
    AttributeReference("d", DecimalType.Unlimited)(),
    AttributeReference("e", ShortType)())

  before {
    caseSensitiveCatalog.registerTable(Seq("TaBlE"), testRelation)
    caseInsensitiveCatalog.registerTable(Seq("TaBlE"), testRelation)
  }

  test("union project *") {
    val plan = (1 to 100)
      .map(_ => testRelation)
      .fold[LogicalPlan](testRelation) { (a, b) =>
        a.select(UnresolvedStar(None)).select('a).unionAll(b.select(UnresolvedStar(None)))
      }

    assert(caseInsensitiveAnalyze(plan).resolved)
  }

  test("check project's resolved") {
    assert(Project(testRelation.output, testRelation).resolved)

    assert(!Project(Seq(UnresolvedAttribute("a")), testRelation).resolved)

    val explode = Explode(Nil, AttributeReference("a", IntegerType, nullable = true)())
    assert(!Project(Seq(Alias(explode, "explode")()), testRelation).resolved)

    assert(!Project(Seq(Alias(Count(Literal(1)), "count")()), testRelation).resolved)
  }

  test("analyze project") {
    assert(
      caseSensitiveAnalyze(Project(Seq(UnresolvedAttribute("a")), testRelation)) ===
        Project(testRelation.output, testRelation))

    assert(
      caseSensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("TbL.a")),
          UnresolvedRelation(Seq("TaBlE"), Some("TbL")))) ===
        Project(testRelation.output, testRelation))

    val e = intercept[AnalysisException] {
      caseSensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("tBl.a")),
          UnresolvedRelation(Seq("TaBlE"), Some("TbL"))))
    }
    assert(e.getMessage().toLowerCase.contains("cannot resolve"))

    assert(
      caseInsensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("TbL.a")),
          UnresolvedRelation(Seq("TaBlE"), Some("TbL")))) ===
        Project(testRelation.output, testRelation))

    assert(
      caseInsensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("tBl.a")),
          UnresolvedRelation(Seq("TaBlE"), Some("TbL")))) ===
        Project(testRelation.output, testRelation))
  }

  test("resolve relations") {
    val e = intercept[RuntimeException] {
      caseSensitiveAnalyze(UnresolvedRelation(Seq("tAbLe"), None))
    }
    assert(e.getMessage == "Table Not Found: tAbLe")

    assert(
      caseSensitiveAnalyze(UnresolvedRelation(Seq("TaBlE"), None)) ===
        testRelation)

    assert(
      caseInsensitiveAnalyze(UnresolvedRelation(Seq("tAbLe"), None)) ===
        testRelation)

    assert(
      caseInsensitiveAnalyze(UnresolvedRelation(Seq("TaBlE"), None)) ===
        testRelation)
  }

  def errorTest(
      name: String,
      plan: LogicalPlan,
      errorMessages: Seq[String],
      caseSensitive: Boolean = true) = {
    test(name) {
      val error = intercept[AnalysisException] {
        if(caseSensitive) {
          caseSensitiveAnalyze(plan)
        } else {
          caseInsensitiveAnalyze(plan)
        }
      }

      errorMessages.foreach(m => assert(error.getMessage contains m))
    }
  }

  errorTest(
    "unresolved attributes",
    testRelation.select('abcd),
    "cannot resolve" :: "abcd" :: Nil)

  errorTest(
    "bad casts",
    testRelation.select(Literal(1).cast(BinaryType).as('badCast)),
    "invalid cast" :: Literal(1).dataType.simpleString :: BinaryType.simpleString :: Nil)

  errorTest(
    "non-boolean filters",
    testRelation.where(Literal(1)),
    "filter" :: "'1'" :: "not a boolean" :: Literal(1).dataType.simpleString :: Nil)

  errorTest(
    "missing group by",
    testRelation2.groupBy('a)('b),
    "'b'" :: "group by" :: Nil
  )

  case class UnresolvedTestPlan() extends LeafNode {
    override lazy val resolved = false
    override def output = Nil
  }

  errorTest(
    "catch all unresolved plan",
    UnresolvedTestPlan(),
    "unresolved" :: Nil)


  test("divide should be casted into fractional types") {
    val testRelation2 = LocalRelation(
      AttributeReference("a", StringType)(),
      AttributeReference("b", StringType)(),
      AttributeReference("c", DoubleType)(),
      AttributeReference("d", DecimalType.Unlimited)(),
      AttributeReference("e", ShortType)())

    val plan = caseInsensitiveAnalyze(
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
    assert(pl(3).dataType == DecimalType.Unlimited)
    assert(pl(4).dataType == DoubleType)
  }
}
