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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._

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
    caseSensitiveCatalog.registerTable(None, "TaBlE", testRelation)
    caseInsensitiveCatalog.registerTable(None, "TaBlE", testRelation)
  }

  test("union project *") {
    val plan = (1 to 100)
      .map(_ => testRelation)
      .fold[LogicalPlan](testRelation)((a,b) => a.select(Star(None)).select('a).unionAll(b.select(Star(None))))

    assert(caseInsensitiveAnalyze(plan).resolved)
  }

  test("analyze project") {
    assert(
      caseSensitiveAnalyze(Project(Seq(UnresolvedAttribute("a")), testRelation)) ===
        Project(testRelation.output, testRelation))

    assert(
      caseSensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("TbL.a")),
          UnresolvedRelation(None, "TaBlE", Some("TbL")))) ===
        Project(testRelation.output, testRelation))

    val e = intercept[TreeNodeException[_]] {
      caseSensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("tBl.a")),
          UnresolvedRelation(None, "TaBlE", Some("TbL"))))
    }
    assert(e.getMessage().toLowerCase.contains("unresolved"))

    assert(
      caseInsensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("TbL.a")),
          UnresolvedRelation(None, "TaBlE", Some("TbL")))) ===
        Project(testRelation.output, testRelation))

    assert(
      caseInsensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("tBl.a")),
          UnresolvedRelation(None, "TaBlE", Some("TbL")))) ===
        Project(testRelation.output, testRelation))
  }

  test("resolve relations") {
    val e = intercept[RuntimeException] {
      caseSensitiveAnalyze(UnresolvedRelation(None, "tAbLe", None))
    }
    assert(e.getMessage == "Table Not Found: tAbLe")

    assert(
      caseSensitiveAnalyze(UnresolvedRelation(None, "TaBlE", None)) ===
        testRelation)

    assert(
      caseInsensitiveAnalyze(UnresolvedRelation(None, "tAbLe", None)) ===
        testRelation)

    assert(
      caseInsensitiveAnalyze(UnresolvedRelation(None, "TaBlE", None)) ===
        testRelation)
  }

  test("throw errors for unresolved attributes during analysis") {
    val e = intercept[TreeNodeException[_]] {
      caseSensitiveAnalyze(Project(Seq(UnresolvedAttribute("abcd")), testRelation))
    }
    assert(e.getMessage().toLowerCase.contains("unresolved attribute"))
  }

  test("throw errors for unresolved plans during analysis") {
    case class UnresolvedTestPlan() extends LeafNode {
      override lazy val resolved = false
      override def output = Nil
    }
    val e = intercept[TreeNodeException[_]] {
      caseSensitiveAnalyze(UnresolvedTestPlan())
    }
    assert(e.getMessage().toLowerCase.contains("unresolved plan"))
  }

  test("divide should be casted into fractional types") {
    val testRelation2 = LocalRelation(
      AttributeReference("a", StringType)(),
      AttributeReference("b", StringType)(),
      AttributeReference("c", DoubleType)(),
      AttributeReference("d", DecimalType.Unlimited)(),
      AttributeReference("e", ShortType)())

    val expr0 = 'a / 2
    val expr1 = 'a / 'b
    val expr2 = 'a / 'c
    val expr3 = 'a / 'd
    val expr4 = 'e / 'e
    val plan = caseInsensitiveAnalyze(Project(
      Alias(expr0, s"Analyzer($expr0)")() ::
      Alias(expr1, s"Analyzer($expr1)")() ::
      Alias(expr2, s"Analyzer($expr2)")() ::
      Alias(expr3, s"Analyzer($expr3)")() ::
      Alias(expr4, s"Analyzer($expr4)")() :: Nil, testRelation2))
    val pl = plan.asInstanceOf[Project].projectList
    assert(pl(0).dataType == DoubleType)
    assert(pl(1).dataType == DoubleType)
    assert(pl(2).dataType == DoubleType)
    assert(pl(3).dataType == DecimalType.Unlimited)
    assert(pl(4).dataType == DoubleType)
  }
}
