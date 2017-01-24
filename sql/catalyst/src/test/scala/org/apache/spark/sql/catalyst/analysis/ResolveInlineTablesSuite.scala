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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Literal, Rand}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.types.{LongType, NullType}

/**
 * Unit tests for [[ResolveInlineTables]]. Note that there are also test cases defined in
 * end-to-end tests (in sql/core module) for verifying the correct error messages are shown
 * in negative cases.
 */
class ResolveInlineTablesSuite extends PlanTest with BeforeAndAfter {

  private def lit(v: Any): Literal = Literal(v)

  test("validate inputs are foldable") {
    ResolveInlineTables.validateInputEvaluable(
      UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(lit(1)))))

    // nondeterministic (rand) should not work
    intercept[AnalysisException] {
      ResolveInlineTables.validateInputEvaluable(
        UnresolvedInlineTable(Seq("c1"), Seq(Seq(Rand(1)))))
    }

    // aggregate should not work
    intercept[AnalysisException] {
      ResolveInlineTables.validateInputEvaluable(
        UnresolvedInlineTable(Seq("c1"), Seq(Seq(Count(lit(1))))))
    }

    // unresolved attribute should not work
    intercept[AnalysisException] {
      ResolveInlineTables.validateInputEvaluable(
        UnresolvedInlineTable(Seq("c1"), Seq(Seq(UnresolvedAttribute("A")))))
    }
  }

  test("validate input dimensions") {
    ResolveInlineTables.validateInputDimension(
      UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(lit(2)))))

    // num alias != data dimension
    intercept[AnalysisException] {
      ResolveInlineTables.validateInputDimension(
        UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(lit(1)), Seq(lit(2)))))
    }

    // num alias == data dimension, but data themselves are inconsistent
    intercept[AnalysisException] {
      ResolveInlineTables.validateInputDimension(
        UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(lit(21), lit(22)))))
    }
  }

  test("do not fire the rule if not all expressions are resolved") {
    val table = UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(UnresolvedAttribute("A"))))
    assert(ResolveInlineTables(table) == table)
  }

  test("convert") {
    val table = UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(lit(2L))))
    val converted = ResolveInlineTables.convert(table)

    assert(converted.output.map(_.dataType) == Seq(LongType))
    assert(converted.data.size == 2)
    assert(converted.data(0).getLong(0) == 1L)
    assert(converted.data(1).getLong(0) == 2L)
  }

  test("nullability inference in convert") {
    val table1 = UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(lit(2L))))
    val converted1 = ResolveInlineTables.convert(table1)
    assert(!converted1.schema.fields(0).nullable)

    val table2 = UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(Literal(null, NullType))))
    val converted2 = ResolveInlineTables.convert(table2)
    assert(converted2.schema.fields(0).nullable)
  }
}
