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
import org.apache.spark.sql.catalyst.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, CurrentTime, CurrentTimestamp, Literal, Rand}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, EvalInlineTables}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{LongType, NullType, TimestampType, TimeType}

/**
 * Unit tests for [[ResolveInlineTables]]. Note that there are also test cases defined in
 * end-to-end tests (in sql/core module) for verifying the correct error messages are shown
 * in negative cases.
 */
class ResolveInlineTablesSuite extends AnalysisTest with BeforeAndAfter {

  private def lit(v: Any): Literal = Literal(v)

  test("validate inputs are foldable") {
    EvaluateUnresolvedInlineTable.validateInputEvaluable(
      UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(lit(1)))))

    // Alias is OK
    EvaluateUnresolvedInlineTable.validateInputEvaluable(
      UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(Alias(lit(1), "a")()))))

    // nondeterministic (rand) should not work
    intercept[AnalysisException] {
      EvaluateUnresolvedInlineTable.validateInputEvaluable(
        UnresolvedInlineTable(Seq("c1"), Seq(Seq(Rand(1)))))
    }

    // aggregate should not work
    intercept[AnalysisException] {
      EvaluateUnresolvedInlineTable.validateInputEvaluable(
        UnresolvedInlineTable(Seq("c1"), Seq(Seq(Count(lit(1))))))
    }

    // unresolved attribute should not work
    intercept[AnalysisException] {
      EvaluateUnresolvedInlineTable.validateInputEvaluable(
        UnresolvedInlineTable(Seq("c1"), Seq(Seq(UnresolvedAttribute("A")))))
    }
  }

  test("validate input dimensions") {
    EvaluateUnresolvedInlineTable.validateInputDimension(
      UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(lit(2)))))

    // num alias != data dimension
    intercept[AnalysisException] {
      EvaluateUnresolvedInlineTable.validateInputDimension(
        UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(lit(1)), Seq(lit(2)))))
    }

    // num alias == data dimension, but data themselves are inconsistent
    intercept[AnalysisException] {
      EvaluateUnresolvedInlineTable.validateInputDimension(
        UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(lit(21), lit(22)))))
    }
  }

  test("do not fire the rule if not all expressions are resolved") {
    val table = UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(UnresolvedAttribute("A"))))
    assert(ResolveInlineTables(table) == table)
  }

  test("cast and execute") {
    val table = UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(lit(2L))))
    val resolved = ResolveInlineTables(table)
    assert(resolved.isInstanceOf[LocalRelation])
    val converted = resolved.asInstanceOf[LocalRelation]

    assert(converted.output.map(_.dataType) == Seq(LongType))
    assert(converted.data.size == 2)
    assert(converted.data(0).getLong(0) == 1L)
    assert(converted.data(1).getLong(0) == 2L)
  }

  test("cast and execute CURRENT_LIKE expressions") {
    val table = UnresolvedInlineTable(Seq("c1"), Seq(
      Seq(CurrentTimestamp()), Seq(CurrentTimestamp())))
    val resolved = ResolveInlineTables(table)
    // Early eval should keep it in expression form.
    assert(resolved.isInstanceOf[ResolvedInlineTable])

    EvalInlineTables(ComputeCurrentTime(resolved)) match {
      case LocalRelation(output, data, _, _) =>
        assert(output.map(_.dataType) == Seq(TimestampType))
        assert(data.size == 2)
        // Make sure that both CURRENT_TIMESTAMP expressions are evaluated to the same value.
        assert(data(0).getLong(0) == data(1).getLong(0))
    }
  }

  test("cast and execute CURRENT_TIME expressions") {
    val table = UnresolvedInlineTable(
      Seq("c1"),
      Seq(
        Seq(CurrentTime()),
        Seq(CurrentTime())
      )
    )
    val resolved = ResolveInlineTables(table)
    assert(resolved.isInstanceOf[ResolvedInlineTable],
      "Expected an inline table to be resolved into a ResolvedInlineTable")

    val transformed = ComputeCurrentTime(resolved)
    EvalInlineTables(transformed) match {
      case LocalRelation(output, data, _, _) =>
        // expect default precision = 6
        assert(output.map(_.dataType) == Seq(TimeType(6)))
        // Should have 2 rows
        assert(data.size == 2)
        // Both rows should have the *same* microsecond value for current_time
        assert(data(0).getLong(0) == data(1).getLong(0),
          "Both CURRENT_TIME calls must yield the same value in the same query")
    }
  }


  test("convert TimeZoneAwareExpression") {
    val table = UnresolvedInlineTable(Seq("c1"),
      Seq(Seq(Cast(lit("1991-12-06 00:00:00.0"), TimestampType))))
    val withTimeZone = ResolveTimeZone.apply(table)
    val LocalRelation(output, data, _, _) =
      EvalInlineTables(ResolveInlineTables.apply(withTimeZone))
    val correct = Cast(lit("1991-12-06 00:00:00.0"), TimestampType)
      .withTimeZone(conf.sessionLocalTimeZone).eval().asInstanceOf[Long]
    assert(output.map(_.dataType) == Seq(TimestampType))
    assert(data.size == 1)
    assert(data.head.getLong(0) == correct)
  }

  test("nullability inference in convert") {
    val table1 = UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(lit(2L))))
    val converted1 = EvaluateUnresolvedInlineTable.findCommonTypesAndCast(table1)
    assert(!converted1.schema.fields(0).nullable)

    val table2 = UnresolvedInlineTable(Seq("c1"), Seq(Seq(lit(1)), Seq(Literal(null, NullType))))
    val converted2 = EvaluateUnresolvedInlineTable.findCommonTypesAndCast(table2)
    assert(converted2.schema.fields(0).nullable)
  }
}
