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
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Cast, Concat, CreateArray, CurrentTime, CurrentTimestamp, CurrentUser, Exists, Explode, InSubquery, ListQuery, Literal, OuterReference, Rand, ScalarSubquery, Uuid}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, EvalInlineTables}
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LocalRelation, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.util.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, NullType, TimestampNTZType, TimestampType, TimeType}

/**
 * Unit tests for [[ResolveInlineTables]]. Note that there are also test cases defined in
 * end-to-end tests (in sql/core module) for verifying the correct error messages are shown
 * in negative cases.
 */
class ResolveInlineTablesSuite extends AnalysisTest with BeforeAndAfter {

  private def lit(v: Any): Literal = Literal(v)

  test("validate inputs are foldable") {
    withSQLConf(SQLConf.INLINE_TABLE_NON_FOLDABLE_EXPRESSIONS_ENABLED.key -> "false") {
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
      intercept[AnalysisException] {
        EvaluateUnresolvedInlineTable.validateInputEvaluable(
          UnresolvedInlineTable(Seq("c1"), Seq(Seq(Cast(Rand(1), TimestampNTZType)))))
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
  }

  test("validate inputs are foldable or can be evaluated when the query runs") {
    val oneRow = Project(Seq(Alias(lit(1), "a")()), OneRowRelation())
    Seq(lit(1), Alias(lit(1), "a")(), Rand(1), Uuid(Some(1L)), ScalarSubquery(oneRow)).foreach {
      e =>
        EvaluateUnresolvedInlineTable.validateInputEvaluable(
          UnresolvedInlineTable(Seq("c1"), Seq(Seq(e))))
    }

    val outerReference = OuterReference(AttributeReference("a", IntegerType)())
    val correlatedPlan = Project(Seq(Alias(outerReference, "b")()), OneRowRelation())
    Seq(
      Count(lit(1)),
      Count(lit(1)).toAggregateExpression(),
      Explode(CreateArray(Seq(lit(1), lit(2)))),
      outerReference,
      ScalarSubquery(correlatedPlan, outerAttrs = Seq(outerReference)),
      Exists(oneRow),
      InSubquery(Seq(lit(1)), ListQuery(oneRow, numCols = 1)),
      UnresolvedAttribute("A")
    ).foreach { e =>
      intercept[AnalysisException] {
        EvaluateUnresolvedInlineTable.validateInputEvaluable(
          UnresolvedInlineTable(Seq("c1"), Seq(Seq(e))))
      }
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
    val resolved = ResolveInlineTables(ResolveTimeZone(table))
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

  test("inline tables with non-foldable expressions are evaluated when the query runs") {
    val table = UnresolvedInlineTable(Seq("c1", "c2"),
      Seq(Seq(lit(1), Rand(1)), Seq(Add(lit(2), lit(3)), lit(1.5))))
    val resolved = ResolveInlineTables(table)
    resolved match {
      case Expand(projections, output, _: OneRowRelation) =>
        assert(output.map(_.dataType) == Seq(IntegerType, DoubleType))
        // Foldable expressions are evaluated during analysis.
        assert(projections == Seq(Seq(lit(1), Rand(1)), Seq(lit(5), lit(1.5))))
      case other => fail(s"Expected an Expand, but got $other")
    }
    assert(resolved.maxRows.contains(2L))
    // The optimizer leaves the table in the plan, to be evaluated when the query runs.
    assert(EvalInlineTables(resolved) == resolved)
  }

  test("inline tables without non-foldable expressions are resolved as before") {
    val flag = SQLConf.INLINE_TABLE_NON_FOLDABLE_EXPRESSIONS_ENABLED.key
    Seq(
      Seq(Seq(lit(1), Add(lit(2), lit(3))), Seq(lit(4L), lit(5))),
      // CURRENT_LIKE expressions are replaced at the end of the analysis phase, so the other
      // expressions of the table are evaluated at that point too.
      Seq(Seq(CurrentTimestamp(), Add(lit(2), lit(3)))),
      // The cell is evaluated at the end of the analysis phase like before, even though it isn't
      // foldable once CURRENT_LIKE expressions are replaced.
      Seq(Seq(Concat(Seq(CurrentUser(), Uuid(Some(1L))))))
    ).foreach { rows =>
      val table = UnresolvedInlineTable(rows.head.indices.map(i => s"c$i"), rows)
      val resolved = ResolveInlineTables(table)
      assert(!resolved.isInstanceOf[Expand])
      comparePlans(
        resolved, withSQLConf(flag -> "false")(ResolveInlineTables(table)), checkAnalysis = false)
    }
  }

  test("aliased foldable expressions are evaluated during analysis") {
    // Column DEFAULT values are substituted as aliased expressions.
    val table = UnresolvedInlineTable(Seq("c1"), Seq(Seq(Alias(lit(1), "a")())))
    assert(ResolveInlineTables(table).isInstanceOf[LocalRelation])

    val mixed = UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(Alias(lit(1), "a")(), Rand(1))))
    ResolveInlineTables(mixed) match {
      case Expand(Seq(Seq(l: Literal, _: Rand)), _, _) => assert(l == lit(1))
      case other => fail(s"Expected an Expand, but got $other")
    }
  }

  test("CURRENT_LIKE expressions next to non-foldable expressions") {
    val table = UnresolvedInlineTable(Seq("c1", "c2"),
      Seq(Seq(CurrentTimestamp(), Rand(1)), Seq(CurrentTimestamp(), Rand(2))))
    EvalInlineTables(ComputeCurrentTime(ResolveInlineTables(table))) match {
      case Expand(Seq(Seq(ts1: Literal, _: Rand), Seq(ts2: Literal, _: Rand)), _, _) =>
        // Both CURRENT_TIMESTAMP expressions are evaluated to the same value.
        assert(ts1 == ts2)
      case other => fail(s"Expected an Expand, but got $other")
    }
  }

  test("evaluation errors of foldable expressions next to non-foldable expressions") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val table = UnresolvedInlineTable(Seq("c1", "c2"),
        Seq(Seq(Cast(lit("abc"), IntegerType), Rand(1))))
      val e = intercept[AnalysisException](ResolveInlineTables(table))
      assert(e.getCondition == "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION")
    }
  }

  test("analysis of inline tables with nondeterministic expressions") {
    assertAnalysisSuccess(UnresolvedInlineTable(Seq("c1"), Seq(Seq(Rand(1)), Seq(Rand(2)))))
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
