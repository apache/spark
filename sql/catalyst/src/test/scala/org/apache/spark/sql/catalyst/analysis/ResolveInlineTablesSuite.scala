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

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Cast, Concat, CreateArray, CurrentTime, CurrentTimestamp, CurrentUser, Exists, Explode, Expression, InSubquery, ListQuery, Literal, OuterReference, Rand, ScalarSubquery, UnaryExpression, Uuid}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, EvalInlineTables}
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LocalRelation, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.util.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, DoubleType, IntegerType, LongType, NullType, TimestampNTZType, TimestampType, TimeType}

/**
 * Unit tests for [[ResolveInlineTables]]. Note that there are also test cases defined in
 * end-to-end tests (in sql/core module) for verifying the correct error messages are shown
 * in negative cases.
 */
class ResolveInlineTablesSuite extends AnalysisTest with BeforeAndAfter {

  for (scale <- Seq(1, 10, 20)) {
    test(s"inline table nullability includes decimal coercion at scale $scale") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST.key -> "true") {
        val large = Literal(Decimal(BigDecimal("9" * 38)), DecimalType(38, 0))
        val fraction = Literal(Decimal(BigDecimal("0." + "1" * scale)),
          DecimalType(scale, scale))
        val table = UnresolvedInlineTable(Seq("c"), Seq(Seq(large), Seq(fraction)))
        val resolved = EvaluateUnresolvedInlineTable.findCommonTypesAndCast(table)
        assert(resolved.output.head.nullable)
        assert(resolved.rows.head.head.eval() == null)
        assert(resolved.rows(1).head.eval() != null)
        val converted = EvaluateUnresolvedInlineTable.evaluate(table).asInstanceOf[LocalRelation]
        assert(converted.output.head.nullable)
        assert(converted.data.head.isNullAt(0))
        assert(!converted.data(1).isNullAt(0))

        val fitting = table.copy(rows = Seq(
          Seq(Literal(Decimal(1), DecimalType(38, 0))), Seq(fraction)))
        val fittingResult =
          EvaluateUnresolvedInlineTable.evaluate(fitting).asInstanceOf[LocalRelation]
        assert(fittingResult.output.head.dataType == DecimalType(38, scale))
        assert(!fittingResult.output.head.nullable)
        assert(fittingResult.data.map(_.getDecimal(0, 38, scale)) ==
          Seq(Decimal(1), fraction.value))

        val integral = EvaluateUnresolvedInlineTable.findCommonTypesAndCast(
          UnresolvedInlineTable(Seq("c"), Seq(Seq(Literal(1)), Seq(Literal(2L)))))
        assert(!integral.output.head.nullable)
      }
    }
  }

  test("inline table preserves originally nullable inputs") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST.key -> "true") {
      val fraction = Literal(Decimal(BigDecimal("0.1")), DecimalType(1, 1))
      val fittingCast = Cast(Literal("1"), DecimalType(38, 0))
      assert(fittingCast.nullable)
      Seq(fittingCast, Literal.create(null, DecimalType(38, 0))).foreach { first =>
        val table = UnresolvedInlineTable(Seq("c"), Seq(Seq(first), Seq(fraction)))
        val result = EvaluateUnresolvedInlineTable.evaluate(table).asInstanceOf[LocalRelation]
        assert(result.output.head.nullable)
        assert(result.data.head.isNullAt(0) == (first != fittingCast))
        assert(result.data(1).getDecimal(0, 38, 1) == Decimal(BigDecimal("0.1")))
      }
    }
  }

  test("inline table refines List-backed columns independently") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST.key -> "true") {
      val fraction = Literal(Decimal(BigDecimal("0.1")), DecimalType(1, 1))
      val fitting = Literal(Decimal(1), DecimalType(38, 0))
      val overflow = Literal(Decimal(BigDecimal("9" * 38)), DecimalType(38, 0))
      val nullable = Cast(Literal("1"), DecimalType(38, 0))
      val table = UnresolvedInlineTable(List("fitting", "overflow", "nullable"), List(
        List(fitting, overflow, nullable), List(fraction, fraction, fraction)))
      val result = EvaluateUnresolvedInlineTable.evaluate(table).asInstanceOf[LocalRelation]
      assert(result.output.map(_.nullable) == Seq(false, true, true))
      assert(result.output.map(_.dataType) == Seq.fill(3)(DecimalType(38, 1)))
      assert(result.data.head.getDecimal(0, 38, 1) == Decimal(1))
      assert(result.data.head.isNullAt(1))
      assert(result.data.head.getDecimal(2, 38, 1) == Decimal(1))
    }
  }

  for (deferred <- Seq(false, true)) {
    test(s"inline table evaluates coercion inputs once, deferred=$deferred") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST.key -> "true") {
        val evaluations = new AtomicInteger()
        def counted(value: String, dataType: DecimalType): Expression =
          CountedInlineValue(Literal(Decimal(BigDecimal(value)), dataType), evaluations)
        val rows = Seq(
          Seq(counted("1", DecimalType(38, 0)), counted("9" * 38, DecimalType(38, 0))),
          Seq(counted("0.1", DecimalType(1, 1)), counted("0.1", DecimalType(1, 1))))
        val input = if (deferred) {
          UnresolvedInlineTable(Seq("fitting", "overflow", "time"),
            rows.map(_ :+ CurrentTimestamp()))
        } else {
          UnresolvedInlineTable(Seq("fitting", "overflow"), rows)
        }
        val resolved = EvaluateUnresolvedInlineTable.evaluate(input)
        val result = if (deferred) {
          assert(resolved.isInstanceOf[ResolvedInlineTable])
          assert(resolved.output.map(_.nullable) == Seq(true, true, false))
          assert(evaluations.get() == 0)
          EvalInlineTables(ComputeCurrentTime(resolved)).asInstanceOf[LocalRelation]
        } else {
          resolved.asInstanceOf[LocalRelation]
        }
        assert(evaluations.get() == 4)
        assert(result.output.take(2).map(_.nullable) == Seq(deferred, true))
        assert(result.data.head.getDecimal(0, 38, 1) == Decimal(1))
        assert(result.data.head.isNullAt(1))
        assert(result.data(1).getDecimal(0, 38, 1) == Decimal(BigDecimal("0.1")))
        assert(result.data(1).getDecimal(1, 38, 1) == Decimal(BigDecimal("0.1")))
        if (deferred) {
          assert(result.data.head.getLong(2) == result.data(1).getLong(2))
        }
      }
    }
  }

  test("inline table preserves ANSI overflow errors") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST.key -> "true") {
      val table = UnresolvedInlineTable(Seq("c"), Seq(
        Seq(Literal(Decimal(BigDecimal("9" * 38)), DecimalType(38, 0))),
        Seq(Literal(Decimal(BigDecimal("0.1")), DecimalType(1, 1)))))
      val error = intercept[AnalysisException] {
        EvaluateUnresolvedInlineTable.evaluate(table)
      }
      assert(error.getCondition == "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION")
    }
  }

  // Test-only instrumentation of a foldable value to detect duplicate evaluation.
  private case class CountedInlineValue(child: Expression, evaluations: AtomicInteger)
    extends UnaryExpression with CodegenFallback {
    override def dataType: DataType = child.dataType
    override protected def nullSafeEval(input: Any): Any = {
      evaluations.incrementAndGet()
      input
    }
    override protected def withNewChildInternal(newChild: Expression): Expression =
      copy(child = newChild)
  }

  test("inline table runtime projections include nullable coerced inputs") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST.key -> "true",
      SQLConf.INLINE_TABLE_NON_FOLDABLE_EXPRESSIONS_ENABLED.key -> "true") {
      val table = UnresolvedInlineTable(Seq("c", "r"), Seq(
        Seq(Literal(Decimal(BigDecimal("9" * 38)), DecimalType(38, 0)), Rand(1)),
        Seq(Literal(Decimal(BigDecimal("0.1")), DecimalType(1, 1)), Rand(2))))
      val result = EvaluateUnresolvedInlineTable.evaluate(table).asInstanceOf[Expand]
      assert(result.output.head.nullable)
      assert(result.projections.head.head.eval() == null)
      assert(result.projections(1).head.eval() == Decimal(BigDecimal("0.1")))
    }
  }

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
      Seq(Seq(CurrentTimestamp(), Add(lit(2), lit(3))))
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

  test("nondeterministic expressions next to CURRENT_LIKE expressions in the same expression") {
    // A ResolvedInlineTable cannot contain nondeterministic expressions.
    val e = Concat(Seq(CurrentUser(), Uuid(Some(1L))))
    val table = UnresolvedInlineTable(Seq("c1"), Seq(Seq(e)))
    ResolveInlineTables(table) match {
      case Expand(Seq(Seq(projection)), _, _: OneRowRelation) => assert(projection == e)
      case other => fail(s"Expected an Expand, but got $other")
    }
    withSQLConf(SQLConf.INLINE_TABLE_NON_FOLDABLE_EXPRESSIONS_ENABLED.key -> "false") {
      assert(ResolveInlineTables(table).isInstanceOf[ResolvedInlineTable])
    }
  }

  test("subqueries with CURRENT_LIKE expressions are not evaluated during analysis") {
    // The tree patterns of a subquery expression include those of its plan.
    val currentTimestampRow = Project(Seq(Alias(CurrentTimestamp(), "ts")()), OneRowRelation())
    Seq(
      ScalarSubquery(currentTimestampRow),
      CreateArray(Seq(CurrentTimestamp(), ScalarSubquery(currentTimestampRow)))
    ).foreach { e =>
      ResolveInlineTables(UnresolvedInlineTable(Seq("c1"), Seq(Seq(e)))) match {
        case Expand(Seq(Seq(projection)), _, _: OneRowRelation) => assert(projection == e)
        case other => fail(s"Expected an Expand, but got $other")
      }
    }
    Seq(
      Exists(currentTimestampRow),
      InSubquery(Seq(CurrentTimestamp()), ListQuery(currentTimestampRow, numCols = 1))
    ).foreach { e =>
      val table = UnresolvedInlineTable(Seq("c1"), Seq(Seq(e)))
      val ex = intercept[AnalysisException](ResolveInlineTables(table))
      assert(ex.getCondition == "INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE")
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
