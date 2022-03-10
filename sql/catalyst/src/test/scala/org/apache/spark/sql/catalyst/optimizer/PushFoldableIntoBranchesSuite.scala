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

import java.sql.{Date, Timestamp}
import java.time.{Duration, Period}

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, TimestampType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.CalendarInterval


class PushFoldableIntoBranchesSuite
  extends PlanTest with ExpressionEvalHelper with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PushFoldableIntoBranches", FixedPoint(50),
      BooleanSimplification, ConstantFolding, SimplifyConditionals, PushFoldableIntoBranches) :: Nil
  }

  private val relation = LocalRelation('a.int, 'b.int, 'c.boolean)
  private val a = EqualTo(UnresolvedAttribute("a"), Literal(100))
  private val b = UnresolvedAttribute("b")
  private val c = EqualTo(UnresolvedAttribute("c"), Literal(true))
  private val ifExp = If(a, Literal(2), Literal(3))
  private val caseWhen = CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), Some(Literal(3)))

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, relation).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, relation).analyze)
    comparePlans(actual, correctAnswer)
  }

  test("Push down EqualTo through If") {
    assertEquivalent(EqualTo(ifExp, Literal(4)), FalseLiteral)
    assertEquivalent(EqualTo(ifExp, Literal(3)), Not(a <=> TrueLiteral))

    // Push down at most one not foldable expressions.
    assertEquivalent(
      EqualTo(If(a, b, Literal(2)), Literal(2)),
      If(a, EqualTo(b, Literal(2)), TrueLiteral))
    assertEquivalent(
      EqualTo(If(a, b, b + 1), Literal(2)),
      EqualTo(If(a, b, b + 1), Literal(2)))

    // Push down non-deterministic expressions.
    val nonDeterministic = If(LessThan(Rand(1), Literal(0.5)), Literal(1), Literal(2))
    assert(!nonDeterministic.deterministic)
    assertEquivalent(EqualTo(nonDeterministic, Literal(2)),
      GreaterThanOrEqual(Rand(1), Literal(0.5)))
    assertEquivalent(EqualTo(nonDeterministic, Literal(3)),
      If(LessThan(Rand(1), Literal(0.5)), FalseLiteral, FalseLiteral))

    // Handle Null values.
    assertEquivalent(
      EqualTo(If(a, Literal(null, IntegerType), Literal(1)), Literal(1)),
      If(a, Literal(null, BooleanType), TrueLiteral))
    assertEquivalent(
      EqualTo(If(a, Literal(null, IntegerType), Literal(1)), Literal(2)),
      If(a, Literal(null, BooleanType), FalseLiteral))
    assertEquivalent(
      EqualTo(If(a, Literal(1), Literal(2)), Literal(null, IntegerType)),
      Literal(null, BooleanType))
    assertEquivalent(
      EqualTo(If(a, Literal(null, IntegerType), Literal(null, IntegerType)), Literal(1)),
      Literal(null, BooleanType))
  }

  test("Push down other BinaryComparison through If") {
    assertEquivalent(EqualNullSafe(ifExp, Literal(4)), FalseLiteral)
    assertEquivalent(GreaterThan(ifExp, Literal(4)), FalseLiteral)
    assertEquivalent(GreaterThanOrEqual(ifExp, Literal(4)), FalseLiteral)
    assertEquivalent(LessThan(ifExp, Literal(4)), TrueLiteral)
    assertEquivalent(LessThanOrEqual(ifExp, Literal(4)), TrueLiteral)
  }

  test("Push down other BinaryOperator through If") {
    assertEquivalent(Add(ifExp, Literal(4)), If(a, Literal(6), Literal(7)))
    assertEquivalent(Subtract(ifExp, Literal(4)), If(a, Literal(-2), Literal(-1)))
    assertEquivalent(Multiply(ifExp, Literal(4)), If(a, Literal(8), Literal(12)))
    assertEquivalent(Pmod(ifExp, Literal(4)), If(a, Literal(2), Literal(3)))
    assertEquivalent(Remainder(ifExp, Literal(4)), If(a, Literal(2), Literal(3)))
    assertEquivalent(Divide(If(a, Literal(2.0), Literal(3.0)), Literal(1.0)),
      If(a, Literal(2.0), Literal(3.0)))
    assertEquivalent(And(If(a, FalseLiteral, TrueLiteral), TrueLiteral), Not(a <=> TrueLiteral))
    assertEquivalent(Or(If(a, FalseLiteral, TrueLiteral), TrueLiteral), TrueLiteral)
  }

  test("Push down other BinaryExpression through If") {
    assertEquivalent(BRound(If(a, Literal(1.23), Literal(1.24)), Literal(1)), Literal(1.2))
    assertEquivalent(StartsWith(If(a, Literal("ab"), Literal("ac")), Literal("a")), TrueLiteral)
    assertEquivalent(FindInSet(If(a, Literal("ab"), Literal("ac")), Literal("a")), Literal(0))
    assertEquivalent(
      AddMonths(If(a, Literal(Date.valueOf("2020-01-01")), Literal(Date.valueOf("2021-01-01"))),
        Literal(1)),
      If(a, Literal(Date.valueOf("2020-02-01")), Literal(Date.valueOf("2021-02-01"))))
  }

  test("Push down EqualTo through CaseWhen") {
    assertEquivalent(EqualTo(caseWhen, Literal(4)), FalseLiteral)
    assertEquivalent(EqualTo(caseWhen, Literal(3)),
      CaseWhen(Seq((a, FalseLiteral), (c, FalseLiteral)), Some(TrueLiteral)))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), None), Literal(4)),
      CaseWhen(Seq((a, FalseLiteral), (c, FalseLiteral)), None))

    assertEquivalent(
      And(EqualTo(caseWhen, Literal(5)), EqualTo(caseWhen, Literal(6))),
      FalseLiteral)

    // Push down at most one branch is not foldable expressions.
    assertEquivalent(EqualTo(CaseWhen(Seq((a, b), (c, Literal(1))), None), Literal(1)),
      CaseWhen(Seq((a, EqualTo(b, Literal(1))), (c, TrueLiteral)), None))
    assertEquivalent(EqualTo(CaseWhen(Seq((a, b), (c, b + 1)), None), Literal(1)),
      EqualTo(CaseWhen(Seq((a, b), (c, b + 1)), None), Literal(1)))
    assertEquivalent(EqualTo(CaseWhen(Seq((a, b)), None), Literal(1)),
      CaseWhen(Seq((a, EqualTo(b, Literal(1))))))

    // Push down non-deterministic expressions.
    val nonDeterministic =
      CaseWhen(Seq((LessThan(Rand(1), Literal(0.5)), Literal(1))), Some(Literal(2)))
    assert(!nonDeterministic.deterministic)
    assertEquivalent(EqualTo(nonDeterministic, Literal(2)),
      GreaterThanOrEqual(Rand(1), Literal(0.5)))
    assertEquivalent(EqualTo(nonDeterministic, Literal(3)),
      CaseWhen(Seq((LessThan(Rand(1), Literal(0.5)), FalseLiteral)), Some(FalseLiteral)))

    // Handle Null values.
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(null, IntegerType))), Some(Literal(1))), Literal(2)),
      CaseWhen(Seq((a, Literal(null, BooleanType))), Some(FalseLiteral)))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(1))), Some(Literal(2))), Literal(null, IntegerType)),
      Literal(null, BooleanType))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(null, IntegerType))), Some(Literal(1))), Literal(1)),
      CaseWhen(Seq((a, Literal(null, BooleanType))), Some(TrueLiteral)))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(null, IntegerType))), Some(Literal(null, IntegerType))),
        Literal(1)),
      Literal(null, BooleanType))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(null, IntegerType))), Some(Literal(null, IntegerType))),
        Literal(null, IntegerType)),
      Literal(null, BooleanType))
  }

  test("Push down other BinaryComparison through CaseWhen") {
    assertEquivalent(EqualNullSafe(caseWhen, Literal(4)), FalseLiteral)
    assertEquivalent(GreaterThan(caseWhen, Literal(4)), FalseLiteral)
    assertEquivalent(GreaterThanOrEqual(caseWhen, Literal(4)), FalseLiteral)
    assertEquivalent(LessThan(caseWhen, Literal(4)), TrueLiteral)
    assertEquivalent(LessThanOrEqual(caseWhen, Literal(4)), TrueLiteral)
  }

  test("Push down other BinaryOperator through CaseWhen") {
    assertEquivalent(Add(caseWhen, Literal(4)),
      CaseWhen(Seq((a, Literal(5)), (c, Literal(6))), Some(Literal(7))))
    assertEquivalent(Subtract(caseWhen, Literal(4)),
      CaseWhen(Seq((a, Literal(-3)), (c, Literal(-2))), Some(Literal(-1))))
    assertEquivalent(Multiply(caseWhen, Literal(4)),
      CaseWhen(Seq((a, Literal(4)), (c, Literal(8))), Some(Literal(12))))
    assertEquivalent(Pmod(caseWhen, Literal(4)),
      CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), Some(Literal(3))))
    assertEquivalent(Remainder(caseWhen, Literal(4)),
      CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), Some(Literal(3))))
    assertEquivalent(Divide(CaseWhen(Seq((a, Literal(1.0)), (c, Literal(2.0))), Some(Literal(3.0))),
      Literal(1.0)),
      CaseWhen(Seq((a, Literal(1.0)), (c, Literal(2.0))), Some(Literal(3.0))))
    assertEquivalent(And(CaseWhen(Seq((a, FalseLiteral), (c, TrueLiteral)), Some(TrueLiteral)),
      TrueLiteral),
      CaseWhen(Seq((a, FalseLiteral), (c, TrueLiteral)), Some(TrueLiteral)))
    assertEquivalent(Or(CaseWhen(Seq((a, FalseLiteral), (c, TrueLiteral)), Some(TrueLiteral)),
      TrueLiteral), TrueLiteral)
  }

  test("Push down other BinaryExpression through CaseWhen") {
    assertEquivalent(
      BRound(CaseWhen(Seq((a, Literal(1.23)), (c, Literal(1.24))), Some(Literal(1.25))),
        Literal(1)),
      Literal(1.2))
    assertEquivalent(
      StartsWith(CaseWhen(Seq((a, Literal("ab")), (c, Literal("ac"))), Some(Literal("ad"))),
        Literal("a")),
      TrueLiteral)
    assertEquivalent(
      FindInSet(CaseWhen(Seq((a, Literal("ab")), (c, Literal("ac"))), Some(Literal("ad"))),
        Literal("a")),
      Literal(0))
    assertEquivalent(
      AddMonths(CaseWhen(Seq((a, Literal(Date.valueOf("2020-01-01"))),
        (c, Literal(Date.valueOf("2021-01-01")))),
        Some(Literal(Date.valueOf("2022-01-01")))),
        Literal(1)),
      CaseWhen(Seq((a, Literal(Date.valueOf("2020-02-01"))),
        (c, Literal(Date.valueOf("2021-02-01")))),
        Some(Literal(Date.valueOf("2022-02-01")))))
  }

  test("Push down BinaryExpression through If/CaseWhen backwards") {
    assertEquivalent(EqualTo(Literal(4), ifExp), FalseLiteral)
    assertEquivalent(EqualTo(Literal(4), caseWhen), FalseLiteral)
  }

  test("SPARK-33848: Push down cast through If/CaseWhen") {
    assertEquivalent(If(a, Literal(2), Literal(3)).cast(StringType),
      If(a, Literal("2"), Literal("3")))
    assertEquivalent(If(a, b, Literal(3)).cast(StringType),
      If(a, b.cast(StringType), Literal("3")))
    assertEquivalent(If(a, b, b + 1).cast(StringType),
      If(a, b, b + 1).cast(StringType))

    assertEquivalent(
      CaseWhen(Seq((a, Literal(1))), Some(Literal(3))).cast(StringType),
      CaseWhen(Seq((a, Literal("1"))), Some(Literal("3"))))
    assertEquivalent(
      CaseWhen(Seq((a, Literal(1))), Some(b)).cast(StringType),
      CaseWhen(Seq((a, Literal("1"))), Some(b.cast(StringType))))
    assertEquivalent(
      CaseWhen(Seq((a, b)), Some(b + 1)).cast(StringType),
      CaseWhen(Seq((a, b)), Some(b + 1)).cast(StringType))
  }

  test("SPARK-33848: Push down abs through If/CaseWhen") {
    assertEquivalent(Abs(If(a, Literal(-2), Literal(-3))), If(a, Literal(2), Literal(3)))
    assertEquivalent(
      Abs(CaseWhen(Seq((a, Literal(-1))), Some(Literal(-3)))),
      CaseWhen(Seq((a, Literal(1))), Some(Literal(3))))
  }

  test("SPARK-33848: Push down cast with binary expression through If/CaseWhen") {
    assertEquivalent(EqualTo(If(a, Literal(2), Literal(3)).cast(StringType), Literal("4")),
      FalseLiteral)
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(1))), Some(Literal(3))).cast(StringType), Literal("4")),
      FalseLiteral)
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), None).cast(StringType), Literal("4")),
      CaseWhen(Seq((a, FalseLiteral), (c, FalseLiteral)), None))
  }

  test("SPARK-33848: Push down dateTimeExpression with binary expression through If/CaseWhen") {
    val d = Date.valueOf("2021-01-01")
    // If
    assertEquivalent(AddMonths(Literal(d),
      If(a, Literal(1), Literal(2))),
      If(a, Literal(Date.valueOf("2021-02-01")), Literal(Date.valueOf("2021-03-01"))))
    assertEquivalent(DateAdd(Literal(d),
      If(a, Literal(1), Literal(2))),
      If(a, Literal(Date.valueOf("2021-01-02")), Literal(Date.valueOf("2021-01-03"))))
    assertEquivalent(DateAddInterval(Literal(d),
      If(a, Literal(new CalendarInterval(1, 1, 0)),
        Literal(new CalendarInterval(1, 2, 0)))),
      If(a, Literal(Date.valueOf("2021-02-02")), Literal(Date.valueOf("2021-02-03"))))
    assertEquivalent(DateAddYMInterval(Literal(d),
      If(a, Literal.create(Period.ofMonths(1), YearMonthIntervalType()),
        Literal.create(Period.ofMonths(2), YearMonthIntervalType()))),
      If(a, Literal(Date.valueOf("2021-02-01")), Literal(Date.valueOf("2021-03-01"))))
    assertEquivalent(DateDiff(Literal(d),
      If(a, Literal(Date.valueOf("2021-02-01")), Literal(Date.valueOf("2021-03-01")))),
      If(a, Literal(-31), Literal(-59)))
    assertEquivalent(DateSub(Literal(d),
      If(a, Literal(1), Literal(2))),
      If(a, Literal(Date.valueOf("2020-12-31")), Literal(Date.valueOf("2020-12-30"))))
    assertEquivalent(TimestampAddYMInterval(
      Literal.create(Timestamp.valueOf("2021-01-01 00:00:00.000"), TimestampType),
      If(a, Literal.create(Period.ofMonths(1), YearMonthIntervalType()),
        Literal.create(Period.ofMonths(2), YearMonthIntervalType()))),
      If(a, Literal.create(Timestamp.valueOf("2021-02-01 00:00:00"), TimestampType),
        Literal.create(Timestamp.valueOf("2021-03-01 00:00:00"), TimestampType)))
    assertEquivalent(TimeAdd(
      Literal.create(Timestamp.valueOf("2021-01-01 00:00:00.000"), TimestampType),
      If(a, Literal(Duration.ofDays(10).plusMinutes(10).plusMillis(321)),
        Literal(Duration.ofDays(10).plusMinutes(10).plusMillis(456)))),
      If(a, Literal.create(Timestamp.valueOf("2021-01-11 00:10:00.321"), TimestampType),
        Literal.create(Timestamp.valueOf("2021-01-11 00:10:00.456"), TimestampType)))

    // CaseWhen
    assertEquivalent(AddMonths(Literal(d),
      CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), None)),
      CaseWhen(Seq((a, Literal(Date.valueOf("2021-02-01"))),
        (c, Literal(Date.valueOf("2021-03-01")))), None))
    assertEquivalent(DateAdd(Literal(d),
      CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), None)),
      CaseWhen(Seq((a, Literal(Date.valueOf("2021-01-02"))),
        (c, Literal(Date.valueOf("2021-01-03")))), None))
    assertEquivalent(DateAddInterval(Literal(d),
      CaseWhen(Seq((a, Literal(new CalendarInterval(1, 1, 0))),
        (c, Literal(new CalendarInterval(1, 2, 0)))), None)),
      CaseWhen(Seq((a, Literal(Date.valueOf("2021-02-02"))),
        (c, Literal(Date.valueOf("2021-02-03")))), None))
    assertEquivalent(DateAddYMInterval(Literal(d),
      CaseWhen(Seq((a, Literal.create(Period.ofMonths(1), YearMonthIntervalType())),
        (c, Literal.create(Period.ofMonths(2), YearMonthIntervalType()))), None)),
      CaseWhen(Seq((a, Literal(Date.valueOf("2021-02-01"))),
        (c, Literal(Date.valueOf("2021-03-01")))), None))
    assertEquivalent(DateDiff(Literal(d),
      CaseWhen(Seq((a, Literal(Date.valueOf("2021-02-01"))),
        (c, Literal(Date.valueOf("2021-03-01")))), None)),
      CaseWhen(Seq((a, Literal(-31)), (c, Literal(-59))), None))
    assertEquivalent(DateSub(Literal(d),
      CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), None)),
      CaseWhen(Seq((a, Literal(Date.valueOf("2020-12-31"))),
        (c, Literal(Date.valueOf("2020-12-30")))), None))
    assertEquivalent(TimestampAddYMInterval(
      Literal.create(Timestamp.valueOf("2021-01-01 00:00:00.000"), TimestampType),
      CaseWhen(Seq((a, Literal.create(Period.ofMonths(1), YearMonthIntervalType())),
        (c, Literal.create(Period.ofMonths(2), YearMonthIntervalType()))), None)),
      CaseWhen(Seq((a, Literal.create(Timestamp.valueOf("2021-02-01 00:00:00"), TimestampType)),
        (c, Literal.create(Timestamp.valueOf("2021-03-01 00:00:00"), TimestampType))), None))
    assertEquivalent(TimeAdd(
      Literal.create(Timestamp.valueOf("2021-01-01 00:00:00.000"), TimestampType),
      CaseWhen(Seq((a, Literal(Duration.ofDays(10).plusMinutes(10).plusMillis(321))),
        (c, Literal(Duration.ofDays(10).plusMinutes(10).plusMillis(456)))), None)),
      CaseWhen(Seq((a, Literal.create(Timestamp.valueOf("2021-01-11 00:10:00.321"), TimestampType)),
        (c, Literal.create(Timestamp.valueOf("2021-01-11 00:10:00.456"), TimestampType))), None))
  }

  test("SPARK-33847: Remove the CaseWhen if elseValue is empty and other outputs are null") {
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal.create(null, IntegerType)))), Literal(2)),
      Literal.create(null, BooleanType))
    if (!conf.ansiEnabled) {
      assertEquivalent(
        EqualTo(CaseWhen(Seq((LessThan(Rand(1), Literal(0.5)), Literal("str")))).cast(IntegerType),
          Literal(2)),
        CaseWhen(Seq((LessThan(Rand(1), Literal(0.5)), Literal.create(null, BooleanType)))))
    }
  }

  test("SPARK-33884: simplify CaseWhen clauses with (true and false) and (false and true)") {
    assertEquivalent(
      EqualTo(CaseWhen(Seq(('a > 10, Literal(0))), Literal(1)), Literal(0)),
      'a > 10 <=> TrueLiteral)
    assertEquivalent(
      EqualTo(CaseWhen(Seq(('a > 10, Literal(0))), Literal(1)), Literal(1)),
      Not('a > 10 <=> TrueLiteral))
  }

  test("SPARK-37270: Fix push foldable into CaseWhen branches if elseValue is empty") {
    assertEquivalent(
      IsNull(CaseWhen(Seq(('a > 10, Literal(0))), Literal(1))),
      FalseLiteral)
    assertEquivalent(
      IsNull(CaseWhen(Seq(('a > 10, Literal(0))))),
      !('a > 10 <=> true))

    assertEquivalent(
      CaseWhen(Seq(('a > 10, Literal(0))), Literal(1)) <=> Literal(null, IntegerType),
      FalseLiteral)
    assertEquivalent(
      CaseWhen(Seq(('a > 10, Literal(0)))) <=> Literal(null, IntegerType),
      !('a > 10 <=> true))

    assertEquivalent(
      Literal(null, IntegerType) <=> CaseWhen(Seq(('a > 10, Literal(0))), Literal(1)),
      FalseLiteral)
    assertEquivalent(
      Literal(null, IntegerType) <=> CaseWhen(Seq(('a > 10, Literal(0)))),
      !('a > 10 <=> true))
  }
}
