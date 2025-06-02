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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class DataSourceV2StrategySuite extends PlanTest with SharedSparkSession {
  val attrInts = Seq(
    $"cint".int,
    $"`c.int`".int,
    GetStructField($"a".struct(StructType(
      StructField("cstr", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 1, None),
    GetStructField($"a".struct(StructType(
      StructField("c.int", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 0, None),
    GetStructField($"`a.b`".struct(StructType(
      StructField("cstr1", StringType, nullable = true) ::
        StructField("cstr2", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 2, None),
    GetStructField($"`a.b`".struct(StructType(
      StructField("c.int", IntegerType, nullable = true) :: Nil)), 0, None),
    GetStructField(GetStructField($"a".struct(StructType(
      StructField("cstr1", StringType, nullable = true) ::
        StructField("b", StructType(StructField("cint", IntegerType, nullable = true) ::
          StructField("cstr2", StringType, nullable = true) :: Nil)) :: Nil)), 1, None), 0, None)
  ).zip(Seq(
    "cint",
    "`c.int`", // single level field that contains `dot` in name
    "a.cint", // two level nested field
    "a.`c.int`", // two level nested field, and nested level contains `dot`
    "`a.b`.cint", // two level nested field, and top level contains `dot`
    "`a.b`.`c.int`", // two level nested field, and both levels contain `dot`
    "a.b.cint" // three level nested field
  ))

  val attrStrs = Seq(
    $"cstr".string,
    $"`c.str`".string,
    GetStructField($"a".struct(StructType(
      StructField("cint", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 1, None),
    GetStructField($"a".struct(StructType(
      StructField("c.str", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 0, None),
    GetStructField($"`a.b`".struct(StructType(
      StructField("cint1", IntegerType, nullable = true) ::
        StructField("cint2", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 2, None),
    GetStructField($"`a.b`".struct(StructType(
      StructField("c.str", StringType, nullable = true) :: Nil)), 0, None),
    GetStructField(GetStructField($"a".struct(StructType(
      StructField("cint1", IntegerType, nullable = true) ::
        StructField("b", StructType(StructField("cstr", StringType, nullable = true) ::
          StructField("cint2", IntegerType, nullable = true) :: Nil)) :: Nil)), 1, None), 0, None)
  ).zip(Seq(
    "cstr",
    "`c.str`", // single level field that contains `dot` in name
    "a.cstr", // two level nested field
    "a.`c.str`", // two level nested field, and nested level contains `dot`
    "`a.b`.cstr", // two level nested field, and top level contains `dot`
    "`a.b`.`c.str`", // two level nested field, and both levels contain `dot`
    "a.b.cstr" // three level nested field
  ))

  test("translate simple expression") { attrInts.zip(attrStrs)
    .foreach { case ((attrInt, intColName), (attrStr, strColName)) =>
      testTranslateFilter(EqualTo(attrInt, 1),
        Some(new Predicate("=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(EqualTo(1, attrInt),
        Some(new Predicate("=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(EqualNullSafe(attrInt, 1),
        Some(new Predicate("<=>", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(EqualNullSafe(1, attrInt),
        Some(new Predicate("<=>", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(GreaterThan(attrInt, 1),
        Some(new Predicate(">", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(GreaterThan(1, attrInt),
        Some(new Predicate("<", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(LessThan(attrInt, 1),
        Some(new Predicate("<", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(LessThan(1, attrInt),
        Some(new Predicate(">", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(GreaterThanOrEqual(attrInt, 1),
        Some(new Predicate(">=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(GreaterThanOrEqual(1, attrInt),
        Some(new Predicate("<=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(LessThanOrEqual(attrInt, 1),
        Some(new Predicate("<=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))
      testTranslateFilter(LessThanOrEqual(1, attrInt),
        Some(new Predicate(">=", Array(FieldReference(intColName), LiteralValue(1, IntegerType)))))

      testTranslateFilter(IsNull(attrInt),
        Some(new Predicate("IS_NULL", Array(FieldReference(intColName)))))
      testTranslateFilter(IsNotNull(attrInt),
        Some(new Predicate("IS_NOT_NULL", Array(FieldReference(intColName)))))

      testTranslateFilter(InSet(attrInt, Set(1, 2, 3)),
        Some(new Predicate("IN", Array(FieldReference(intColName),
          LiteralValue(1, IntegerType), LiteralValue(2, IntegerType),
          LiteralValue(3, IntegerType)))))

      testTranslateFilter(In(attrInt, Seq(1, 2, 3)),
        Some(new Predicate("IN", Array(FieldReference(intColName),
          LiteralValue(1, IntegerType), LiteralValue(2, IntegerType),
          LiteralValue(3, IntegerType)))))

      // cint > 1 AND cint < 10
      testTranslateFilter(And(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)),
        Some(new V2And(
          new Predicate(">", Array(FieldReference(intColName), LiteralValue(1, IntegerType))),
          new Predicate("<", Array(FieldReference(intColName), LiteralValue(10, IntegerType))))))

      // cint >= 8 OR cint <= 2
      testTranslateFilter(Or(
        GreaterThanOrEqual(attrInt, 8),
        LessThanOrEqual(attrInt, 2)),
        Some(new V2Or(
          new Predicate(">=", Array(FieldReference(intColName), LiteralValue(8, IntegerType))),
          new Predicate("<=", Array(FieldReference(intColName), LiteralValue(2, IntegerType))))))

      testTranslateFilter(Not(GreaterThanOrEqual(attrInt, 8)),
        Some(new V2Not(new Predicate(">=", Array(FieldReference(intColName),
          LiteralValue(8, IntegerType))))))

      testTranslateFilter(StartsWith(attrStr, "a"),
        Some(new Predicate("STARTS_WITH", Array(FieldReference(strColName),
          LiteralValue(UTF8String.fromString("a"), StringType)))))

      testTranslateFilter(EndsWith(attrStr, "a"),
        Some(new Predicate("ENDS_WITH", Array(FieldReference(strColName),
          LiteralValue(UTF8String.fromString("a"), StringType)))))

      testTranslateFilter(Contains(attrStr, "a"),
        Some(new Predicate("CONTAINS", Array(FieldReference(strColName),
          LiteralValue(UTF8String.fromString("a"), StringType)))))
    }
  }

  test("translate complex expression") {
    attrInts.foreach { case (attrInt, intColName) =>

      // ABS(cint) - 2 <= 1
      testTranslateFilter(LessThanOrEqual(
        // Expressions are not supported
        // Functions such as 'Abs' are not pushed down with ANSI mode off
        Subtract(Abs(attrInt, failOnError = false), 2), 1), None)

      // (cin1 > 1 AND cint < 10) OR (cint > 50 AND cint > 100)
      testTranslateFilter(Or(
        And(
          GreaterThan(attrInt, 1),
          LessThan(attrInt, 10)
        ),
        And(
          GreaterThan(attrInt, 50),
          LessThan(attrInt, 100))),
        Some(new V2Or(
          new V2And(
            new Predicate(">", Array(FieldReference(intColName), LiteralValue(1, IntegerType))),
            new Predicate("<", Array(FieldReference(intColName), LiteralValue(10, IntegerType)))),
          new V2And(
            new Predicate(">", Array(FieldReference(intColName), LiteralValue(50, IntegerType))),
            new Predicate("<", Array(FieldReference(intColName),
              LiteralValue(100, IntegerType)))))
        )
      )

      // (cint > 1 AND ABS(cint) < 10) OR (cint < 50 AND cint > 100)
      testTranslateFilter(Or(
        And(
          GreaterThan(attrInt, 1),
          // Functions such as 'Abs' are not pushed down with ANSI mode off
          LessThan(Abs(attrInt, failOnError = false), 10)
        ),
        And(
          GreaterThan(attrInt, 50),
          LessThan(attrInt, 100))), None)

      // NOT ((cint <= 1 OR ABS(cint) >= 10) AND (cint <= 50 OR cint >= 100))
      testTranslateFilter(Not(And(
        Or(
          LessThanOrEqual(attrInt, 1),
          // Functions such as 'Abs' are not pushed down with ANSI mode off
          GreaterThanOrEqual(Abs(attrInt, failOnError = false), 10)
        ),
        Or(
          LessThanOrEqual(attrInt, 50),
          GreaterThanOrEqual(attrInt, 100)))), None)

      // (cint = 1 OR cint = 10) OR (cint > 0 OR cint < -10)
      testTranslateFilter(Or(
        Or(
          EqualTo(attrInt, 1),
          EqualTo(attrInt, 10)
        ),
        Or(
          GreaterThan(attrInt, 0),
          LessThan(attrInt, -10))),
        Some(new V2Or(
          new V2Or(
            new Predicate("=", Array(FieldReference(intColName), LiteralValue(1, IntegerType))),
            new Predicate("=", Array(FieldReference(intColName), LiteralValue(10, IntegerType)))),
          new V2Or(
            new Predicate(">", Array(FieldReference(intColName), LiteralValue(0, IntegerType))),
            new Predicate("<", Array(FieldReference(intColName), LiteralValue(-10, IntegerType)))))
        )
      )

      // (cint = 1 OR ABS(cint) = 10) OR (cint > 0 OR cint < -10)
      testTranslateFilter(Or(
        Or(
          EqualTo(attrInt, 1),
          // Functions such as 'Abs' are not pushed down with ANSI mode off
          EqualTo(Abs(attrInt, failOnError = false), 10)
        ),
        Or(
          GreaterThan(attrInt, 0),
          LessThan(attrInt, -10))), None)

      // In end-to-end testing, conjunctive predicate should has been split
      // before reaching DataSourceStrategy.translateFilter.
      // This is for UT purpose to test each [[case]].
      // (cint > 1 AND cint < 10) AND (cint = 6 AND cint IS NOT NULL)
      testTranslateFilter(And(
        And(
          GreaterThan(attrInt, 1),
          LessThan(attrInt, 10)
        ),
        And(
          EqualTo(attrInt, 6),
          IsNotNull(attrInt))),
        Some(new V2And(
          new V2And(
            new Predicate(">", Array(FieldReference(intColName), LiteralValue(1, IntegerType))),
            new Predicate("<", Array(FieldReference(intColName), LiteralValue(10, IntegerType)))),
          new V2And(
            new Predicate("=", Array(FieldReference(intColName), LiteralValue(6, IntegerType))),
            new Predicate("IS_NOT_NULL", Array(FieldReference(intColName)))))
        )
      )

      // (cint > 1 AND cint < 10) AND (ABS(cint) = 6 AND cint IS NOT NULL)
      testTranslateFilter(And(
        And(
          GreaterThan(attrInt, 1),
          LessThan(attrInt, 10)
        ),
        And(
          // Functions such as 'Abs' are not pushed down with ANSI mode off
          EqualTo(Abs(attrInt, failOnError = false), 6),
          IsNotNull(attrInt))), None)

      // (cint > 1 OR cint < 10) AND (cint = 6 OR cint IS NOT NULL)
      testTranslateFilter(And(
        Or(
          GreaterThan(attrInt, 1),
          LessThan(attrInt, 10)
        ),
        Or(
          EqualTo(attrInt, 6),
          IsNotNull(attrInt))),
        Some(new V2And(
          new V2Or(
            new Predicate(">", Array(FieldReference(intColName), LiteralValue(1, IntegerType))),
            new Predicate("<", Array(FieldReference(intColName), LiteralValue(10, IntegerType)))),
          new V2Or(
            new Predicate("=", Array(FieldReference(intColName), LiteralValue(6, IntegerType))),
            new Predicate("IS_NOT_NULL", Array(FieldReference(intColName)))))
        )
      )

      // (cint > 1 OR cint < 10) AND (cint = 6 OR cint IS NOT NULL)
      testTranslateFilter(And(
        Or(
          GreaterThan(attrInt, 1),
          LessThan(attrInt, 10)
        ),
        Or(
          // Functions such as 'Abs' are not pushed down with ANSI mode off
          EqualTo(Abs(attrInt, failOnError = false), 6),
          IsNotNull(attrInt))), None)
    }
  }

  test("SPARK-36644: Push down boolean column filter") {
    testTranslateFilter($"col".boolean,
      Some(new Predicate("=", Array(FieldReference("col"), LiteralValue(true, BooleanType)))))
  }

  test("inability to convert unknown expressions and predicates") {
    val unknownExpr = new GeneralScalarExpression("UNKNOWN", Array())
    assert(V2ExpressionUtils.toCatalyst(unknownExpr).isEmpty)

    val unknownPred = new Predicate("UNKNOWN", Array())
    assert(V2ExpressionUtils.toCatalyst(unknownPred).isEmpty)
  }

  test("round trip conversion of CASE_WHEN expression") {
    // CASE WHEN cond1 THEN value1 WHEN cond2 THEN value2
    checkRoundTripConversion(
      catalystExpr = CaseWhen(
        Seq(
          (EqualTo(Literal(1), Literal(2)), Literal("a")),
          (EqualTo(Literal(3), Literal(4)), Literal("b"))),
        None),
      v2Expr = new GeneralScalarExpression(
        "CASE_WHEN",
        Array(
          new Predicate("=", Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))),
          LiteralValue(UTF8String.fromString("a"), StringType),
          new Predicate("=", Array(LiteralValue(3, IntegerType), LiteralValue(4, IntegerType))),
          LiteralValue(UTF8String.fromString("b"), StringType))))

    // CASE WHEN cond1 THEN value1 ELSE elseValue
    checkRoundTripConversion(
      catalystExpr = CaseWhen(
        Seq((EqualTo(Literal(1), Literal(2)), Literal("yes"))),
        Some(Literal("no"))),
      v2Expr = new GeneralScalarExpression(
        "CASE_WHEN",
        Array(
          new Predicate("=", Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))),
          LiteralValue(UTF8String.fromString("yes"), StringType),
          LiteralValue(UTF8String.fromString("no"), StringType))))

    // CASE WHEN cond1 THEN true ELSE false
    checkRoundTripConversion(
      catalystExpr = CaseWhen(
        Seq((EqualTo(Literal(1), Literal(2)), Literal(true))),
        Some(Literal(false))),
      v2Expr = new Predicate(
        "CASE_WHEN",
        Array(
          new Predicate("=", Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))),
          new AlwaysTrue,
          new AlwaysFalse)),
      isPredicate = true)

    // CASE WHEN cond1 THEN true WHEN cond2 THEN false ELSE true
    checkRoundTripConversion(
      catalystExpr = CaseWhen(
        Seq(
          (EqualTo(Literal(1), Literal(2)), Literal(true)),
          (EqualTo(Literal(3), Literal(4)), Literal(false))),
        Some(Literal(true))),
      v2Expr = new Predicate(
        "CASE_WHEN",
        Array(
          new Predicate("=", Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))),
          new AlwaysTrue,
          new Predicate("=", Array(LiteralValue(3, IntegerType), LiteralValue(4, IntegerType))),
          new AlwaysFalse,
          new AlwaysTrue)),
      isPredicate = true)
  }

  test("round trip conversion of math functions") {
    checkRoundTripConversion(
      catalystExpr = Log10(Literal(100)),
      v2Expr = new GeneralScalarExpression("LOG10", Array(LiteralValue(100, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = new Rand(),
      v2Expr = new GeneralScalarExpression("RAND", Array()))

    checkRoundTripConversion(
      catalystExpr = new Rand(Literal(17L)),
      v2Expr = new GeneralScalarExpression("RAND", Array(LiteralValue(17L, LongType))))

    checkRoundTripConversion(
      catalystExpr = Abs(Literal(-5), failOnError = true),
      v2Expr = new GeneralScalarExpression("ABS", Array(LiteralValue(-5, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = UnaryMinus(Literal(5), failOnError = true),
      v2Expr = new GeneralScalarExpression("-", Array(LiteralValue(5, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Log2(Literal(8)),
      v2Expr = new GeneralScalarExpression("LOG2", Array(LiteralValue(8, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Log(Literal(100)),
      v2Expr = new GeneralScalarExpression("LN", Array(LiteralValue(100, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Exp(Literal(1.0)),
      v2Expr = new GeneralScalarExpression("EXP", Array(LiteralValue(1.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Sqrt(Literal(4.0)),
      v2Expr = new GeneralScalarExpression("SQRT", Array(LiteralValue(4.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Floor(Literal(4.7)),
      v2Expr = new GeneralScalarExpression("FLOOR", Array(LiteralValue(4.7, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Ceil(Literal(4.3)),
      v2Expr = new GeneralScalarExpression("CEIL", Array(LiteralValue(4.3, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Sin(Literal(0)),
      v2Expr = new GeneralScalarExpression("SIN", Array(LiteralValue(0, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Sinh(Literal(0)),
      v2Expr = new GeneralScalarExpression("SINH", Array(LiteralValue(0, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Cos(Literal(0)),
      v2Expr = new GeneralScalarExpression("COS", Array(LiteralValue(0, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Cosh(Literal(0)),
      v2Expr = new GeneralScalarExpression("COSH", Array(LiteralValue(0, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Tan(Literal(0)),
      v2Expr = new GeneralScalarExpression("TAN", Array(LiteralValue(0, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Tanh(Literal(0)),
      v2Expr = new GeneralScalarExpression("TANH", Array(LiteralValue(0, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Cot(Literal(1)),
      v2Expr = new GeneralScalarExpression("COT", Array(LiteralValue(1, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Asin(Literal(1.0)),
      v2Expr = new GeneralScalarExpression("ASIN", Array(LiteralValue(1.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Asinh(Literal(1.0)),
      v2Expr = new GeneralScalarExpression("ASINH", Array(LiteralValue(1.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Acos(Literal(1.0)),
      v2Expr = new GeneralScalarExpression("ACOS", Array(LiteralValue(1.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Acosh(Literal(1.0)),
      v2Expr = new GeneralScalarExpression("ACOSH", Array(LiteralValue(1.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Atan(Literal(1.0)),
      v2Expr = new GeneralScalarExpression("ATAN", Array(LiteralValue(1.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Atanh(Literal(0.5)),
      v2Expr = new GeneralScalarExpression("ATANH", Array(LiteralValue(0.5, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Cbrt(Literal(8.0)),
      v2Expr = new GeneralScalarExpression("CBRT", Array(LiteralValue(8.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = ToDegrees(Literal(3.14)),
      v2Expr = new GeneralScalarExpression("DEGREES", Array(LiteralValue(3.14, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = ToRadians(Literal(180.0)),
      v2Expr = new GeneralScalarExpression("RADIANS", Array(LiteralValue(180.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Signum(Literal(-42)),
      v2Expr = new GeneralScalarExpression("SIGN", Array(LiteralValue(-42, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Add(Literal(1), Literal(2), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "+",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Subtract(Literal(5), Literal(3), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "-",
        Array(LiteralValue(5, IntegerType), LiteralValue(3, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Multiply(Literal(2), Literal(4), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "*",
        Array(LiteralValue(2, IntegerType), LiteralValue(4, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Divide(Literal(10), Literal(2), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "/",
        Array(LiteralValue(10, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Remainder(Literal(7), Literal(3), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "%",
        Array(LiteralValue(7, IntegerType), LiteralValue(3, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Logarithm(Literal(10), Literal(100)),
      v2Expr = new GeneralScalarExpression(
        "LOG",
        Array(LiteralValue(10, IntegerType), LiteralValue(100, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Pow(Literal(2), Literal(3)),
      v2Expr = new GeneralScalarExpression(
        "POWER",
        Array(LiteralValue(2, IntegerType), LiteralValue(3, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Round(Literal(123.456), Literal(2), ansiEnabled = true),
      v2Expr = new GeneralScalarExpression(
        "ROUND",
        Array(LiteralValue(123.456, DoubleType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Atan2(Literal(1.0), Literal(1.0)),
      v2Expr = new GeneralScalarExpression(
        "ATAN2",
        Array(LiteralValue(1.0, DoubleType), LiteralValue(1.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Coalesce(Seq(Literal(null, IntegerType), Literal(5))),
      v2Expr = new GeneralScalarExpression(
        "COALESCE",
        Array(LiteralValue(null, IntegerType), LiteralValue(5, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Greatest(Seq(Literal(1), Literal(2))),
      v2Expr = new GeneralScalarExpression(
        "GREATEST",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Least(Seq(Literal(1), Literal(2))),
      v2Expr = new GeneralScalarExpression(
        "LEAST",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = WidthBucket(Literal(5), Literal(0), Literal(10), Literal(5)),
      v2Expr = new GeneralScalarExpression(
        "WIDTH_BUCKET",
        Array(
          LiteralValue(5, IntegerType),
          LiteralValue(0, IntegerType),
          LiteralValue(10, IntegerType),
          LiteralValue(5, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Sqrt(Pow(Abs(Literal(-3), failOnError = true), Literal(2))),
      v2Expr = new GeneralScalarExpression(
        "SQRT",
        Array(
          new GeneralScalarExpression(
            "POWER",
            Array(new GeneralScalarExpression("ABS", Array(LiteralValue(-3, IntegerType))),
          LiteralValue(2, IntegerType))))))
  }

  test("round trip conversion of bitwise functions") {
    checkRoundTripConversion(
      catalystExpr = BitwiseNot(Literal(5)),
      v2Expr = new GeneralScalarExpression("~", Array(LiteralValue(5, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = BitwiseAnd(Literal(6), Literal(3)),
      v2Expr = new GeneralScalarExpression("&", Array(
        LiteralValue(6, IntegerType),
        LiteralValue(3, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = BitwiseOr(Literal(4), Literal(1)),
      v2Expr = new GeneralScalarExpression("|", Array(
        LiteralValue(4, IntegerType),
        LiteralValue(1, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = BitwiseXor(Literal(12), Literal(5)),
      v2Expr = new GeneralScalarExpression("^", Array(
        LiteralValue(12, IntegerType),
        LiteralValue(5, IntegerType))))
  }

  test("round trip conversion of predicate expressions") {
    checkRoundTripConversion(
      catalystExpr = IsNull($"a".boolean),
      v2Expr = new Predicate("IS_NULL", Array(FieldReference("a"))))

    checkRoundTripConversion(
      catalystExpr = IsNotNull($"a".boolean),
      v2Expr = new Predicate("IS_NOT_NULL", Array(FieldReference("a"))))

    checkV2Conversion(
      catalystExpr = Not($"a".boolean),
      v2Expr = new V2Not(new Predicate(
        "=",
        Array(FieldReference("a"), LiteralValue(true, BooleanType)))))

    checkCatalystConversion(
      v2Expr = new V2Not(new Predicate(
        "=",
        Array(FieldReference("a"), LiteralValue(true, BooleanType)))),
      catalystExpr = Not(EqualTo($"a".boolean, Literal(true))))

    checkRoundTripConversion(
      catalystExpr = EqualTo(Literal(1), Literal(2)),
      v2Expr = new Predicate(
        "=",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = EqualNullSafe(Literal(1), Literal(2)),
      v2Expr = new Predicate(
        "<=>",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = GreaterThan(Literal(1), Literal(2)),
      v2Expr = new Predicate(
        ">",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = GreaterThanOrEqual(Literal(1), Literal(2)),
      v2Expr = new Predicate(
        ">=",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = LessThan(Literal(1), Literal(2)),
      v2Expr = new Predicate(
        "<",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = LessThanOrEqual(Literal(1), Literal(2)),
      v2Expr = new Predicate(
        "<=",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Not(EqualTo(Literal(1), Literal(2))),
      v2Expr = new Predicate(
        "<>",
        Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = And(Literal.TrueLiteral, Literal.FalseLiteral),
      v2Expr = new V2And(new AlwaysTrue, new AlwaysFalse))

    checkRoundTripConversion(
      catalystExpr = Or(Literal.TrueLiteral, Literal.FalseLiteral),
      v2Expr = new V2Or(new AlwaysTrue, new AlwaysFalse))

    checkRoundTripConversion(
      catalystExpr = StartsWith($"a".string, Literal("foo")),
      v2Expr = new Predicate(
        "STARTS_WITH",
        Array(FieldReference("a"), LiteralValue(UTF8String.fromString("foo"), StringType))))

    checkRoundTripConversion(
      catalystExpr = EndsWith($"a".string, Literal("bar")),
      v2Expr = new Predicate(
        "ENDS_WITH",
        Array(FieldReference("a"), LiteralValue(UTF8String.fromString("bar"), StringType))))

    checkRoundTripConversion(
      catalystExpr = Contains($"a".string, Literal("baz")),
      v2Expr = new Predicate(
        "CONTAINS",
        Array(FieldReference("a"), LiteralValue(UTF8String.fromString("baz"), StringType))))

    checkRoundTripConversion(
      catalystExpr = In($"a".int, Seq(Literal(1), Literal(2), Literal(3))),
      v2Expr = new Predicate("IN", Array(
        FieldReference("a"),
        LiteralValue(1, IntegerType),
        LiteralValue(2, IntegerType),
        LiteralValue(3, IntegerType))))
  }

  /**
   * Translate the given Catalyst [[Expression]] into data source V2 [[Predicate]]
   * then verify against the given [[Predicate]].
   */
  def testTranslateFilter(catalystFilter: Expression, result: Option[Predicate]): Unit = {
    assertResult(result) {
      DataSourceV2Strategy.translateFilterV2(catalystFilter)
    }
  }

  private def checkV2Conversion(
      catalystExpr: Expression,
      v2Expr: V2Expression,
      isPredicate: Boolean = false): Unit = {
    val v2ExprActual = new V2ExpressionBuilder(catalystExpr, isPredicate).build().getOrElse {
      fail(s"can't convert to V2 expression: $catalystExpr")
    }
    assert(v2ExprActual == v2Expr, "V2 expressions must match")
  }

  private def checkCatalystConversion(
      v2Expr: V2Expression,
      catalystExpr: Expression): Unit = {
    val catalystExprActual = V2ExpressionUtils.toCatalyst(v2Expr).getOrElse {
      fail(s"can't convert to Catalyst expression: $v2Expr")
    }
    val catalystExprExpected = catalystExpr.transform {
      case attr: Attribute => UnresolvedAttribute(attr.name)
    }
    assert(catalystExprActual == catalystExprExpected, "V1 expressions must match")
  }

  private def checkRoundTripConversion(
      catalystExpr: Expression,
      v2Expr: V2Expression,
      isPredicate: Boolean = false): Unit = {
    checkV2Conversion(catalystExpr, v2Expr, isPredicate)
    checkCatalystConversion(v2Expr, catalystExpr)
  }
}
