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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.connector.util.CustomOperatorParserExtension
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class DataSourceV2StrategySuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)

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
    val intCol = $"cint".int
    val intColRef = FieldReference("cint")
    // CASE WHEN cond1 THEN value1 WHEN cond2 THEN value2
    checkRoundTripConversion(
      catalystExpr = CaseWhen(
        Seq(
          (EqualTo(intCol, Literal(2)), Literal("a")),
          (EqualTo(intCol, Literal(4)), Literal("b"))),
        None),
      v2Expr = new GeneralScalarExpression(
        "CASE_WHEN",
        Array(
          new Predicate("=", Array(intColRef, LiteralValue(2, IntegerType))),
          LiteralValue(UTF8String.fromString("a"), StringType),
          new Predicate("=", Array(intColRef, LiteralValue(4, IntegerType))),
          LiteralValue(UTF8String.fromString("b"), StringType))))

    // CASE WHEN cond1 THEN value1 ELSE elseValue
    checkRoundTripConversion(
      catalystExpr = CaseWhen(
        Seq((EqualTo(intCol, Literal(2)), Literal("yes"))),
        Some(Literal("no"))),
      v2Expr = new GeneralScalarExpression(
        "CASE_WHEN",
        Array(
          new Predicate("=", Array(intColRef, LiteralValue(2, IntegerType))),
          LiteralValue(UTF8String.fromString("yes"), StringType),
          LiteralValue(UTF8String.fromString("no"), StringType))))

    // CASE WHEN cond1 THEN true ELSE false
    checkRoundTripConversion(
      catalystExpr = CaseWhen(
        Seq((EqualTo(intCol, Literal(2)), Literal(true))),
        Some(Literal(false))),
      v2Expr = new Predicate(
        "CASE_WHEN",
        Array(
          new Predicate("=", Array(intColRef, LiteralValue(2, IntegerType))),
          new AlwaysTrue,
          new AlwaysFalse)),
      isPredicate = true)

    // CASE WHEN cond1 THEN true WHEN cond2 THEN false ELSE true
    checkRoundTripConversion(
      catalystExpr = CaseWhen(
        Seq(
          (EqualTo(intCol, Literal(2)), Literal(true)),
          (EqualTo(intCol, Literal(4)), Literal(false))),
        Some(Literal(true))),
      v2Expr = new Predicate(
        "CASE_WHEN",
        Array(
          new Predicate("=", Array(intColRef, LiteralValue(2, IntegerType))),
          new AlwaysTrue,
          new Predicate("=", Array(intColRef, LiteralValue(4, IntegerType))),
          new AlwaysFalse,
          new AlwaysTrue)),
      isPredicate = true)
  }

  test("round trip conversion of math functions") {
    val intCol = $"cint".int
    val intColRef = FieldReference("cint")
    val doubleCol = $"cdouble".double
    val doubleColRef = FieldReference("cdouble")
    checkRoundTripConversion(
      catalystExpr = Log10(intCol),
      v2Expr = new GeneralScalarExpression("LOG10", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = new Rand(),
      v2Expr = new GeneralScalarExpression("RAND", Array()))

    checkRoundTripConversion(
      catalystExpr = new Rand(intCol),
      v2Expr = new GeneralScalarExpression("RAND", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Abs(intCol, failOnError = true),
      v2Expr = new GeneralScalarExpression("ABS", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = UnaryMinus(intCol, failOnError = true),
      v2Expr = new GeneralScalarExpression("-", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Log2(intCol),
      v2Expr = new GeneralScalarExpression("LOG2", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Log(intCol),
      v2Expr = new GeneralScalarExpression("LN", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Exp(doubleCol),
      v2Expr = new GeneralScalarExpression("EXP", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Sqrt(doubleCol),
      v2Expr = new GeneralScalarExpression("SQRT", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Floor(doubleCol),
      v2Expr = new GeneralScalarExpression("FLOOR", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Ceil(doubleCol),
      v2Expr = new GeneralScalarExpression("CEIL", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Sin(intCol),
      v2Expr = new GeneralScalarExpression("SIN", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Sinh(intCol),
      v2Expr = new GeneralScalarExpression("SINH", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Cos(intCol),
      v2Expr = new GeneralScalarExpression("COS", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Cosh(intCol),
      v2Expr = new GeneralScalarExpression("COSH", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Tan(intCol),
      v2Expr = new GeneralScalarExpression("TAN", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Tanh(intCol),
      v2Expr = new GeneralScalarExpression("TANH", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Cot(intCol),
      v2Expr = new GeneralScalarExpression("COT", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Asin(doubleCol),
      v2Expr = new GeneralScalarExpression("ASIN", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Asinh(doubleCol),
      v2Expr = new GeneralScalarExpression("ASINH", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Acos(doubleCol),
      v2Expr = new GeneralScalarExpression("ACOS", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Acosh(doubleCol),
      v2Expr = new GeneralScalarExpression("ACOSH", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Atan(doubleCol),
      v2Expr = new GeneralScalarExpression("ATAN", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Atanh(doubleCol),
      v2Expr = new GeneralScalarExpression("ATANH", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Cbrt(doubleCol),
      v2Expr = new GeneralScalarExpression("CBRT", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = ToDegrees(doubleCol),
      v2Expr = new GeneralScalarExpression("DEGREES", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = ToRadians(doubleCol),
      v2Expr = new GeneralScalarExpression("RADIANS", Array(doubleColRef)))

    checkRoundTripConversion(
      catalystExpr = Signum(intCol),
      v2Expr = new GeneralScalarExpression("SIGN", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = Add(intCol, Literal(2), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "+",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Subtract(intCol, Literal(3), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "-",
        Array(intColRef, LiteralValue(3, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Multiply(intCol, Literal(4), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "*",
        Array(intColRef, LiteralValue(4, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Divide(intCol, Literal(2), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "/",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Remainder(intCol, Literal(3), EvalMode.ANSI),
      v2Expr = new GeneralScalarExpression(
        "%",
        Array(intColRef, LiteralValue(3, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Logarithm(Literal(10), intCol),
      v2Expr = new GeneralScalarExpression(
        "LOG",
        Array(LiteralValue(10, IntegerType), intColRef)))

    checkRoundTripConversion(
      catalystExpr = Pow(intCol, Literal(3)),
      v2Expr = new GeneralScalarExpression(
        "POWER",
        Array(intColRef, LiteralValue(3, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Round(doubleCol, Literal(2), ansiEnabled = true),
      v2Expr = new GeneralScalarExpression(
        "ROUND",
        Array(doubleColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Atan2(doubleCol, Literal(1.0)),
      v2Expr = new GeneralScalarExpression(
        "ATAN2",
        Array(doubleColRef, LiteralValue(1.0, DoubleType))))

    checkRoundTripConversion(
      catalystExpr = Coalesce(Seq(Literal(null, IntegerType), intCol)),
      v2Expr = new GeneralScalarExpression(
        "COALESCE",
        Array(LiteralValue(null, IntegerType), intColRef)))

    checkRoundTripConversion(
      catalystExpr = Greatest(Seq(intCol, Literal(2))),
      v2Expr = new GeneralScalarExpression(
        "GREATEST",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Least(Seq(intCol, Literal(2))),
      v2Expr = new GeneralScalarExpression(
        "LEAST",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = WidthBucket(intCol, Literal(0), Literal(10), Literal(5)),
      v2Expr = new GeneralScalarExpression(
        "WIDTH_BUCKET",
        Array(
          intColRef,
          LiteralValue(0, IntegerType),
          LiteralValue(10, IntegerType),
          LiteralValue(5, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Sqrt(Pow(Abs(intCol, failOnError = true), Literal(2))),
      v2Expr = new GeneralScalarExpression(
        "SQRT",
        Array(
          new GeneralScalarExpression(
            "POWER",
            Array(new GeneralScalarExpression("ABS", Array(intColRef)),
              LiteralValue(2, IntegerType))))))
  }

  test("round trip conversion of bitwise functions") {
    val intCol = $"cint".int
    val intColRef = FieldReference("cint")

    checkRoundTripConversion(
      catalystExpr = BitwiseNot(intCol),
      v2Expr = new GeneralScalarExpression("~", Array(intColRef)))

    checkRoundTripConversion(
      catalystExpr = BitwiseAnd(intCol, Literal(3)),
      v2Expr = new GeneralScalarExpression("&", Array(
        intColRef,
        LiteralValue(3, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = BitwiseOr(intCol, Literal(1)),
      v2Expr = new GeneralScalarExpression("|", Array(
        intColRef,
        LiteralValue(1, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = BitwiseXor(intCol, Literal(5)),
      v2Expr = new GeneralScalarExpression("^", Array(
        intColRef,
        LiteralValue(5, IntegerType))))
  }

  test("round trip conversion of predicate expressions") {
    val intCol = $"cint".int
    val intColRef = FieldReference("cint")
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
      catalystExpr = EqualTo(intCol, Literal(2)),
      v2Expr = new Predicate(
        "=",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = EqualNullSafe(intCol, Literal(2)),
      v2Expr = new Predicate(
        "<=>",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = GreaterThan(intCol, Literal(2)),
      v2Expr = new Predicate(
        ">",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = GreaterThanOrEqual(intCol, Literal(2)),
      v2Expr = new Predicate(
        ">=",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = LessThan(intCol, Literal(2)),
      v2Expr = new Predicate(
        "<",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = LessThanOrEqual(intCol, Literal(2)),
      v2Expr = new Predicate(
        "<=",
        Array(intColRef, LiteralValue(2, IntegerType))))

    checkRoundTripConversion(
      catalystExpr = Not(EqualTo(intCol, Literal(2))),
      v2Expr = new Predicate(
        "<>",
        Array(intColRef, LiteralValue(2, IntegerType))))

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

  test("Constant foldable CASE_WHEN expression") {
    checkV2Conversion(
      catalystExpr = CaseWhen(
        Seq(
          (EqualTo(Literal(1), Literal(2)), Literal("a")),
          (EqualTo(Literal(3), Literal(3)), Literal("b"))),
        None),
      v2Expr = LiteralValue(UTF8String.fromString("b"), StringType)
    )

    checkV2Conversion(
      catalystExpr = CaseWhen(
        Seq((EqualTo(Literal(1), Literal(1)), Literal("yes"))),
        Some(Literal("no"))),
      v2Expr = LiteralValue(UTF8String.fromString("yes"), StringType)
    )
  }

  test("Constant foldable math functions") {
    checkV2Conversion(
      catalystExpr = Log10(Literal(100.0)),
      v2Expr = LiteralValue(2.0, DoubleType)
    )

    checkV2Conversion(
      catalystExpr = Abs(Literal(-5), failOnError = true),
      v2Expr = LiteralValue(5, IntegerType)
    )

    checkV2Conversion(
      catalystExpr = UnaryMinus(Literal(5), failOnError = true),
      v2Expr = LiteralValue(-5, IntegerType)
    )

    checkV2Conversion(
      catalystExpr = Log2(Literal(8.0)),
      v2Expr = LiteralValue(3.0, DoubleType)
    )

    checkV2Conversion(
      catalystExpr = Sqrt(Literal(4.0)),
      v2Expr = LiteralValue(2.0, DoubleType)
    )

    checkV2Conversion(
      catalystExpr = Floor(Literal(3.7)),
      v2Expr = LiteralValue(3L, LongType)
    )

    checkV2Conversion(
      catalystExpr = Ceil(Literal(3.1)),
      v2Expr = LiteralValue(4L, LongType)
    )
  }

  test("Partial constant folding of math functions") {
    checkV2Conversion(
      catalystExpr = Log10(Literal(100.0)) + $"cint".int,
      v2Expr = new GeneralScalarExpression("+", Array(
        LiteralValue(2.0, DoubleType),
        FieldReference("cint"))))

    checkV2Conversion(
      catalystExpr = Abs(Literal(-10), failOnError = true) * $"cdouble".double,
      v2Expr = new GeneralScalarExpression("*", Array(
        LiteralValue(10, IntegerType),
        FieldReference("cdouble"))))

    checkV2Conversion(
      catalystExpr = Sqrt(Literal(16.0)) - $"cint".int,
      v2Expr = new GeneralScalarExpression("-", Array(
        LiteralValue(4.0, DoubleType),
        FieldReference("cint"))))

    checkV2Conversion(
      catalystExpr = $"cdouble".double / Log2(Literal(32.0)),
      v2Expr = new GeneralScalarExpression("/", Array(
        FieldReference("cdouble"),
        LiteralValue(5.0, DoubleType))))

    checkV2Conversion(
      catalystExpr = Floor(Literal(7.9)) + Ceil(Literal(2.1)),
      v2Expr = LiteralValue(10L, LongType))

    checkV2Conversion(
      catalystExpr = $"cint".int % Abs(Literal(-3), failOnError = true),
      v2Expr = new GeneralScalarExpression("%", Array(
        FieldReference("cint"),
        LiteralValue(3, IntegerType))))

    checkV2Conversion(
      catalystExpr = Exp(Literal(0.0)) * $"cdouble".double,
      v2Expr = new GeneralScalarExpression("*", Array(
        LiteralValue(1.0, DoubleType),
        FieldReference("cdouble"))))
  }

  test("Current Like functions are not supported") {
    val currentFunctions = Seq(
      CurrentDate(),
      CurrentTimestamp(),
      CurrentUser()
    )

    currentFunctions.foreach { catalystExpr =>
      assert(new V2ExpressionBuilder(catalystExpr).build().isEmpty)
    }
  }

  test("SPARK-53474: Check failure when datasourceV2ExprFolding = false") {
    // when spark.sql.optimizer.datasourceV2ExprFolding = true
    // expression will first convert to V2 expressions, then fold to constant
    val expr = Abs(Literal(-5), failOnError = true)
    checkV2Conversion(expr, LiteralValue(5, IntegerType))

    withSQLConf(SQLConf.DATA_SOURCE_V2_EXPR_FOLDING.key -> "false") {
      // when spark.sql.optimizer.datasourceV2ExprFolding = false
      // expression will be converted to V2 expressions, but not folded
      checkV2Conversion(expr,
        new GeneralScalarExpression("ABS", Array(LiteralValue(-5, IntegerType))))
    }
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

  /**
   * Translate with extra capabilities and verify against the expected [[Predicate]].
   */
  def testTranslateFilterWithCaps(
      catalystFilter: Expression,
      result: Option[Predicate],
      extraCapabilities: Set[String]): Unit = {
    assertResult(result) {
      DataSourceV2Strategy.translateFilterV2WithMapping(
        catalystFilter, None, extraCapabilities)
    }
  }

  test("capability-gated LIKE predicate translation") {
    val strCol = $"cstr".string
    val likeExpr = Like(strCol, Literal("%pattern%"), '\\')

    // Without capability: LIKE should NOT translate
    testTranslateFilterWithCaps(likeExpr, None, Set.empty)

    // With capability: LIKE should translate
    testTranslateFilterWithCaps(likeExpr,
      Some(new Predicate("LIKE", Array(
        FieldReference("cstr"),
        LiteralValue(UTF8String.fromString("%pattern%"), StringType)))),
      Set("LIKE"))
  }

  test("capability-gated RLIKE predicate translation") {
    val strCol = $"cstr".string
    val rlikeExpr = RLike(strCol, Literal("^abc.*"))

    // Without capability: RLIKE should NOT translate
    testTranslateFilterWithCaps(rlikeExpr, None, Set.empty)

    // With capability: RLIKE should translate
    testTranslateFilterWithCaps(rlikeExpr,
      Some(new Predicate("RLIKE", Array(
        FieldReference("cstr"),
        LiteralValue(UTF8String.fromString("^abc.*"), StringType)))),
      Set("RLIKE"))
  }

  test("capability-gated IS_NAN predicate translation") {
    val doubleCol = $"cdouble".double
    val isNanExpr = IsNaN(doubleCol)

    // Without capability: IS_NAN should NOT translate
    testTranslateFilterWithCaps(isNanExpr, None, Set.empty)

    // With capability: IS_NAN should translate
    testTranslateFilterWithCaps(isNanExpr,
      Some(new Predicate("IS_NAN", Array(FieldReference("cdouble")))),
      Set("IS_NAN"))
  }

  test("capability-gated ARRAY_CONTAINS predicate translation") {
    val arrCol = AttributeReference("carr", ArrayType(IntegerType))()
    val arrContainsExpr = ArrayContains(arrCol, Literal(42))

    // Without capability: should NOT translate
    testTranslateFilterWithCaps(arrContainsExpr, None, Set.empty)

    // With capability: should translate
    testTranslateFilterWithCaps(arrContainsExpr,
      Some(new Predicate("ARRAY_CONTAINS", Array(
        FieldReference("carr"),
        LiteralValue(42, IntegerType)))),
      Set("ARRAY_CONTAINS"))
  }

  test("capability-gated MAP_CONTAINS_KEY predicate translation") {
    val mapCol = AttributeReference("cmap", MapType(StringType, IntegerType))()
    // MapContainsKey RuntimeReplaceable form: ArrayContains(MapKeys(map), key)
    val mapContainsKeyExpr = ArrayContains(MapKeys(mapCol), Literal("mykey"))

    // Without capability: should NOT translate (not even as ARRAY_CONTAINS)
    testTranslateFilterWithCaps(mapContainsKeyExpr, None, Set.empty)

    // With MAP_CONTAINS_KEY capability: should translate as MAP_CONTAINS_KEY
    testTranslateFilterWithCaps(mapContainsKeyExpr,
      Some(new Predicate("MAP_CONTAINS_KEY", Array(
        FieldReference("cmap"),
        LiteralValue(UTF8String.fromString("mykey"), StringType)))),
      Set("MAP_CONTAINS_KEY"))

    // With both MAP_CONTAINS_KEY and ARRAY_CONTAINS: MAP_CONTAINS_KEY takes precedence
    testTranslateFilterWithCaps(mapContainsKeyExpr,
      Some(new Predicate("MAP_CONTAINS_KEY", Array(
        FieldReference("cmap"),
        LiteralValue(UTF8String.fromString("mykey"), StringType)))),
      Set("MAP_CONTAINS_KEY", "ARRAY_CONTAINS"))

    // With only ARRAY_CONTAINS (not MAP_CONTAINS_KEY): should NOT translate
    // because ArrayContains(MapKeys(map), key) is not a plain ArrayContains on a column
    testTranslateFilterWithCaps(mapContainsKeyExpr, None, Set("ARRAY_CONTAINS"))
  }

  test("capability-gated ARRAYS_OVERLAP predicate translation") {
    val arr1 = AttributeReference("carr1", ArrayType(IntegerType))()
    val arr2 = AttributeReference("carr2", ArrayType(IntegerType))()
    val expr = ArraysOverlap(arr1, arr2)

    // Without capability: should NOT translate
    testTranslateFilterWithCaps(expr, None, Set.empty)

    // With capability: should translate
    testTranslateFilterWithCaps(expr,
      Some(new Predicate("ARRAYS_OVERLAP", Array(
        FieldReference("carr1"),
        FieldReference("carr2")))),
      Set("ARRAYS_OVERLAP"))
  }

  test("capability-gated LIKE_ALL predicate translation") {
    val strCol = $"cstr".string
    val patterns = Seq(
      UTF8String.fromString("%a%"),
      UTF8String.fromString("%b%"))
    val likeAllExpr = LikeAll(strCol, patterns)

    // Without capability: should NOT translate
    testTranslateFilterWithCaps(likeAllExpr, None, Set.empty)

    // With capability: should translate
    testTranslateFilterWithCaps(likeAllExpr,
      Some(new Predicate("LIKE_ALL", Array(
        FieldReference("cstr"),
        LiteralValue(UTF8String.fromString("%a%"), StringType),
        LiteralValue(UTF8String.fromString("%b%"), StringType)))),
      Set("LIKE_ALL"))
  }

  test("capability-gated LIKE_ANY predicate translation") {
    val strCol = $"cstr".string
    val patterns = Seq(
      UTF8String.fromString("%x%"),
      UTF8String.fromString("%y%"))
    val likeAnyExpr = LikeAny(strCol, patterns)

    // Without capability: should NOT translate
    testTranslateFilterWithCaps(likeAnyExpr, None, Set.empty)

    // With capability: should translate
    testTranslateFilterWithCaps(likeAnyExpr,
      Some(new Predicate("LIKE_ANY", Array(
        FieldReference("cstr"),
        LiteralValue(UTF8String.fromString("%x%"), StringType),
        LiteralValue(UTF8String.fromString("%y%"), StringType)))),
      Set("LIKE_ANY"))
  }

  test("capability-gated NOT_LIKE_ALL predicate translation") {
    val strCol = $"cstr".string
    val patterns = Seq(UTF8String.fromString("%z%"))
    val notLikeAllExpr = NotLikeAll(strCol, patterns)

    // Without capability: should NOT translate
    testTranslateFilterWithCaps(notLikeAllExpr, None, Set.empty)

    // With capability: should translate
    testTranslateFilterWithCaps(notLikeAllExpr,
      Some(new Predicate("NOT_LIKE_ALL", Array(
        FieldReference("cstr"),
        LiteralValue(UTF8String.fromString("%z%"), StringType)))),
      Set("NOT_LIKE_ALL"))
  }

  test("capability-gated NOT_LIKE_ANY predicate translation") {
    val strCol = $"cstr".string
    val patterns = Seq(
      UTF8String.fromString("a%"),
      UTF8String.fromString("b%"))
    val notLikeAnyExpr = NotLikeAny(strCol, patterns)

    // Without capability: should NOT translate
    testTranslateFilterWithCaps(notLikeAnyExpr, None, Set.empty)

    // With capability: should translate
    testTranslateFilterWithCaps(notLikeAnyExpr,
      Some(new Predicate("NOT_LIKE_ANY", Array(
        FieldReference("cstr"),
        LiteralValue(UTF8String.fromString("a%"), StringType),
        LiteralValue(UTF8String.fromString("b%"), StringType)))),
      Set("NOT_LIKE_ANY"))
  }

  test("capability-gated ILIKE predicate translation") {
    val strCol = $"cstr".string
    // ILike RuntimeReplaceable form: Like(Lower(left), Lower(right))
    // This pattern only survives when the pattern is non-literal (non-constant)
    val ilikeExpr = Like(Lower(strCol), Lower($"cpattern".string), '\\')

    // Without capability: should NOT translate
    testTranslateFilterWithCaps(ilikeExpr, None, Set.empty)

    // With ILIKE capability: should translate as ILIKE
    testTranslateFilterWithCaps(ilikeExpr,
      Some(new Predicate("ILIKE", Array(
        FieldReference("cstr"),
        FieldReference("cpattern")))),
      Set("ILIKE"))

    // With both ILIKE and LIKE: ILIKE takes precedence for Like(Lower, Lower) pattern
    testTranslateFilterWithCaps(ilikeExpr,
      Some(new Predicate("ILIKE", Array(
        FieldReference("cstr"),
        FieldReference("cpattern")))),
      Set("ILIKE", "LIKE"))

    // With only LIKE (not ILIKE): should translate as LIKE (falls through to generic LIKE)
    testTranslateFilterWithCaps(ilikeExpr,
      Some(new Predicate("LIKE", Array(
        new GeneralScalarExpression("LOWER", Array[V2Expression](FieldReference("cstr"))),
        new GeneralScalarExpression("LOWER",
          Array[V2Expression](FieldReference("cpattern")))))),
      Set("LIKE"))
  }

  test("capability-gated predicates combined with AND/OR") {
    val strCol = $"cstr".string
    val intCol = $"cint".int
    val likeExpr = Like(strCol, Literal("%test%"), '\\')
    val eqExpr = EqualTo(intCol, Literal(1))

    // LIKE inside AND: with capability, both sides should translate
    testTranslateFilterWithCaps(And(likeExpr, eqExpr),
      Some(new V2And(
        new Predicate("LIKE", Array(
          FieldReference("cstr"),
          LiteralValue(UTF8String.fromString("%test%"), StringType))),
        new Predicate("=", Array(
          FieldReference("cint"),
          LiteralValue(1, IntegerType))))),
      Set("LIKE"))

    // LIKE inside AND: without capability, entire AND should fail
    testTranslateFilterWithCaps(And(likeExpr, eqExpr), None, Set.empty)

    // LIKE inside OR: with capability, both sides should translate
    testTranslateFilterWithCaps(Or(likeExpr, eqExpr),
      Some(new V2Or(
        new Predicate("LIKE", Array(
          FieldReference("cstr"),
          LiteralValue(UTF8String.fromString("%test%"), StringType))),
        new Predicate("=", Array(
          FieldReference("cint"),
          LiteralValue(1, IntegerType))))),
      Set("LIKE"))

    // LIKE inside OR: without capability, entire OR should fail
    testTranslateFilterWithCaps(Or(likeExpr, eqExpr), None, Set.empty)
  }

  test("capability-gated predicates do not affect default translations") {
    val intCol = $"cint".int
    val strCol = $"cstr".string

    // Standard predicates still work regardless of extraCapabilities
    Seq(Set.empty[String], Set("LIKE", "RLIKE", "IS_NAN")).foreach { caps =>
      testTranslateFilterWithCaps(EqualTo(intCol, Literal(1)),
        Some(new Predicate("=", Array(
          FieldReference("cint"), LiteralValue(1, IntegerType)))),
        caps)
      testTranslateFilterWithCaps(IsNull(intCol),
        Some(new Predicate("IS_NULL", Array(FieldReference("cint")))),
        caps)
      testTranslateFilterWithCaps(StartsWith(strCol, Literal("abc")),
        Some(new Predicate("STARTS_WITH", Array(
          FieldReference("cstr"),
          LiteralValue(UTF8String.fromString("abc"), StringType)))),
        caps)
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

  // Tests for SPARK-55869 Layer 3: CustomOperatorParserExtension

  private def createTestExtension(
      ops: Map[String, String]): CustomOperatorParserExtension = {
    val delegate = spark.sessionState.sqlParser
    new CustomOperatorParserExtension(delegate) {
      override def customOperators: Map[String, String] = ops
    }
  }

  /** Parse with the extension and extract the SQL that the delegate sees. */
  private def rewrittenSQL(
      ops: Map[String, String], sql: String): String = {
    var captured: String = null
    val delegate = spark.sessionState.sqlParser
    val ext = new CustomOperatorParserExtension(
      new org.apache.spark.sql.catalyst.parser.ParserInterface {
        override def parsePlan(sqlText: String) = {
          captured = sqlText
          delegate.parsePlan(sqlText)
        }
        override def parseQuery(sqlText: String) = delegate.parseQuery(sqlText)
        override def parseExpression(sqlText: String) =
          delegate.parseExpression(sqlText)
        override def parseTableIdentifier(sqlText: String) =
          delegate.parseTableIdentifier(sqlText)
        override def parseFunctionIdentifier(sqlText: String) =
          delegate.parseFunctionIdentifier(sqlText)
        override def parseMultipartIdentifier(sqlText: String) =
          delegate.parseMultipartIdentifier(sqlText)
        override def parseTableSchema(sqlText: String) =
          delegate.parseTableSchema(sqlText)
        override def parseDataType(sqlText: String) =
          delegate.parseDataType(sqlText)
        override def parseRoutineParam(sqlText: String) =
          delegate.parseRoutineParam(sqlText)
      }
    ) {
      override def customOperators: Map[String, String] = ops
    }
    ext.parsePlan(sql)
    captured
  }

  test("SPARK-55869: CustomOperatorParserExtension rewrites infix to function call") {
    val ops = Map("INDEXQUERY" -> "indexquery")
    val result = rewrittenSQL(ops,
      "SELECT * FROM t WHERE col INDEXQUERY 'search term'")
    assert(result.contains("indexquery(col, 'search term')"),
      s"Expected function call syntax, got: $result")
  }

  test("SPARK-55869: CustomOperatorParserExtension is case-insensitive") {
    val ops = Map("INDEXQUERY" -> "indexquery")
    val result = rewrittenSQL(ops,
      "SELECT * FROM t WHERE col indexquery 'value'")
    assert(result.contains("indexquery(col, 'value')"),
      s"Expected function call syntax, got: $result")
  }

  test("SPARK-55869: CustomOperatorParserExtension handles identifiers") {
    val ops = Map("MYSEARCH" -> "my_search")
    val result = rewrittenSQL(ops,
      "SELECT * FROM t WHERE col1 MYSEARCH col2")
    assert(result.contains("my_search(col1, col2)"),
      s"Expected function call syntax, got: $result")
  }

  test("SPARK-55869: CustomOperatorParserExtension handles numeric literals") {
    val ops = Map("NEARBY" -> "nearby")
    val result = rewrittenSQL(ops,
      "SELECT * FROM t WHERE col NEARBY 42")
    assert(result.contains("nearby(col, 42)"),
      s"Expected function call syntax, got: $result")
  }

  test("SPARK-55869: CustomOperatorParserExtension preserves string literals") {
    val ops = Map("INDEXQUERY" -> "indexquery")
    // The operator keyword inside a string literal should NOT be rewritten
    val result = rewrittenSQL(ops,
      "SELECT 'col INDEXQUERY val' FROM t WHERE col INDEXQUERY 'term'")
    assert(result.contains("indexquery(col, 'term')"),
      s"Expected function call in WHERE, got: $result")
    assert(result.contains("'col INDEXQUERY val'"),
      s"String literal should be preserved, got: $result")
  }

  test("SPARK-55869: CustomOperatorParserExtension with multiple operators") {
    val ops = Map("INDEXQUERY" -> "indexquery", "NEARBY" -> "nearby")
    val result = rewrittenSQL(ops,
      "SELECT * FROM t WHERE col1 INDEXQUERY 'a' AND col2 NEARBY 5")
    assert(result.contains("indexquery(col1, 'a')"),
      s"Expected first operator rewrite, got: $result")
    assert(result.contains("nearby(col2, 5)"),
      s"Expected second operator rewrite, got: $result")
  }

  test("SPARK-55869: CustomOperatorParserExtension with no operators is passthrough") {
    val ops = Map.empty[String, String]
    val sql = "SELECT * FROM t WHERE col > 5"
    val result = rewrittenSQL(ops, sql)
    assert(result == sql)
  }

  test("SPARK-55869: CustomOperatorParserExtension placeholder collision resistance") {
    // SQL containing text that looks like internal placeholders should not be corrupted
    val ops = Map("INDEXQUERY" -> "indexquery")
    val result = rewrittenSQL(ops,
      "SELECT '__COPLIT_' FROM t WHERE col INDEXQUERY 'term'")
    assert(result.contains("indexquery(col, 'term')"),
      s"Expected function call syntax, got: $result")
    assert(result.contains("'__COPLIT_'"),
      s"Placeholder-like string literal should be preserved, got: $result")
  }

  test("SPARK-55869: CustomOperatorParserExtension handles complex WHERE clauses") {
    val ops = Map("INDEXQUERY" -> "indexquery")
    val result = rewrittenSQL(ops,
      "SELECT * FROM t WHERE col1 INDEXQUERY 'a' AND col2 > 5 OR col3 INDEXQUERY 'b'")
    assert(result.contains("indexquery(col1, 'a')"),
      s"Expected first operator rewrite, got: $result")
    assert(result.contains("indexquery(col3, 'b')"),
      s"Expected second operator rewrite, got: $result")
    assert(result.contains("col2 > 5"),
      s"Standard predicate should be unchanged, got: $result")
  }

  test("SPARK-55869: CustomOperatorParserExtension nested AND/OR with multiple operators") {
    val ops = Map("INDEXQUERY" -> "indexquery", "XYZQUERY" -> "xyzquery")
    val sql =
      "SELECT * FROM t WHERE (col INDEXQUERY 'value' AND col1 XYZQUERY 'value2') " +
      "OR (col2 INDEXQUERY 'value' AND " +
      "(col3 XYZQUERY 'value3' OR col4 INDEXQUERY 'value4'))"

    // Verify rewritten SQL text
    val result = rewrittenSQL(ops, sql)
    assert(result.contains("indexquery(col, 'value')"),
      s"Expected first INDEXQUERY rewrite, got: $result")
    assert(result.contains("xyzquery(col1, 'value2')"),
      s"Expected first XYZQUERY rewrite, got: $result")
    assert(result.contains("indexquery(col2, 'value')"),
      s"Expected second INDEXQUERY rewrite, got: $result")
    assert(result.contains("xyzquery(col3, 'value3')"),
      s"Expected second XYZQUERY rewrite, got: $result")
    assert(result.contains("indexquery(col4, 'value4')"),
      s"Expected third INDEXQUERY rewrite, got: $result")

    // Verify the parsed plan has the correct tree structure
    val ext = createTestExtension(ops)
    val plan = ext.parsePlan(sql)

    // Extract the filter condition from Project -> Filter -> ...
    val filterCondition = plan.collect {
      case f: org.apache.spark.sql.catalyst.plans.logical.Filter =>
        f.condition
    }.head

    // The top-level should be OR
    filterCondition match {
      case Or(
        And(left1, right1),
        And(left2, Or(innerLeft, innerRight))) =>
        // (col INDEXQUERY 'value' AND col1 XYZQUERY 'value2')
        assert(left1.isInstanceOf[UnresolvedFunction])
        assert(left1.asInstanceOf[UnresolvedFunction]
          .nameParts == Seq("indexquery"))
        assert(right1.isInstanceOf[UnresolvedFunction])
        assert(right1.asInstanceOf[UnresolvedFunction]
          .nameParts == Seq("xyzquery"))
        // (col2 INDEXQUERY 'value' AND (...))
        assert(left2.isInstanceOf[UnresolvedFunction])
        assert(left2.asInstanceOf[UnresolvedFunction]
          .nameParts == Seq("indexquery"))
        // (col3 XYZQUERY 'value3' OR col4 INDEXQUERY 'value4')
        assert(innerLeft.isInstanceOf[UnresolvedFunction])
        assert(innerLeft.asInstanceOf[UnresolvedFunction]
          .nameParts == Seq("xyzquery"))
        assert(innerRight.isInstanceOf[UnresolvedFunction])
        assert(innerRight.asInstanceOf[UnresolvedFunction]
          .nameParts == Seq("indexquery"))
      case other =>
        fail(s"Expected Or(And(...), And(..., Or(...))), got: $other")
    }
  }

  test("SPARK-55869: CustomPredicateDescriptor rejects null canonicalName") {
    val e = intercept[IllegalArgumentException] {
      new org.apache.spark.sql.connector.catalog.CustomPredicateDescriptor(
        null, "my_func", Array(IntegerType), true)
    }
    assert(e.getMessage.contains("canonicalName must not be null"))
  }

  test("SPARK-55869: CustomPredicateDescriptor rejects null sqlName") {
    val e = intercept[IllegalArgumentException] {
      new org.apache.spark.sql.connector.catalog.CustomPredicateDescriptor(
        "com.test.FUNC", null, Array(IntegerType), true)
    }
    assert(e.getMessage.contains("sqlName must not be null"))
  }

  test("SPARK-55869: CustomPredicateDescriptor rejects non-dot-qualified name") {
    val e = intercept[IllegalArgumentException] {
      new org.apache.spark.sql.connector.catalog.CustomPredicateDescriptor(
        "FUNC_NO_DOT", "func", Array(IntegerType), true)
    }
    assert(e.getMessage.contains("dot-qualified"))
  }

  test("SPARK-55869: CustomPredicateDescriptor convenience constructor derives sqlName") {
    val desc = new org.apache.spark.sql.connector.catalog.CustomPredicateDescriptor(
      "com.mycompany.MY_FUNC", Array(IntegerType), true)
    assert(desc.canonicalName() == "COM.MYCOMPANY.MY_FUNC")
    assert(desc.sqlName() == "MY_FUNC")
  }

  test("SPARK-55869: CustomOperatorParserExtension parseQuery also rewrites") {
    val ops = Map("INDEXQUERY" -> "indexquery")
    var captured: String = null
    val delegate = spark.sessionState.sqlParser
    val ext = new CustomOperatorParserExtension(
      new org.apache.spark.sql.catalyst.parser.ParserInterface {
        override def parsePlan(s: String) = delegate.parsePlan(s)
        override def parseQuery(sqlText: String) = {
          captured = sqlText
          delegate.parseQuery(sqlText)
        }
        override def parseExpression(s: String) =
          delegate.parseExpression(s)
        override def parseTableIdentifier(s: String) =
          delegate.parseTableIdentifier(s)
        override def parseFunctionIdentifier(s: String) =
          delegate.parseFunctionIdentifier(s)
        override def parseMultipartIdentifier(s: String) =
          delegate.parseMultipartIdentifier(s)
        override def parseTableSchema(s: String) =
          delegate.parseTableSchema(s)
        override def parseDataType(s: String) =
          delegate.parseDataType(s)
        override def parseRoutineParam(s: String) =
          delegate.parseRoutineParam(s)
      }
    ) {
      override def customOperators: Map[String, String] = ops
    }
    ext.parseQuery(
      "SELECT * FROM t WHERE col INDEXQUERY 'value'")
    assert(captured != null, "parseQuery should delegate")
    assert(captured.contains("indexquery(col, 'value')"),
      s"parseQuery should rewrite infix operators, got: $captured")
  }
}
