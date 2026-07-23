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

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference, GeneralScalarExpression, LiteralValue, VariantGet => V2VariantGet}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType, VariantType}
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

  test("SPARK-57512: V2 pushdown sees through a DelegateExpression wrapper") {
    val pred = GreaterThan($"cint".int, Literal(1))
    val delegate = DelegateExpression("wrap", Seq($"cint".int, Literal(1)), pred)
    // The wrapper is unfolded to its definition, so it pushes down exactly like the bare predicate.
    assert(DataSourceV2Strategy.translateFilterV2(delegate).isDefined)
    assert(DataSourceV2Strategy.translateFilterV2(delegate) ==
      DataSourceV2Strategy.translateFilterV2(pred))
  }

  test("SPARK-57512: a compound-definition DelegateExpression round-trips through filter rebuild") {
    val a = $"cint".int
    val b = $"`c.int`".int
    // The definition is a compound predicate, so the wrapper translates to a structural V2And.
    val definition = And(GreaterThan(a, Literal(1)), LessThan(b, Literal(2)))
    val delegate = DelegateExpression("wrap", Seq(a, b), definition)
    val map = mutable.HashMap.empty[Predicate, Expression]
    val translated = DataSourceV2Strategy.translateFilterV2WithMapping(delegate, Some(map))
    assert(translated.isDefined, "the compound delegate should translate via its definition")
    // The whole V2And is mapped back to the delegate (it was translated as a single leaf).
    // Rebuilding must restore the delegate via the exact map entry, not descend into the synthetic
    // children that have no map entries -- descending would throw
    // "Failed to rebuild Expression for filter".
    val rebuilt = DataSourceV2Strategy.rebuildExpressionFromFilter(translated.get, map)
    assert(rebuilt == delegate, s"expected the original delegate, got $rebuilt")
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

  test("VariantGet translates to V2VariantGet connector expression") {
    val ref = AttributeReference("v", VariantType)()
    val path = Literal.create("$.city", StringType)
    val expr = VariantGet(ref, path, StringType, failOnError = true)
    val gt = GreaterThan(expr, Literal.create("NYC", StringType))
    val result = new V2ExpressionBuilder(gt, isPredicate = true).build()
    result match {
      case Some(v2pred: Predicate) if v2pred.name() == ">" =>
        v2pred.children()(0) match {
          case vg: V2VariantGet =>
            assert(vg.path() == "$.city")
            assert(vg.targetType() == StringType)
            assert(vg.failOnError())
            assert(vg.timeZoneId() == null)
            assert(vg.children().length == 1)
            assert(vg.children()(0) == FieldReference("v"))
          case other => fail(s"expected V2VariantGet, got ${other.getClass.getName}")
        }
      case _ => fail("expected predicate with name '>'")
    }
  }

  test("try_variant_get translates with failOnError=false") {
    val ref = AttributeReference("v", VariantType)()
    val path = Literal.create("$.city", StringType)
    val expr = VariantGet(ref, path, StringType, failOnError = false)
    val gt = GreaterThan(expr, Literal.create("NYC", StringType))
    val result = new V2ExpressionBuilder(gt, isPredicate = true).build()
    result match {
      case Some(v2pred: Predicate) if v2pred.name() == ">" =>
        v2pred.children()(0) match {
          case vg: V2VariantGet =>
            assert(!vg.failOnError())
            assert(vg.path() == "$.city")
          case other => fail(s"expected V2VariantGet, got ${other.getClass.getName}")
        }
      case _ => fail("expected predicate with name '>'")
    }
  }

  test("VariantGet predicate is translated by translateFilterV2") {
    val ref = AttributeReference("v", VariantType)()
    val path = Literal.create("$.city", StringType)
    val expr = VariantGet(ref, path, StringType, failOnError = true)
    val gt = GreaterThan(expr, Literal.create("NYC", StringType))
    val result = DataSourceV2Strategy.translateFilterV2(gt)
    assert(result.isDefined)
    result.get.children()(0) match {
      case vg: V2VariantGet =>
        assert(vg.path() == "$.city")
        assert(vg.targetType() == StringType)
        assert(vg.failOnError())
      case other =>
        fail(s"expected V2VariantGet in translated predicate, got " +
          s"${other.getClass.getName}")
    }
  }

  test("VariantGet with integer targetType preserves type") {
    val ref = AttributeReference("v", VariantType)()
    val path = Literal.create("$.count", StringType)
    val expr = VariantGet(ref, path, IntegerType, failOnError = true)
    val gt = GreaterThan(expr, Literal(100))
    val result = new V2ExpressionBuilder(gt, isPredicate = true).build()
    assert(result.isDefined)
    result.get.children()(0) match {
      case vg: V2VariantGet =>
        assert(vg.path() == "$.count")
        assert(vg.targetType() == IntegerType)
      case other => fail(s"expected V2VariantGet, got ${other.getClass.getName}")
    }
  }

  test("VariantGet with non-foldable path returns None") {
    val ref = AttributeReference("v", VariantType)()
    val s = AttributeReference("s", StringType)()
    val expr = VariantGet(ref, s, StringType, failOnError = true)
    val result = new V2ExpressionBuilder(expr).build()
    assert(result.isEmpty, "non-foldable path should not translate")
  }

  test("VariantGet with foldable null path returns None") {
    val ref = AttributeReference("v", VariantType)()
    val nullPath = Literal.create(null, StringType)
    val expr = VariantGet(ref, nullPath, StringType, failOnError = true)
    val result = new V2ExpressionBuilder(expr).build()
    assert(result.isEmpty, "null path should not translate (graceful, no NPE)")
  }

  test("VariantGet with non-column child returns None") {
    val lit = Literal("v")
    val path = Literal.create("$.a", StringType)
    val expr = VariantGet(lit, path, StringType, failOnError = true)
    val result = new V2ExpressionBuilder(expr).build()
    assert(result.isEmpty, "non-column child should not translate")
  }

  test("VariantGet boolean targetType wraps in BOOLEAN_EXPRESSION predicate when isPredicate") {
    val ref = AttributeReference("v", VariantType)()
    val path = Literal.create("$.flag", StringType)
    val expr = VariantGet(ref, path, BooleanType, failOnError = true)
    val result = new V2ExpressionBuilder(expr, isPredicate = true).build()
    result match {
      case Some(p: Predicate) if p.name() == "BOOLEAN_EXPRESSION" =>
        p.children()(0) match {
          case vg: V2VariantGet =>
            assert(vg.targetType() == BooleanType)
          case other =>
            fail(s"expected V2VariantGet inside BOOLEAN_EXPRESSION, got " +
              s"${other.getClass.getName}")
        }
      case _ => fail(s"expected BOOLEAN_EXPRESSION predicate, got $result")
    }
  }

  test("VariantGet boolean targetType does not crash under Or (isPredicate path)") {
    val ref = AttributeReference("v", VariantType)()
    val path = Literal.create("$.flag", StringType)
    val boolExpr = VariantGet(ref, path, BooleanType, failOnError = true)
    val x = AttributeReference("x", IntegerType)()
    val orExpr = Or(boolExpr, GreaterThan(x, Literal(0)))
    // A boolean-typed VariantGet in predicate position must translate to a V2Predicate, or the
    // enclosing And/Or's `isInstanceOf[V2Predicate]` assert crashes planning;
    // the BOOLEAN_EXPRESSION predicate provides that.
    val result = new V2ExpressionBuilder(orExpr, isPredicate = true).build()
    assert(result.isDefined, "Or with boolean VariantGet should translate without AssertionError")
    result.get match {
      case p: Predicate => // expected
      case other => fail(s"expected a Predicate, got ${other.getClass.getName}")
    }
  }

  test("VariantGet boolean targetType is scalar when not isPredicate") {
    val ref = AttributeReference("v", VariantType)()
    val path = Literal.create("$.flag", StringType)
    val expr = VariantGet(ref, path, BooleanType, failOnError = true)
    val result = new V2ExpressionBuilder(expr, isPredicate = false).build()
    result match {
      case Some(vg: V2VariantGet) =>
        assert(vg.targetType() == BooleanType)
      case _ => fail(s"expected V2VariantGet scalar when isPredicate=false, got $result")
    }
  }

  test("V2VariantGet toString renders as variant_get SQL") {
    val ref = AttributeReference("v", VariantType)()
    val vg = new V2VariantGet(FieldReference("v"), "$.city", StringType, true, null)
    assert(vg.toString == "variant_get(v, '$.city', string)")
  }

  test("V2VariantGet toString renders as try_variant_get with timezone") {
    val vg = new V2VariantGet(FieldReference("v"), "$.ts", TimestampType, false, "UTC")
    assert(vg.toString == "try_variant_get(v, '$.ts', timestamp, tz=UTC)")
  }

  test("VariantGet with resolved timeZoneId passes it through the builder") {
    val ref = AttributeReference("v", VariantType)()
    val path = Literal.create("$.ts", StringType)
    val expr = VariantGet(ref, path, TimestampType, failOnError = true, timeZoneId = Some("UTC"))
    val gt = GreaterThan(expr, Literal.create(null, TimestampType))
    val result = new V2ExpressionBuilder(gt, isPredicate = true).build()
    assert(result.isDefined)
    result.get.children()(0) match {
      case vg: V2VariantGet =>
        assert(vg.timeZoneId() == "UTC")
        assert(vg.targetType() == TimestampType)
      case other => fail(s"expected V2VariantGet, got ${other.getClass.getName}")
    }
  }

  test("VariantGet with struct-nested variant column translates to nested FieldReference") {
    val structType = StructType(Seq(StructField("v", VariantType)))
    val parentRef = AttributeReference("s", structType)()
    val nestedVariant = GetStructField(parentRef, 0)
    val path = Literal.create("$.city", StringType)
    val expr = VariantGet(nestedVariant, path, StringType, failOnError = true)
    val gt = GreaterThan(expr, Literal.create("NYC", StringType))
    val result = new V2ExpressionBuilder(gt, isPredicate = true).build()
    assert(result.isDefined)
    result.get.children()(0) match {
      case vg: V2VariantGet =>
        assert(vg.children()(0) == FieldReference(Seq("s", "v")))
        assert(vg.path() == "$.city")
        assert(vg.targetType() == StringType)
      case other => fail(s"expected V2VariantGet with nested FieldReference, got " +
        s"${other.getClass.getName}")
    }
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
