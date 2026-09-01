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

import java.util.EnumSet

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, CombineFilters, ConstantFolding}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, Limit, LogicalPlan, Offset, OneRowRelation, Project, Sort}
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference, GeneralScalarExpression, LiteralValue, SortOrder => V2SortOrder, VariantGet => V2VariantGet}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.connector.join.{JoinType => V2JoinType}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, LocalScan, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownJoin, SupportsPushDownLimit, SupportsPushDownOffset, SupportsPushDownRequiredColumns, SupportsPushDownTopN, SupportsPushDownVariantExtractions, V1Scan, VariantExtraction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType, VariantType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
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

    withSQLConf("spark.sql.optimizer.datasourceV2ExprFolding" -> "false") {
      // when spark.sql.optimizer.datasourceV2ExprFolding = false
      // expression will be converted to V2 expressions, but not folded
      checkV2Conversion(expr,
        new GeneralScalarExpression("ABS", Array(LiteralValue(-5, IntegerType))))
    }
  }

  test("SPARK-58428: translating an expression that failed to evaluate does not loop forever") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // `coalesce(c, 1 div 0) = 1`. Constant folding defers the divide by zero error because the
      // failing expression sits in a conditional branch, so it is tagged FAILED_TO_EVALUATE and
      // left as is. `div` returns BIGINT, so `c` is LONG to keep the `coalesce` inputs equal.
      val c = AttributeReference("c", LongType)()
      val predicate =
        EqualTo(Coalesce(Seq(c, IntegralDivide(Literal(1), Literal(0)))), Literal(1L))
      val folded = ConstantFolding.constantFolding(predicate)
      assert(
        folded.exists(_.containsTag(ConstantFolding.FAILED_TO_EVALUATE)),
        "expected the divide by zero branch to be tagged FAILED_TO_EVALUATE")

      // Translating such an expression used to recurse forever. Note that a regression hangs
      // this test instead of failing it, as the recursion is in tail position.
      assert(new V2ExpressionBuilder(folded, isPredicate = true).build().isEmpty)
    }
  }

  test("inferred filters use dotted names for nested columns") {
    val tableSchema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("tz", StringType, nullable = true))), nullable = true)))
    val inferredFilter =
      EqualTo(AttributeReference("s.tz", StringType)(), Literal("UTC"))
    val relation = DataSourceV2Relation.create(
      new InMemoryCatalystFilterTable(tableSchema, inferredFilter),
      None,
      None,
      CaseInsensitiveStringMap.empty)
    val id = relation.output.find(_.name == "id").get
    val struct = relation.output.find(_.name == "s").get
    val expected = EqualTo(GetStructField(struct, 0, Some("tz")), Literal("UTC"))

    val pushedPlan = V2ScanRelationPushDown(Filter(EqualTo(id, Literal(1L)), relation))
    assert(pushedPlan.exists {
      case Filter(condition, _) => condition.exists(_.semanticEquals(expected))
      case _ => false
    }, s"expected rebound nested inferred filter in:\n$pushedPlan")
  }

  test("inferred filters support quoted dotted name parts") {
    val tableSchema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("a.b", StructType(Seq(
        StructField("c.d", StringType, nullable = true))), nullable = true)))
    val inferredFilter =
      EqualTo(AttributeReference("`a.b`.`c.d`", StringType)(), Literal("PST"))
    val relation = DataSourceV2Relation.create(
      new InMemoryCatalystFilterTable(tableSchema, inferredFilter),
      None,
      None,
      CaseInsensitiveStringMap.empty)
    val id = relation.output.find(_.name == "id").get
    val struct = relation.output.find(_.name == "a.b").get
    val expected = EqualTo(GetStructField(struct, 0, Some("c.d")), Literal("PST"))

    val pushedPlan = V2ScanRelationPushDown(Filter(EqualTo(id, Literal(1L)), relation))
    assert(pushedPlan.exists {
      case Filter(condition, _) => condition.exists(_.semanticEquals(expected))
      case _ => false
    }, s"expected rebound quoted inferred filter in:\n$pushedPlan")
  }

  test("inferred filters exclude user-defined expressions") {
    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    val inferredUDF = ScalaUDF(
      function = (() => true),
      dataType = BooleanType,
      children = Nil,
      udfName = Some("inferred_udf"))
    val externalInferredUDF = ExternalUserDefinedFunction(
      name = Some("external_inferred_udf"),
      payload = Array.emptyByteArray,
      dataType = BooleanType,
      children = Nil,
      udfDeterministic = true,
      udfNullable = false)

    Seq(inferredUDF, externalInferredUDF).foreach { udf =>
      val relation = DataSourceV2Relation.create(
        new InMemoryCatalystFilterTable(schema, udf),
        None,
        None,
        CaseInsensitiveStringMap.empty)
      val id = relation.output.head

      val pushedPlan = V2ScanRelationPushDown(Filter(EqualTo(id, Literal(1L)), relation))
      val scan = pushedPlan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
      assert(scan.inferredFilters.isEmpty)
      assert(!pushedPlan.exists {
        case Filter(condition, _) => condition.exists(_ eq udf)
        case _ => false
      }, s"user-defined inferred filter $udf must be discarded:\n$pushedPlan")
    }
  }

  test("inferred filters exclude non-deterministic and subquery expressions") {
    val scalarSubquery = ScalarSubquery(
      Project(Seq(Alias(Literal(1L), "value")()), OneRowRelation()))
    val invalidInferredFilters = Seq(
      GreaterThan(Rand(0), Literal(0.5)),
      GreaterThan(scalarSubquery, Literal(0L)))

    invalidInferredFilters.foreach { inferred =>
      val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
      val relation = DataSourceV2Relation.create(
        new InMemoryCatalystFilterTable(schema, inferred),
        None,
        None,
        CaseInsensitiveStringMap.empty)
      val pushedPlan = V2ScanRelationPushDown(
        Filter(EqualTo(relation.output.head, Literal(1L)), relation))
      val scan = pushedPlan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get

      assert(scan.inferredFilters.isEmpty)
      assert(!pushedPlan.exists {
        case Filter(condition, _) => condition.exists(_.semanticEquals(inferred))
        case _ => false
      }, s"invalid inferred filter $inferred must be discarded:\n$pushedPlan")
    }
  }

  test("inferred filters ignore unresolvable and ill-typed expressions") {
    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    val invalidInferredFilters = Seq(
      GreaterThan(AttributeReference("missing", LongType)(), Literal(0L)),
      GreaterThan(AttributeReference("id", StringType)(), Literal("zero")),
      AttributeReference("id", LongType)())

    invalidInferredFilters.foreach { inferred =>
      val relation = DataSourceV2Relation.create(
        new InMemoryCatalystFilterTable(schema, inferred),
        None,
        None,
        CaseInsensitiveStringMap.empty)
      val pushedPlan = V2ScanRelationPushDown(
        Filter(EqualTo(relation.output.head, Literal(1L)), relation))
      val scan = pushedPlan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
      assert(scan.inferredFilters.isEmpty,
        s"invalid inferred filter $inferred must be ignored:\n$pushedPlan")
    }

    val ambiguousSchema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("ID", LongType, nullable = false)))
    val ambiguousInferred =
      GreaterThan(AttributeReference("Id", LongType)(), Literal(0L))
    val ambiguousRelation = DataSourceV2Relation.create(
      new InMemoryCatalystFilterTable(ambiguousSchema, ambiguousInferred),
      None,
      None,
      CaseInsensitiveStringMap.empty)
    val ambiguousPlan = V2ScanRelationPushDown(
      Filter(EqualTo(ambiguousRelation.output.head, Literal(1L)), ambiguousRelation))
    val ambiguousScan =
      ambiguousPlan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
    assert(ambiguousScan.inferredFilters.isEmpty,
      s"ambiguous inferred filter must be ignored:\n$ambiguousPlan")
  }

  test("inferred filters retain referenced columns in the pruned scan schema") {
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("part", LongType, nullable = false),
      StructField("value", LongType, nullable = false)))
    val inferred =
      GreaterThanOrEqual(AttributeReference("part", LongType)(), Literal(0L))
    val table = new PruningCatalystFilterTable(schema, inferred)
    val relation = DataSourceV2Relation.create(
      table,
      None,
      None,
      CaseInsensitiveStringMap.empty)
    val id = relation.output.find(_.name == "id").get
    val value = relation.output.find(_.name == "value").get
    val plan = Project(Seq(value), Filter(EqualTo(id, Literal(1L)), relation))

    val pushedPlan = V2ScanRelationPushDown(plan)
    val scan = pushedPlan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
    assert(table.builder.requiredSchema.fieldNames.contains("part"))
    assert(scan.output.exists(_.name == "part"))
    assert(scan.inferredFilters.exists(_.references.exists(_.name == "part")))
    assert(pushedPlan.exists {
      case Filter(condition, _) => condition.exists(_.semanticEquals(scan.inferredFilters.head))
      case _ => false
    }, s"the inferred filter must remain executable:\n$pushedPlan")
  }

  test("Boolean simplification preserves executable inferred filters") {
    val schema = StructType(Seq(
      StructField("a", BooleanType, nullable = false),
      StructField("b", BooleanType, nullable = false),
      StructField("c", BooleanType, nullable = false)))
    val inferred = Or(
      EqualTo(AttributeReference("a", BooleanType)(), Literal(true)),
      EqualTo(AttributeReference("b", BooleanType)(), Literal(true)))
    val relation = DataSourceV2Relation.create(
      new InMemoryCatalystFilterTable(schema, inferred),
      None,
      None,
      CaseInsensitiveStringMap.empty)
    val a = relation.output.find(_.name == "a").get
    val c = relation.output.find(_.name == "c").get
    val residual = Or(EqualTo(a, Literal(true)), EqualTo(c, Literal(true)))

    val optimized = BooleanSimplification(CombineFilters(
      V2ScanRelationPushDown(Filter(residual, relation))))
    val logicalFilters = optimized.collect { case filter: Filter => filter.condition }
    assert(logicalFilters.exists(_.references.exists(_.name == "b")),
      s"the inferred filter must remain executable after Boolean simplification:\n$optimized")
    val scan = optimized.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
    assert(scan.inferredFilters.exists(_.references.exists(_.name == "b")))
    val physicalPlans = new DataSourceV2Strategy(spark).apply(optimized)
    assert(physicalPlans.exists(_.exists {
      case filter: org.apache.spark.sql.execution.FilterExec =>
        filter.condition.references.exists(_.name == "b")
      case _ => false
    }), s"the simplified inferred filter must remain in FilterExec:\n" +
      physicalPlans.mkString("\n"))
  }

  test("inferred filters are evaluated for V1 scans") {
    checkInferredEvaluatedForScan { tableSchema =>
      new V1Scan {
        override def readSchema(): StructType = tableSchema

        override def toV1TableScan[T <: BaseRelation with TableScan](
            context: SQLContext): T = {
          new BaseRelation with TableScan {
            override def sqlContext: SQLContext = context

            override def schema: StructType = tableSchema

            override def buildScan() = context.sparkContext.emptyRDD[Row]
          }.asInstanceOf[T]
        }
      }
    }
  }

  test("inferred filters are evaluated for local scans") {
    checkInferredEvaluatedForScan { tableSchema =>
      new LocalScan {
        override def readSchema(): StructType = tableSchema

        override def rows(): Array[InternalRow] = Array.empty
      }
    }
  }

  test("inferred filters do not block limit pushdown") {
    val (table, relation) = newPushdownRelation()
    val id = relation.output.find(_.name == "id").get
    val plan = Limit(Literal(5), Filter(EqualTo(id, Literal(1L)), relation))

    val pushedPlan = V2ScanRelationPushDown(plan)
    assert(table.builder.pushedLimit.contains(5))
    assertInferredFilter(pushedPlan)
  }

  test("inferred filters do not block offset pushdown") {
    val (table, relation) = newPushdownRelation()
    val id = relation.output.find(_.name == "id").get
    val plan = Offset(Literal(3), Filter(EqualTo(id, Literal(1L)), relation))

    val pushedPlan = V2ScanRelationPushDown(plan)
    assert(table.builder.pushedOffset.contains(3))
    assertInferredFilter(pushedPlan)
  }

  test("inferred filters do not block top-N pushdown") {
    val (table, relation) = newPushdownRelation()
    val id = relation.output.find(_.name == "id").get
    val filtered = Filter(EqualTo(id, Literal(1L)), relation)
    val plan = Limit(Literal(5), Sort(Seq(id.asc), global = true, filtered))

    val pushedPlan = V2ScanRelationPushDown(plan)
    assert(table.builder.pushedTopN.exists(_._2 == 5))
    assertInferredFilter(pushedPlan)
  }

  test("inferred filters do not block aggregate pushdown") {
    val (table, relation) = newPushdownRelation()
    val id = relation.output.find(_.name == "id").get
    val filtered = Filter(EqualTo(id, Literal(1L)), relation)
    val inferredFilter = GreaterThanOrEqual(id, Literal(0L))
    val count = Alias(Count(id).toAggregateExpression(), "count")()
    val plan = Aggregate(Nil, Seq(count), filtered)

    val pushedPlan = V2ScanRelationPushDown(plan)
    assert(table.builder.pushedAggregation.nonEmpty)
    val scan = pushedPlan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
    assert(scan.inferredFilters.isEmpty,
      s"aggregate output replacement must discard inferred metadata:\n$pushedPlan")
    assert(!pushedPlan.exists {
      case filter: Filter => filter.condition.exists(_.semanticEquals(inferredFilter))
      case _ => false
    }, s"the original inferred filter must not survive aggregate pushdown:\n$pushedPlan")
    val physicalPlans = new DataSourceV2Strategy(spark).apply(pushedPlan)
    assert(physicalPlans.nonEmpty, s"expected DataSourceV2Strategy to plan:\n$pushedPlan")
    assert(!physicalPlans.exists(_.exists {
      case filter: org.apache.spark.sql.execution.FilterExec =>
        filter.condition.exists(_.semanticEquals(inferredFilter))
      case _ => false
    }), s"the original inferred filter must not reach FilterExec:\n${physicalPlans.mkString("\n")}")
  }

  test("inferred filters do not block join pushdown") {
    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val (leftTable, left) = newPushdownRelation()
      val (rightTable, right) = newPushdownRelation()
      val leftId = left.output.find(_.name == "id").get
      val rightId = right.output.find(_.name == "id").get
      val leftFiltered = Filter(EqualTo(leftId, Literal(1L)), left)
      val rightFiltered = Filter(EqualTo(rightId, Literal(1L)), right)
      val plan = Join(
        leftFiltered,
        rightFiltered,
        Inner,
        Some(EqualTo(leftId, rightId)),
        JoinHint.NONE)

      val pushedPlan = V2ScanRelationPushDown(plan)
      assert(leftTable.builder.joinPushed)
      assert(!rightTable.builder.joinPushed)
      val scan = pushedPlan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
      assert(scan.inferredFilters.isEmpty)
      assert(!pushedPlan.exists {
        case filter: Filter => filter.condition.exists(_.isInstanceOf[GreaterThanOrEqual])
        case _ => false
      }, s"inferred filters must not survive join pushdown:\n$pushedPlan")
    }
  }

  test("inferred filters do not block variant pushdown") {
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("v", VariantType, nullable = true)))
    val inferredFilter = GreaterThan(
      VariantGet(
        AttributeReference("v", VariantType)(),
        Literal("$.b"),
        IntegerType,
        failOnError = true,
        timeZoneId = Some("UTC")),
      Literal(0))
    val table = new VariantPushdownTable(schema, inferredFilter)
    val relation = DataSourceV2Relation.create(
      table,
      None,
      None,
      CaseInsensitiveStringMap.empty)
    val id = relation.output.find(_.name == "id").get
    val variant = relation.output.find(_.name == "v").get
    val extracted = VariantGet(
      variant,
      Literal("$.a"),
      IntegerType,
      failOnError = true,
      timeZoneId = Some("UTC"))
    val plan = Project(
      Seq(Alias(extracted, "a")()),
      Filter(EqualTo(id, Literal(1L)), relation))

    val pushedPlan = V2ScanRelationPushDown(plan)
    assert(table.builder.variantPushed)
    val scan = pushedPlan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
    assert(scan.inferredFilters.isEmpty)
    assert(!pushedPlan.exists {
      case filter: Filter => filter.condition.exists(_.semanticEquals(inferredFilter))
      case _ => false
    }, s"inferred filters must not survive variant pushdown:\n$pushedPlan")
  }

  private def newPushdownRelation(
      inferredColumn: String = "id"): (PushdownTable, DataSourceV2Relation) = {
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("value", LongType, nullable = false)))
    val inferredFilter =
      GreaterThanOrEqual(AttributeReference(inferredColumn, LongType)(), Literal(0L))
    val table = new PushdownTable(schema, inferredFilter)
    val relation = DataSourceV2Relation.create(
      table,
      None,
      None,
      CaseInsensitiveStringMap.empty)
    (table, relation)
  }

  private def assertInferredFilter(plan: LogicalPlan): Unit = {
    val scan = plan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
    assert(scan.inferredFilters.nonEmpty)
    assert(plan.exists {
      case filter: Filter =>
        scan.inferredFilters.forall(inferred => filter.condition.exists(_.semanticEquals(inferred)))
      case _ => false
    }, s"expected inferred filter above the scan:\n$plan")
    assertInferredEvaluated(plan)
  }

  private def assertInferredEvaluated(plan: LogicalPlan): Unit = {
    val scan = plan.collectFirst { case scan: DataSourceV2ScanRelation => scan }.get
    val physicalPlans = new DataSourceV2Strategy(spark).apply(plan)
    assert(physicalPlans.nonEmpty, s"expected DataSourceV2Strategy to plan:\n$plan")
    val physicalFilters = physicalPlans.flatMap(_.collect {
      case filter: org.apache.spark.sql.execution.FilterExec => filter.condition
    })
    assert(scan.inferredFilters.forall { inferred =>
      physicalFilters.exists(_.exists(_.semanticEquals(inferred)))
    }, s"inferred filters must remain in FilterExec:\n${physicalPlans.mkString("\n")}")
  }

  private def checkInferredEvaluatedForScan(scanFactory: StructType => Scan): Unit = {
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("value", LongType, nullable = false)))
    val inferredFilter =
      GreaterThanOrEqual(AttributeReference("value", LongType)(), Literal(0L))
    val relation = DataSourceV2Relation.create(
      new InMemoryCatalystFilterTable(schema, inferredFilter, Some(scanFactory)),
      None,
      None,
      CaseInsensitiveStringMap.empty)
    val id = relation.output.find(_.name == "id").get
    val pushedPlan = V2ScanRelationPushDown(Filter(EqualTo(id, Literal(1L)), relation))

    assertInferredFilter(pushedPlan)
  }

  private def emptyBatch: Batch = new Batch {
    override def planInputPartitions() = Array.empty

    override def createReaderFactory(): PartitionReaderFactory = new PartitionReaderFactory {
      override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
        throw new UnsupportedOperationException("reader is not needed")
    }
  }

  private class InMemoryCatalystFilterTable(
      tableSchema: StructType,
      inferredFilter: Expression,
      scanFactory: Option[StructType => Scan] = None) extends Table with SupportsRead {

    override def name(): String = "in-memory-catalyst-filter-table"

    override def schema(): StructType = tableSchema

    override def capabilities(): java.util.Set[TableCapability] =
      EnumSet.of(TableCapability.BATCH_READ)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      new InMemoryCatalystFilterScanBuilder(tableSchema, inferredFilter, scanFactory)
  }

  private class InMemoryCatalystFilterScanBuilder(
      tableSchema: StructType,
      inferredFilter: Expression,
      scanFactory: Option[StructType => Scan])
    extends ScanBuilder with SupportsPushDownCatalystFilters {

    override def build(): Scan = scanFactory.map(_(tableSchema)).getOrElse {
      new Scan {
        override def readSchema(): StructType = tableSchema

        override def toBatch: Batch = emptyBatch
      }
    }

    override def pushFilters(filters: Seq[Expression]): Seq[Expression] = filters

    override def pushedFilters: Array[Predicate] = Array.empty

    override def inferredFilters: Seq[Expression] = Seq(inferredFilter)
  }

  private class PruningCatalystFilterTable(
      tableSchema: StructType,
      inferredFilter: Expression) extends Table with SupportsRead {

    var builder: PruningCatalystFilterScanBuilder = _

    override def name(): String = "pruning-catalyst-filter-table"

    override def schema(): StructType = tableSchema

    override def capabilities(): java.util.Set[TableCapability] =
      EnumSet.of(TableCapability.BATCH_READ)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      builder = new PruningCatalystFilterScanBuilder(tableSchema, inferredFilter)
      builder
    }
  }

  private class PruningCatalystFilterScanBuilder(
      tableSchema: StructType,
      inferredFilter: Expression)
    extends ScanBuilder
    with SupportsPushDownCatalystFilters
    with SupportsPushDownRequiredColumns {

    var requiredSchema: StructType = tableSchema

    override def build(): Scan = new Scan {
      override def readSchema(): StructType = requiredSchema
    }

    override def pushFilters(filters: Seq[Expression]): Seq[Expression] = Nil

    override def pushedFilters: Array[Predicate] = Array.empty

    override def inferredFilters: Seq[Expression] = Seq(inferredFilter)

    override def pruneColumns(schema: StructType): Unit = {
      requiredSchema = schema
    }
  }

  private class PushdownTable(
      tableSchema: StructType,
      inferredFilter: Expression) extends Table with SupportsRead {

    var builder: PushdownScanBuilder = _

    override def name(): String = "pushdown-catalyst-filter-table"

    override def schema(): StructType = tableSchema

    override def capabilities(): java.util.Set[TableCapability] =
      EnumSet.of(TableCapability.BATCH_READ)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      builder = new PushdownScanBuilder(tableSchema, inferredFilter)
      builder
    }
  }

  private class PushdownScanBuilder(
      tableSchema: StructType,
      inferredFilter: Expression)
    extends ScanBuilder
    with SupportsPushDownCatalystFilters
    with SupportsPushDownAggregates
    with SupportsPushDownJoin
    with SupportsPushDownLimit
    with SupportsPushDownOffset
    with SupportsPushDownTopN {

    var pushedAggregation: Option[Aggregation] = None
    var pushedLimit: Option[Int] = None
    var pushedOffset: Option[Int] = None
    var pushedTopN: Option[(Array[V2SortOrder], Int)] = None
    var joinPushed: Boolean = false
    private var scanSchema: StructType = tableSchema

    override def build(): Scan = new Scan {
      override def readSchema(): StructType = scanSchema

      override def toBatch: Batch = emptyBatch
    }

    override def pushFilters(filters: Seq[Expression]): Seq[Expression] = Nil

    override def pushedFilters: Array[Predicate] = Array.empty

    override def inferredFilters: Seq[Expression] = Seq(inferredFilter)

    override def supportCompletePushDown(aggregation: Aggregation): Boolean = true

    override def pushAggregation(aggregation: Aggregation): Boolean = {
      pushedAggregation = Some(aggregation)
      val fields =
        aggregation.groupByExpressions().indices.map(i => StructField(s"group_$i", LongType)) ++
          aggregation.aggregateExpressions().indices.map(i => StructField(s"agg_$i", LongType))
      scanSchema = StructType(fields)
      true
    }

    override def isOtherSideCompatibleForJoin(other: SupportsPushDownJoin): Boolean =
      other.isInstanceOf[PushdownScanBuilder]

    override def pushDownJoin(
        other: SupportsPushDownJoin,
        joinType: V2JoinType,
        leftColumns: Array[SupportsPushDownJoin.ColumnWithAlias],
        rightColumns: Array[SupportsPushDownJoin.ColumnWithAlias],
        condition: Predicate): Boolean = {
      joinPushed = true
      scanSchema = StructType((leftColumns ++ rightColumns).map { column =>
        StructField(Option(column.alias()).getOrElse(column.colName()), LongType)
      })
      true
    }

    override def pushLimit(limit: Int): Boolean = {
      pushedLimit = Some(limit)
      true
    }

    override def pushOffset(offset: Int): Boolean = {
      pushedOffset = Some(offset)
      true
    }

    override def pushTopN(orders: Array[V2SortOrder], limit: Int): Boolean = {
      pushedTopN = Some((orders, limit))
      true
    }

    override def isPartiallyPushed(): Boolean = false
  }

  private class VariantPushdownTable(
      tableSchema: StructType,
      inferredFilter: Expression) extends Table with SupportsRead {

    var builder: VariantPushdownScanBuilder = _

    override def name(): String = "variant-pushdown-catalyst-filter-table"

    override def schema(): StructType = tableSchema

    override def capabilities(): java.util.Set[TableCapability] =
      EnumSet.of(TableCapability.BATCH_READ)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      builder = new VariantPushdownScanBuilder(tableSchema, inferredFilter)
      builder
    }
  }

  private class VariantPushdownScanBuilder(
      tableSchema: StructType,
      inferredFilter: Expression)
    extends ScanBuilder
    with SupportsPushDownCatalystFilters
    with SupportsPushDownVariantExtractions {

    var variantPushed: Boolean = false
    private var scanSchema: StructType = tableSchema

    override def build(): Scan = new Scan {
      override def readSchema(): StructType = scanSchema

      override def toBatch: Batch = emptyBatch
    }

    override def pushFilters(filters: Seq[Expression]): Seq[Expression] = Nil

    override def pushedFilters: Array[Predicate] = Array.empty

    override def inferredFilters: Seq[Expression] = Seq(inferredFilter)

    override def pushVariantExtractions(extractions: Array[VariantExtraction]): Array[Boolean] = {
      variantPushed = true
      val extractionsByColumn = extractions.groupBy(_.columnName().head)
      scanSchema = StructType(tableSchema.fields.map { field =>
        extractionsByColumn.get(field.name) match {
          case Some(columnExtractions) =>
            val extractedFields = columnExtractions.zipWithIndex.map { case (extraction, index) =>
              StructField(
                index.toString,
                extraction.expectedDataType(),
                nullable = true,
                extraction.metadata())
            }
            field.copy(dataType = StructType(extractedFields))
          case None =>
            field
        }
      })
      Array.fill(extractions.length)(true)
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
