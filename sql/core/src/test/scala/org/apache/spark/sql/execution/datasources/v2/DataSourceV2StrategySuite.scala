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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class DataSourceV2StrategySuite extends PlanTest with SharedSparkSession {
  val attrInts = Seq(
    $"cint".int,
    $"c.int".int,
    GetStructField($"a".struct(StructType(
      StructField("cstr", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 1, None),
    GetStructField($"a".struct(StructType(
      StructField("c.int", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 0, None),
    GetStructField($"a.b".struct(StructType(
      StructField("cstr1", StringType, nullable = true) ::
        StructField("cstr2", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 2, None),
    GetStructField($"a.b".struct(StructType(
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
    $"c.str".string,
    GetStructField($"a".struct(StructType(
      StructField("cint", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 1, None),
    GetStructField($"a".struct(StructType(
      StructField("c.str", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 0, None),
    GetStructField($"a.b".struct(StructType(
      StructField("cint1", IntegerType, nullable = true) ::
        StructField("cint2", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 2, None),
    GetStructField($"a.b".struct(StructType(
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

  /**
   * Translate the given Catalyst [[Expression]] into data source V2 [[Predicate]]
   * then verify against the given [[Predicate]].
   */
  def testTranslateFilter(catalystFilter: Expression, result: Option[Predicate]): Unit = {
    assertResult(result) {
      DataSourceV2Strategy.translateFilterV2(catalystFilter)
    }
  }
}
