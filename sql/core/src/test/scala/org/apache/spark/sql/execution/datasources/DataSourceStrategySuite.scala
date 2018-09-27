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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.sources
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DataSourceStrategySuite extends PlanTest with SharedSQLContext {

  test("translate simple expression") {
    val fields = StructField("cint", IntegerType, nullable = true) ::
      StructField("cstr", StringType, nullable = true) :: Nil

    val attrNested1 = 'a.struct(StructType(fields))
    val attrNested2 = 'b.struct(StructType(
      StructField("c", StructType(fields), nullable = true) :: Nil))

    val attrIntNested1 = GetStructField(attrNested1, 0, None)
    val attrStrNested1 = GetStructField(attrNested1, 1, None)

    val attrIntNested2 = GetStructField(GetStructField(attrNested2, 0, None), 0, None)
    val attrStrNested2 = GetStructField(GetStructField(attrNested2, 0, None), 1, None)

    Seq(('cint.int, 'cstr.string, "cint", "cstr"), // no nesting
      (attrIntNested1, attrStrNested1, "a.cint", "a.cstr"), // one level nesting
      (attrIntNested2, attrStrNested2, "b.c.cint", "b.c.cstr") // two level nesting
    ).foreach { case (attrInt, attrStr, attrIntString, attrStrString) =>
      testTranslateSimpleExpression(
        attrInt, attrStr, attrIntString, attrStrString, isResultNone = false)
    }
  }

  test("translate complex expression") {
    val fields = StructField("cint", IntegerType, nullable = true) :: Nil

    val attrNested1 = 'a.struct(StructType(fields))
    val attrNested2 = 'b.struct(StructType(
      StructField("c", StructType(fields), nullable = true) :: Nil))

    val attrIntNested1 = GetStructField(attrNested1, 0, None)
    val attrIntNested2 = GetStructField(GetStructField(attrNested2, 0, None), 0, None)

    StructField("cint", IntegerType, nullable = true)

    Seq(('cint.int, "cint"), // no nesting
      (attrIntNested1, "a.cint"), // one level nesting
      (attrIntNested2, "b.c.cint") // two level nesting
    ).foreach { case (attrInt, attrIntString) =>
      testTranslateComplexExpression(attrInt, attrIntString, isResultNone = false)
    }
  }

  test("column name containing dot can not be pushed down") {
    val fieldsWithoutDot = StructField("cint", IntegerType, nullable = true) ::
      StructField("cstr", StringType, nullable = true) :: Nil

    val fieldsWithDot = StructField("column.cint", IntegerType, nullable = true) ::
      StructField("column.cstr", StringType, nullable = true) :: Nil

    val attrNested1 = 'a.struct(StructType(fieldsWithDot))
    val attrIntNested1 = GetStructField(attrNested1, 0, None)
    val attrStrNested1 = GetStructField(attrNested1, 1, None)

    val attrNested2 = 'b.struct(StructType(
      StructField("c", StructType(fieldsWithDot), nullable = true) :: Nil))
    val attrIntNested2 = GetStructField(GetStructField(attrNested2, 0, None), 0, None)
    val attrStrNested2 = GetStructField(GetStructField(attrNested2, 0, None), 1, None)

    val attrNestedWithDotInTopLevel = Symbol("column.a").struct(StructType(fieldsWithoutDot))
    val attrIntNested1WithDotInTopLevel = GetStructField(attrNestedWithDotInTopLevel, 0, None)
    val attrStrNested1WithDotInTopLevel = GetStructField(attrNestedWithDotInTopLevel, 1, None)

    Seq((Symbol("column.cint").int, Symbol("column.cstr").string), // no nesting
      (attrIntNested1, attrStrNested1), // one level nesting
      (attrIntNested1WithDotInTopLevel, attrStrNested1WithDotInTopLevel), // one level nesting
      (attrIntNested2, attrStrNested2) // two level nesting
    ).foreach { case (attrInt, attrStr) =>
      testTranslateSimpleExpression(
        attrInt, attrStr, "", "", isResultNone = true)
      testTranslateComplexExpression(attrInt, "", isResultNone = true)
    }
  }

  // `isResultNone` is used when testing invalid input expression
  // containing dots which translates into None
  private def testTranslateSimpleExpression(
      attrInt: Expression, attrStr: Expression,
      attrIntString: String, attrStrString: String, isResultNone: Boolean): Unit = {

    def result(result: sources.Filter): Option[sources.Filter] = {
      if (isResultNone) {
        None
      } else {
        Some(result)
      }
    }

    testTranslateFilter(EqualTo(attrInt, 1), result(sources.EqualTo(attrIntString, 1)))
    testTranslateFilter(EqualTo(1, attrInt), result(sources.EqualTo(attrIntString, 1)))

    testTranslateFilter(EqualNullSafe(attrStr, Literal(null)),
      result(sources.EqualNullSafe(attrStrString, null)))
    testTranslateFilter(EqualNullSafe(Literal(null), attrStr),
      result(sources.EqualNullSafe(attrStrString, null)))

    testTranslateFilter(GreaterThan(attrInt, 1), result(sources.GreaterThan(attrIntString, 1)))
    testTranslateFilter(GreaterThan(1, attrInt), result(sources.LessThan(attrIntString, 1)))

    testTranslateFilter(LessThan(attrInt, 1), result(sources.LessThan(attrIntString, 1)))
    testTranslateFilter(LessThan(1, attrInt), result(sources.GreaterThan(attrIntString, 1)))

    testTranslateFilter(GreaterThanOrEqual(attrInt, 1),
      result(sources.GreaterThanOrEqual(attrIntString, 1)))
    testTranslateFilter(GreaterThanOrEqual(1, attrInt),
      result(sources.LessThanOrEqual(attrIntString, 1)))

    testTranslateFilter(LessThanOrEqual(attrInt, 1),
      result(sources.LessThanOrEqual(attrIntString, 1)))
    testTranslateFilter(LessThanOrEqual(1, attrInt),
      result(sources.GreaterThanOrEqual(attrIntString, 1)))

    testTranslateFilter(InSet(attrInt, Set(1, 2, 3)),
      result(sources.In(attrIntString, Array(1, 2, 3))))

    testTranslateFilter(In(attrInt, Seq(1, 2, 3)),
      result(sources.In(attrIntString, Array(1, 2, 3))))

    testTranslateFilter(IsNull(attrInt), result(sources.IsNull(attrIntString)))
    testTranslateFilter(IsNotNull(attrInt), result(sources.IsNotNull(attrIntString)))

    // attrInt > 1 AND attrInt < 10
    testTranslateFilter(And(
      GreaterThan(attrInt, 1),
      LessThan(attrInt, 10)),
      result(sources.And(
        sources.GreaterThan(attrIntString, 1),
        sources.LessThan(attrIntString, 10))))

    // attrInt >= 8 OR attrInt <= 2
    testTranslateFilter(Or(
      GreaterThanOrEqual(attrInt, 8),
      LessThanOrEqual(attrInt, 2)),
      result(sources.Or(
        sources.GreaterThanOrEqual(attrIntString, 8),
        sources.LessThanOrEqual(attrIntString, 2))))

    testTranslateFilter(Not(GreaterThanOrEqual(attrInt, 8)),
      result(sources.Not(sources.GreaterThanOrEqual(attrIntString, 8))))

    testTranslateFilter(StartsWith(attrStr, "a"),
      result(sources.StringStartsWith(attrStrString, "a")))

    testTranslateFilter(EndsWith(attrStr, "a"), result(sources.StringEndsWith(attrStrString, "a")))

    testTranslateFilter(Contains(attrStr, "a"), result(sources.StringContains(attrStrString, "a")))
  }

  // `isResultNone` is used when testing invalid input expression
  // containing dots which translates into None
  private def testTranslateComplexExpression(
      attrInt: Expression, attrIntString: String, isResultNone: Boolean): Unit = {

    def result(result: sources.Filter): Option[sources.Filter] = {
      if (isResultNone) {
        None
      } else {
        Some(result)
      }
    }

    // ABS(attrInt) - 2 <= 1
    testTranslateFilter(LessThanOrEqual(
      // Expressions are not supported
      // Functions such as 'Abs' are not supported
      Subtract(Abs(attrInt), 2), 1), None)

    // (attrInt > 1 AND attrInt < 10) OR (attrInt > 50 AND attrInt > 100)
    testTranslateFilter(Or(
      And(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)
      ),
      And(
        GreaterThan(attrInt, 50),
        LessThan(attrInt, 100))),
      result(sources.Or(
        sources.And(
          sources.GreaterThan(attrIntString, 1),
          sources.LessThan(attrIntString, 10)),
        sources.And(
          sources.GreaterThan(attrIntString, 50),
          sources.LessThan(attrIntString, 100)))))

    // SPARK-22548 Incorrect nested AND expression pushed down to JDBC data source
    // (attrInt > 1 AND ABS(attrInt) < 10) OR (attrInt < 50 AND attrInt > 100)
    testTranslateFilter(Or(
      And(
        GreaterThan(attrInt, 1),
        // Functions such as 'Abs' are not supported
        LessThan(Abs(attrInt), 10)
      ),
      And(
        GreaterThan(attrInt, 50),
        LessThan(attrInt, 100))), None)

    // NOT ((attrInt <= 1 OR ABS(attrInt) >= 10) AND (attrInt <= 50 OR attrInt >= 100))
    testTranslateFilter(Not(And(
      Or(
        LessThanOrEqual(attrInt, 1),
        // Functions such as 'Abs' are not supported
        GreaterThanOrEqual(Abs(attrInt), 10)
      ),
      Or(
        LessThanOrEqual(attrInt, 50),
        GreaterThanOrEqual(attrInt, 100)))), None)

    // (attrInt = 1 OR attrInt = 10) OR (attrInt > 0 OR attrInt < -10)
    testTranslateFilter(Or(
      Or(
        EqualTo(attrInt, 1),
        EqualTo(attrInt, 10)
      ),
      Or(
        GreaterThan(attrInt, 0),
        LessThan(attrInt, -10))),
      result(sources.Or(
        sources.Or(
          sources.EqualTo(attrIntString, 1),
          sources.EqualTo(attrIntString, 10)),
        sources.Or(
          sources.GreaterThan(attrIntString, 0),
          sources.LessThan(attrIntString, -10)))))

    // (attrInt = 1 OR ABS(attrInt) = 10) OR (attrInt > 0 OR attrInt < -10)
    testTranslateFilter(Or(
      Or(
        EqualTo(attrInt, 1),
        // Functions such as 'Abs' are not supported
        EqualTo(Abs(attrInt), 10)
      ),
      Or(
        GreaterThan(attrInt, 0),
        LessThan(attrInt, -10))), None)

    // In end-to-end testing, conjunctive predicate should has been split
    // before reaching DataSourceStrategy.translateFilter.
    // This is for UT purpose to test each [[case]].
    // (attrInt > 1 AND attrInt < 10) AND (attrInt = 6 AND attrInt IS NOT NULL)
    testTranslateFilter(And(
      And(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)
      ),
      And(
        EqualTo(attrInt, 6),
        IsNotNull(attrInt))),
      result(sources.And(
        sources.And(
          sources.GreaterThan(attrIntString, 1),
          sources.LessThan(attrIntString, 10)),
        sources.And(
          sources.EqualTo(attrIntString, 6),
          sources.IsNotNull(attrIntString)))))

    // (attrInt > 1 AND attrInt < 10) AND (ABS(attrInt) = 6 AND attrInt IS NOT NULL)
    testTranslateFilter(And(
      And(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)
      ),
      And(
        // Functions such as 'Abs' are not supported
        EqualTo(Abs(attrInt), 6),
        IsNotNull(attrInt))), None)

    // (attrInt > 1 OR attrInt < 10) AND (attrInt = 6 OR attrInt IS NOT NULL)
    testTranslateFilter(And(
      Or(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)
      ),
      Or(
        EqualTo(attrInt, 6),
        IsNotNull(attrInt))),
      result(sources.And(
        sources.Or(
          sources.GreaterThan(attrIntString, 1),
          sources.LessThan(attrIntString, 10)),
        sources.Or(
          sources.EqualTo(attrIntString, 6),
          sources.IsNotNull(attrIntString)))))

    // (attrInt > 1 OR attrInt < 10) AND (attrInt = 6 OR attrInt IS NOT NULL)
    testTranslateFilter(And(
      Or(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)
      ),
      Or(
        // Functions such as 'Abs' are not supported
        EqualTo(Abs(attrInt), 6),
        IsNotNull(attrInt))), None)
  }

  /**
   * Translate the given Catalyst [[Expression]] into data source [[sources.Filter]]
   * then verify against the given [[sources.Filter]].
   */
  def testTranslateFilter(catalystFilter: Expression, result: Option[sources.Filter]): Unit = {
    assertResult(result) {
      DataSourceStrategy.translateFilter(catalystFilter)
    }
  }
}
