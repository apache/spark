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
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DataSourceStrategySuite extends PlanTest with SharedSparkSession {
  val attrInts = Seq(
    Symbol("cint").int,
    Symbol("c.int").int,
    GetStructField(Symbol("a").struct(StructType(
      StructField("cstr", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 1, None),
    GetStructField(Symbol("a").struct(StructType(
      StructField("c.int", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 0, None),
    GetStructField(Symbol("a.b").struct(StructType(
      StructField("cstr1", StringType, nullable = true) ::
        StructField("cstr2", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 2, None),
    GetStructField(Symbol("a.b").struct(StructType(
      StructField("c.int", IntegerType, nullable = true) :: Nil)), 0, None),
    GetStructField(GetStructField(Symbol("a").struct(StructType(
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
    Symbol("cstr").string,
    Symbol("c.str").string,
    GetStructField(Symbol("a").struct(StructType(
      StructField("cint", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 1, None),
    GetStructField(Symbol("a").struct(StructType(
      StructField("c.str", StringType, nullable = true) ::
        StructField("cint", IntegerType, nullable = true) :: Nil)), 0, None),
    GetStructField(Symbol("a.b").struct(StructType(
      StructField("cint1", IntegerType, nullable = true) ::
        StructField("cint2", IntegerType, nullable = true) ::
        StructField("cstr", StringType, nullable = true) :: Nil)), 2, None),
    GetStructField(Symbol("a.b").struct(StructType(
      StructField("c.str", StringType, nullable = true) :: Nil)), 0, None),
    GetStructField(GetStructField(Symbol("a").struct(StructType(
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

    testTranslateFilter(EqualTo(attrInt, 1), Some(sources.EqualTo(intColName, 1)))
    testTranslateFilter(EqualTo(1, attrInt), Some(sources.EqualTo(intColName, 1)))

    testTranslateFilter(EqualNullSafe(attrStr, Literal(null)),
      Some(sources.EqualNullSafe(strColName, null)))
    testTranslateFilter(EqualNullSafe(Literal(null), attrStr),
      Some(sources.EqualNullSafe(strColName, null)))

    testTranslateFilter(GreaterThan(attrInt, 1), Some(sources.GreaterThan(intColName, 1)))
    testTranslateFilter(GreaterThan(1, attrInt), Some(sources.LessThan(intColName, 1)))

    testTranslateFilter(LessThan(attrInt, 1), Some(sources.LessThan(intColName, 1)))
    testTranslateFilter(LessThan(1, attrInt), Some(sources.GreaterThan(intColName, 1)))

    testTranslateFilter(GreaterThanOrEqual(attrInt, 1),
      Some(sources.GreaterThanOrEqual(intColName, 1)))
    testTranslateFilter(GreaterThanOrEqual(1, attrInt),
      Some(sources.LessThanOrEqual(intColName, 1)))

    testTranslateFilter(LessThanOrEqual(attrInt, 1),
      Some(sources.LessThanOrEqual(intColName, 1)))
    testTranslateFilter(LessThanOrEqual(1, attrInt),
      Some(sources.GreaterThanOrEqual(intColName, 1)))

    testTranslateFilter(InSet(attrInt, Set(1, 2, 3)), Some(sources.In(intColName, Array(1, 2, 3))))

    testTranslateFilter(In(attrInt, Seq(1, 2, 3)), Some(sources.In(intColName, Array(1, 2, 3))))

    testTranslateFilter(IsNull(attrInt), Some(sources.IsNull(intColName)))
    testTranslateFilter(IsNotNull(attrInt), Some(sources.IsNotNull(intColName)))

    // cint > 1 AND cint < 10
    testTranslateFilter(And(
      GreaterThan(attrInt, 1),
      LessThan(attrInt, 10)),
      Some(sources.And(
        sources.GreaterThan(intColName, 1),
        sources.LessThan(intColName, 10))))

    // cint >= 8 OR cint <= 2
    testTranslateFilter(Or(
      GreaterThanOrEqual(attrInt, 8),
      LessThanOrEqual(attrInt, 2)),
      Some(sources.Or(
        sources.GreaterThanOrEqual(intColName, 8),
        sources.LessThanOrEqual(intColName, 2))))

    testTranslateFilter(Not(GreaterThanOrEqual(attrInt, 8)),
      Some(sources.Not(sources.GreaterThanOrEqual(intColName, 8))))

    testTranslateFilter(StartsWith(attrStr, "a"), Some(sources.StringStartsWith(strColName, "a")))

    testTranslateFilter(EndsWith(attrStr, "a"), Some(sources.StringEndsWith(strColName, "a")))

    testTranslateFilter(Contains(attrStr, "a"), Some(sources.StringContains(strColName, "a")))
  }}

  test("translate complex expression") { attrInts.foreach { case (attrInt, intColName) =>

    // ABS(cint) - 2 <= 1
    testTranslateFilter(LessThanOrEqual(
      // Expressions are not supported
      // Functions such as 'Abs' are not supported
      Subtract(Abs(attrInt), 2), 1), None)

    // (cin1 > 1 AND cint < 10) OR (cint > 50 AND cint > 100)
    testTranslateFilter(Or(
      And(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)
      ),
      And(
        GreaterThan(attrInt, 50),
        LessThan(attrInt, 100))),
      Some(sources.Or(
        sources.And(
          sources.GreaterThan(intColName, 1),
          sources.LessThan(intColName, 10)),
        sources.And(
          sources.GreaterThan(intColName, 50),
          sources.LessThan(intColName, 100)))))

    // SPARK-22548 Incorrect nested AND expression pushed down to JDBC data source
    // (cint > 1 AND ABS(cint) < 10) OR (cint < 50 AND cint > 100)
    testTranslateFilter(Or(
      And(
        GreaterThan(attrInt, 1),
        // Functions such as 'Abs' are not supported
        LessThan(Abs(attrInt), 10)
      ),
      And(
        GreaterThan(attrInt, 50),
        LessThan(attrInt, 100))), None)

    // NOT ((cint <= 1 OR ABS(cint) >= 10) AND (cint <= 50 OR cint >= 100))
    testTranslateFilter(Not(And(
      Or(
        LessThanOrEqual(attrInt, 1),
        // Functions such as 'Abs' are not supported
        GreaterThanOrEqual(Abs(attrInt), 10)
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
      Some(sources.Or(
        sources.Or(
          sources.EqualTo(intColName, 1),
          sources.EqualTo(intColName, 10)),
        sources.Or(
          sources.GreaterThan(intColName, 0),
          sources.LessThan(intColName, -10)))))

    // (cint = 1 OR ABS(cint) = 10) OR (cint > 0 OR cint < -10)
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
    // (cint > 1 AND cint < 10) AND (cint = 6 AND cint IS NOT NULL)
    testTranslateFilter(And(
      And(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)
      ),
      And(
        EqualTo(attrInt, 6),
        IsNotNull(attrInt))),
      Some(sources.And(
        sources.And(
          sources.GreaterThan(intColName, 1),
          sources.LessThan(intColName, 10)),
        sources.And(
          sources.EqualTo(intColName, 6),
          sources.IsNotNull(intColName)))))

    // (cint > 1 AND cint < 10) AND (ABS(cint) = 6 AND cint IS NOT NULL)
    testTranslateFilter(And(
      And(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)
      ),
      And(
        // Functions such as 'Abs' are not supported
        EqualTo(Abs(attrInt), 6),
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
      Some(sources.And(
        sources.Or(
          sources.GreaterThan(intColName, 1),
          sources.LessThan(intColName, 10)),
        sources.Or(
          sources.EqualTo(intColName, 6),
          sources.IsNotNull(intColName)))))

    // (cint > 1 OR cint < 10) AND (cint = 6 OR cint IS NOT NULL)
    testTranslateFilter(And(
      Or(
        GreaterThan(attrInt, 1),
        LessThan(attrInt, 10)
      ),
      Or(
        // Functions such as 'Abs' are not supported
        EqualTo(Abs(attrInt), 6),
        IsNotNull(attrInt))), None)
  }}

  test("SPARK-26865 DataSourceV2Strategy should push normalized filters") {
    val attrInt = Symbol("cint").int
    assertResult(Seq(IsNotNull(attrInt))) {
      DataSourceStrategy.normalizeExprs(Seq(IsNotNull(attrInt.withName("CiNt"))), Seq(attrInt))
    }
  }

  test("SPARK-31027 test `PushableColumn.unapply` that finds the column name of " +
    "an expression that can be pushed down") {
    attrInts.foreach { case (attrInt, colName) =>
      assert(PushableColumnAndNestedColumn.unapply(attrInt) === Some(colName))

      if (colName.contains(".")) {
        assert(PushableColumnWithoutNestedColumn.unapply(attrInt) === None)
      } else {
        assert(PushableColumnWithoutNestedColumn.unapply(attrInt) === Some(colName))
      }
    }
    attrStrs.foreach { case (attrStr, colName) =>
      assert(PushableColumnAndNestedColumn.unapply(attrStr) === Some(colName))

      if (colName.contains(".")) {
        assert(PushableColumnWithoutNestedColumn.unapply(attrStr) === None)
      } else {
        assert(PushableColumnWithoutNestedColumn.unapply(attrStr) === Some(colName))
      }
    }

    // `Abs(col)` can not be pushed down, so it returns `None`
    assert(PushableColumnAndNestedColumn.unapply(Abs(Symbol("col").int)) === None)
  }

  test("SPARK-36644: Push down boolean column filter") {
    testTranslateFilter(Symbol("col").boolean, Some(sources.EqualTo("col", true)))
  }

  /**
   * Translate the given Catalyst [[Expression]] into data source [[sources.Filter]]
   * then verify against the given [[sources.Filter]].
   */
  def testTranslateFilter(catalystFilter: Expression, result: Option[sources.Filter]): Unit = {
    assertResult(result) {
      DataSourceStrategy.translateFilter(catalystFilter, true)
    }
  }
}
