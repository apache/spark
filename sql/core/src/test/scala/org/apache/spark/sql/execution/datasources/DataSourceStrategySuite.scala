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

class DataSourceStrategySuite extends PlanTest with SharedSQLContext {

  test("translate simple expression") {
    val attrInt = 'cint.int
    val attrStr = 'cstr.string

    testTranslateFilter(EqualTo(attrInt, 1), Some(sources.EqualTo("cint", 1)))
    testTranslateFilter(EqualTo(1, attrInt), Some(sources.EqualTo("cint", 1)))

    testTranslateFilter(EqualNullSafe(attrStr, Literal(null)),
      Some(sources.EqualNullSafe("cstr", null)))
    testTranslateFilter(EqualNullSafe(Literal(null), attrStr),
      Some(sources.EqualNullSafe("cstr", null)))

    testTranslateFilter(GreaterThan(attrInt, 1), Some(sources.GreaterThan("cint", 1)))
    testTranslateFilter(GreaterThan(1, attrInt), Some(sources.LessThan("cint", 1)))

    testTranslateFilter(LessThan(attrInt, 1), Some(sources.LessThan("cint", 1)))
    testTranslateFilter(LessThan(1, attrInt), Some(sources.GreaterThan("cint", 1)))

    testTranslateFilter(GreaterThanOrEqual(attrInt, 1), Some(sources.GreaterThanOrEqual("cint", 1)))
    testTranslateFilter(GreaterThanOrEqual(1, attrInt), Some(sources.LessThanOrEqual("cint", 1)))

    testTranslateFilter(LessThanOrEqual(attrInt, 1), Some(sources.LessThanOrEqual("cint", 1)))
    testTranslateFilter(LessThanOrEqual(1, attrInt), Some(sources.GreaterThanOrEqual("cint", 1)))

    testTranslateFilter(InSet(attrInt, Set(1, 2, 3)), Some(sources.In("cint", Array(1, 2, 3))))

    testTranslateFilter(In(attrInt, Seq(1, 2, 3)), Some(sources.In("cint", Array(1, 2, 3))))

    testTranslateFilter(IsNull(attrInt), Some(sources.IsNull("cint")))
    testTranslateFilter(IsNotNull(attrInt), Some(sources.IsNotNull("cint")))

    // cint > 1 AND cint < 10
    testTranslateFilter(And(
      GreaterThan(attrInt, 1),
      LessThan(attrInt, 10)),
      Some(sources.And(
        sources.GreaterThan("cint", 1),
        sources.LessThan("cint", 10))))

    // cint >= 8 OR cint <= 2
    testTranslateFilter(Or(
      GreaterThanOrEqual(attrInt, 8),
      LessThanOrEqual(attrInt, 2)),
      Some(sources.Or(
        sources.GreaterThanOrEqual("cint", 8),
        sources.LessThanOrEqual("cint", 2))))

    testTranslateFilter(Not(GreaterThanOrEqual(attrInt, 8)),
      Some(sources.Not(sources.GreaterThanOrEqual("cint", 8))))

    testTranslateFilter(StartsWith(attrStr, "a"), Some(sources.StringStartsWith("cstr", "a")))

    testTranslateFilter(EndsWith(attrStr, "a"), Some(sources.StringEndsWith("cstr", "a")))

    testTranslateFilter(Contains(attrStr, "a"), Some(sources.StringContains("cstr", "a")))
  }

  test("translate complex expression") {
    val attrInt = 'cint.int

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
          sources.GreaterThan("cint", 1),
          sources.LessThan("cint", 10)),
        sources.And(
          sources.GreaterThan("cint", 50),
          sources.LessThan("cint", 100)))))

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
          sources.EqualTo("cint", 1),
          sources.EqualTo("cint", 10)),
        sources.Or(
          sources.GreaterThan("cint", 0),
          sources.LessThan("cint", -10)))))

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
          sources.GreaterThan("cint", 1),
          sources.LessThan("cint", 10)),
        sources.And(
          sources.EqualTo("cint", 6),
          sources.IsNotNull("cint")))))

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
          sources.GreaterThan("cint", 1),
          sources.LessThan("cint", 10)),
        sources.Or(
          sources.EqualTo("cint", 6),
          sources.IsNotNull("cint")))))

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
  }

  test("SPARK-26865 DataSourceV2Strategy should push normalized filters") {
    val attrInt = 'cint.int
    assertResult(Seq(IsNotNull(attrInt))) {
      DataSourceStrategy.normalizeFilters(Seq(IsNotNull(attrInt.withName("CiNt"))), Seq(attrInt))
    }
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
