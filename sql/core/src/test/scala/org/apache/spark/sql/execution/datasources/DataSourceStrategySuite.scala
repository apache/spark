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
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.sources
import org.apache.spark.sql.test.SharedSQLContext


class DataSourceStrategySuite extends PlanTest with SharedSQLContext {

  test("translate simple expression") {
    val attrInt = 'cint.int
    val attrStr = 'cstr.string

    assertResult(Some(sources.EqualTo("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.EqualTo(attrInt, 1))
    }
    assertResult(Some(sources.EqualTo("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.EqualTo(1, attrInt))
    }

    assertResult(Some(sources.EqualNullSafe("cstr", null))) {
      DataSourceStrategy.translateFilter(
        expressions.EqualNullSafe(attrStr, Literal(null)))
    }
    assertResult(Some(sources.EqualNullSafe("cstr", null))) {
      DataSourceStrategy.translateFilter(
        expressions.EqualNullSafe(Literal(null), attrStr))
    }

    assertResult(Some(sources.GreaterThan("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.GreaterThan(attrInt, 1))
    }
    assertResult(Some(sources.GreaterThan("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.LessThan(1, attrInt))
    }

    assertResult(Some(sources.LessThan("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.LessThan(attrInt, 1))
    }
    assertResult(Some(sources.LessThan("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.GreaterThan(1, attrInt))
    }

    assertResult(Some(sources.GreaterThanOrEqual("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.GreaterThanOrEqual(attrInt, 1))
    }
    assertResult(Some(sources.GreaterThanOrEqual("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.LessThanOrEqual(1, attrInt))
    }

    assertResult(Some(sources.LessThanOrEqual("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.LessThanOrEqual(attrInt, 1))
    }
    assertResult(Some(sources.LessThanOrEqual("cint", 1))) {
      DataSourceStrategy.translateFilter(
        expressions.GreaterThanOrEqual(1, attrInt))
    }

    assertResult(Some(sources.In("cint", Array(1, 2, 3)))) {
      DataSourceStrategy.translateFilter(
        expressions.InSet(attrInt, Set(1, 2, 3)))
    }

    assertResult(Some(sources.In("cint", Array(1, 2, 3)))) {
      DataSourceStrategy.translateFilter(
        expressions.In(attrInt, Seq(1, 2, 3)))
    }

    assertResult(Some(sources.IsNull("cint"))) {
      DataSourceStrategy.translateFilter(
        expressions.IsNull(attrInt))
    }
    assertResult(Some(sources.IsNotNull("cint"))) {
      DataSourceStrategy.translateFilter(
        expressions.IsNotNull(attrInt))
    }

    assertResult(Some(sources.And(
      sources.GreaterThan("cint", 1),
      sources.LessThan("cint", 10)))) {
      DataSourceStrategy.translateFilter(expressions.And(
        expressions.GreaterThan(attrInt, 1),
        expressions.LessThan(attrInt, 10)
      ))
    }

    assertResult(Some(sources.Or(
      sources.GreaterThanOrEqual("cint", 8),
      sources.LessThanOrEqual("cint", 2)))) {
      DataSourceStrategy.translateFilter(expressions.Or(
        expressions.GreaterThanOrEqual(attrInt, 8),
        expressions.LessThanOrEqual(attrInt, 2)
      ))
    }

    assertResult(Some(sources.Not(
      sources.GreaterThanOrEqual("cint", 8)))) {
      DataSourceStrategy.translateFilter(
        expressions.Not(expressions.GreaterThanOrEqual(attrInt, 8)
        ))
    }

    assertResult(Some(sources.StringStartsWith("cstr", "a"))) {
      DataSourceStrategy.translateFilter(
        expressions.StartsWith(attrStr, "a"
        ))
    }

    assertResult(Some(sources.StringEndsWith("cstr", "a"))) {
      DataSourceStrategy.translateFilter(
        expressions.EndsWith(attrStr, "a"
        ))
    }

    assertResult(Some(sources.StringContains("cstr", "a"))) {
      DataSourceStrategy.translateFilter(
        expressions.Contains(attrStr, "a"
        ))
    }
  }

  test("translate complex expression") {
    val attrInt = 'cint.int
    // Functions such as 'Abs' are not supported
    assertResult(None) {
      DataSourceStrategy.translateFilter(
        expressions.LessThanOrEqual(
          expressions.Subtract(expressions.Abs(attrInt), 2), 1))
    }

    assertResult(Some(sources.Or(
      sources.And(
        sources.GreaterThan("cint", 1),
        sources.LessThan("cint", 10)),
      sources.And(
        sources.GreaterThan("cint", 50),
        sources.LessThan("cint", 100))))) {
      DataSourceStrategy.translateFilter(expressions.Or(
        expressions.And(
          expressions.GreaterThan(attrInt, 1),
          expressions.LessThan(attrInt, 10)
        ),
        expressions.And(
          expressions.GreaterThan(attrInt, 50),
          expressions.LessThan(attrInt, 100)
        )
      ))
    }
    // SPARK-22548 Incorrect nested AND expression pushed down to JDBC data source
    // Functions such as 'Abs' are not supported
    assertResult(None) {
      DataSourceStrategy.translateFilter(expressions.Or(
        expressions.And(
          expressions.GreaterThan(attrInt, 1),
          expressions.LessThan(
            expressions.Abs(attrInt), 10)
        ),
        expressions.And(
          expressions.GreaterThan(attrInt, 50),
          expressions.LessThan(attrInt, 100)
        )
      ))
    }
    // Functions such as 'Abs' are not supported
    assertResult(None) {
      DataSourceStrategy.translateFilter(
        expressions.Not(expressions.And(
          expressions.Or(
            expressions.LessThanOrEqual(attrInt, 1),
            expressions.GreaterThanOrEqual(
              expressions.Abs(attrInt),
              10)
          ),
          expressions.Or(
            expressions.LessThanOrEqual(attrInt, 50),
            expressions.GreaterThanOrEqual(attrInt, 100)
          )
        )))
    }

    assertResult(Some(sources.Or(
      sources.Or(
        sources.EqualTo("cint", 1),
        sources.EqualTo("cint", 10)),
      sources.Or(
        sources.GreaterThan("cint", 0),
        sources.LessThan("cint", -10))))) {
      DataSourceStrategy.translateFilter(expressions.Or(
        expressions.Or(
          expressions.EqualTo(attrInt, 1),
          expressions.EqualTo(attrInt, 10)
        ),
        expressions.Or(
          expressions.GreaterThan(attrInt, 0),
          expressions.LessThan(attrInt, -10)
        )
      ))
    }
    // Functions such as 'Abs' are not supported
    assertResult(None) {
      DataSourceStrategy.translateFilter(expressions.Or(
        expressions.Or(
          expressions.EqualTo(attrInt, 1),
          expressions.EqualTo(
            expressions.Abs(attrInt), 10)
        ),
        expressions.Or(
          expressions.GreaterThan(attrInt, 0),
          expressions.LessThan(attrInt, -10)
        )
      ))
    }

    assertResult(Some(sources.And(
      sources.And(
        sources.GreaterThan("cint", 1),
        sources.LessThan("cint", 10)),
      sources.And(
        sources.EqualTo("cint", 6),
        sources.IsNotNull("cint"))))) {
      DataSourceStrategy.translateFilter(expressions.And(
        expressions.And(
          expressions.GreaterThan(attrInt, 1),
          expressions.LessThan(attrInt, 10)
        ),
        expressions.And(
          expressions.EqualTo(attrInt, 6),
          expressions.IsNotNull(attrInt)
        )
      ))
    }
    // Functions such as 'Abs' are not supported
    assertResult(None) {
      DataSourceStrategy.translateFilter(expressions.And(
        expressions.And(
          expressions.GreaterThan(attrInt, 1),
          expressions.LessThan(attrInt, 10)
        ),
        expressions.And(
          expressions.EqualTo(expressions.Abs(attrInt), 6),
          expressions.IsNotNull(attrInt)
        )
      ))
    }

    assertResult(Some(sources.And(
      sources.Or(
        sources.GreaterThan("cint", 1),
        sources.LessThan("cint", 10)),
      sources.Or(
        sources.EqualTo("cint", 6),
        sources.IsNotNull("cint"))))) {
      DataSourceStrategy.translateFilter(expressions.And(
        expressions.Or(
          expressions.GreaterThan(attrInt, 1),
          expressions.LessThan(attrInt, 10)
        ),
        expressions.Or(
          expressions.EqualTo(attrInt, 6),
          expressions.IsNotNull(attrInt)
        )
      ))
    }
    // Functions such as 'Abs' are not supported
    assertResult(None) {
      DataSourceStrategy.translateFilter(expressions.And(
        expressions.Or(
          expressions.GreaterThan(attrInt, 1),
          expressions.LessThan(attrInt, 10)
        ),
        expressions.Or(
          expressions.EqualTo(expressions.Abs(attrInt), 6),
          expressions.IsNotNull(attrInt)
        )
      ))
    }
  }
}
