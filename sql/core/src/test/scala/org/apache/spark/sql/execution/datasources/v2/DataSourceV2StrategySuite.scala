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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{EqualTo => V2EqualTo, Filter => V2Filter}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.BooleanType

class DataSourceV2StrategySuite extends PlanTest with SharedSparkSession {
  test("SPARK-36644: Push down boolean column filter") {
    testTranslateFilter('col.boolean,
      Some(new V2EqualTo(FieldReference("col"), LiteralValue(true, BooleanType))))
  }

  /**
   * Translate the given Catalyst [[Expression]] into data source [[V2Filter]]
   * then verify against the given [[V2Filter]].
   */
  def testTranslateFilter(catalystFilter: Expression, result: Option[V2Filter]): Unit = {
    assertResult(result) {
      DataSourceV2Strategy.translateFilterV2(catalystFilter, true)
    }
  }
}
