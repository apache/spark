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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{DateFormatClass, GetTimestamp}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

class SimplifyDateTimeConversionsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("SimplifyDateTimeConversions", FixedPoint(50), SimplifyDateTimeConversions) :: Nil
  }

  val testRelation = LocalRelation($"ts".timestamp, $"s".string)

  test("SPARK-53762: Remove DateFormat - GetTimestamp groups") {
    val pattern = "yyyy-MM-dd"

    val df = DateFormatClass($"ts", pattern)
    val gt = GetTimestamp($"s", pattern, TimestampType)

    val originalQuery = testRelation
      .select(
        DateFormatClass(
          GetTimestamp(
            df,
            pattern,
            TimestampType),
          pattern) as "c1",
        GetTimestamp(
          DateFormatClass(
            gt,
            pattern),
          pattern,
          TimestampType) as "c2")
      .analyze

    val optimized = Optimize.execute(originalQuery)

    val expected = testRelation
      .select(
        df as "c1",
        gt as "c2")
      .analyze

    comparePlans(optimized, expected)
  }
}
