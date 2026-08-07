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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DateFormatClass, GetTimestamp}
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

  test("SPARK-57575: SimplifyDateTimeConversions skips TimeType child") {
    val timeAttr = AttributeReference("t", TimeType(TimeType.NANOS_PRECISION))()
    val timeRelation = LocalRelation(timeAttr)
    val pattern = "HH:mm:ss.SSSSSSSSS"

    val df = DateFormatClass(timeAttr, pattern)

    // date_format(to_timestamp(date_format(time_col, p), p), p) should NOT simplify
    // because TIME(9) -> Timestamp truncates sub-micro precision (nanos lost)
    val originalQuery = timeRelation
      .select(
        DateFormatClass(
          GetTimestamp(
            df,
            pattern,
            TimestampType),
          pattern) as "c1")
      .analyze

    val optimized = Optimize.execute(originalQuery)

    // Should NOT be simplified - optimized plan should equal original (no rewrite)
    comparePlans(optimized, originalQuery)
  }

  test("SPARK-57816: SimplifyDateTimeConversions skips nanosecond timestamp child") {
    val pattern = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"
    Seq(TimestampNTZNanosType(9), TimestampLTZNanosType(9)).foreach { nanosType =>
      val nanosAttr = AttributeReference("t", nanosType)()
      val nanosRelation = LocalRelation(nanosAttr)

      val df = DateFormatClass(nanosAttr, pattern)

      // date_format(to_timestamp(date_format(nanos_col, p), p), p) should NOT simplify because
      // the round trip through micro TimestampType truncates sub-microsecond precision.
      val originalQuery = nanosRelation
        .select(
          DateFormatClass(
            GetTimestamp(
              df,
              pattern,
              TimestampType),
            pattern) as "c1")
        .analyze

      val optimized = Optimize.execute(originalQuery)

      // Should NOT be simplified - optimized plan should equal original (no rewrite)
      comparePlans(optimized, originalQuery)
    }
  }
}
