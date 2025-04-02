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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class SimplifyCastsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("SimplifyCasts", FixedPoint(50), SimplifyCasts) :: Nil
  }

  test("non-nullable element array to nullable element array cast") {
    val input = LocalRelation($"a".array(ArrayType(IntegerType, false)))
    val plan = input.select($"a".cast(ArrayType(IntegerType, true)).as("casted")).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select($"a".as("casted")).analyze
    comparePlans(optimized, expected)
  }

  test("nullable element to non-nullable element array cast") {
    val input = LocalRelation($"a".array(ArrayType(IntegerType, true)))
    val attr = input.output.head
    val plan = input.select(attr.cast(ArrayType(IntegerType, false)).as("casted"))
    val optimized = Optimize.execute(plan)
    // Though cast from `ArrayType(IntegerType, true)` to `ArrayType(IntegerType, false)` is not
    // allowed, here we just ensure that `SimplifyCasts` rule respect the plan.
    comparePlans(optimized, plan, checkAnalysis = false)
  }

  test("non-nullable value map to nullable value map cast") {
    val input = LocalRelation(Symbol("m").map(MapType(StringType, StringType, false)))
    val plan = input.select($"m".cast(MapType(StringType, StringType, true))
      .as("casted")).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select($"m".as("casted")).analyze
    comparePlans(optimized, expected)
  }

  test("nullable value map to non-nullable value map cast") {
    val input = LocalRelation(Symbol("m").map(MapType(StringType, StringType, true)))
    val attr = input.output.head
    val plan = input.select(attr.cast(MapType(StringType, StringType, false))
      .as("casted"))
    val optimized = Optimize.execute(plan)
    // Though cast from `MapType(StringType, StringType, true)` to
    // `MapType(StringType, StringType, false)` is not allowed, here we just ensure that
    // `SimplifyCasts` rule respect the plan.
    comparePlans(optimized, plan, checkAnalysis = false)
  }

  test("SPARK-37922: Combine to one cast if we can safely up-cast two casts") {
    val input = LocalRelation($"a".int, $"b".decimal(18, 2), $"c".date, $"d".timestamp)

    // Combine casts
    comparePlans(
      Optimize.execute(
        input.select($"a".cast(DecimalType(18, 1)).cast(DecimalType(19, 1)).as("casted")).analyze),
      input.select($"a".cast(DecimalType(19, 1)).as("casted")).analyze)
    comparePlans(
      Optimize.execute(
        input.select($"a".cast(LongType).cast(DecimalType(22, 1)).as("casted")).analyze),
      input.select($"a".cast(DecimalType(22, 1)).as("casted")).analyze)
    comparePlans(
      Optimize.execute(
        input.select($"b".cast(DecimalType(20, 2)).cast(DecimalType(24, 2)).as("casted")).analyze),
      input.select($"b".cast(DecimalType(24, 2)).as("casted")).analyze)

    // Can not combine casts
    comparePlans(
      Optimize.execute(
        input.select($"a".cast(DecimalType(2, 1)).cast(DecimalType(3, 1)).as("casted")).analyze),
      input.select($"a".cast(DecimalType(2, 1)).cast(DecimalType(3, 1)).as("casted")).analyze)
    comparePlans(
      Optimize.execute(
        input.select($"b".cast(DecimalType(10, 2)).cast(DecimalType(24, 2)).as("casted")).analyze),
      input.select($"b".cast(DecimalType(10, 2)).cast(DecimalType(24, 2)).as("casted")).analyze)

    withClue("SPARK-39963: cast date to decimal") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> false.toString) {
        // ANSI mode does not allow to cast a date to a decimal.
        comparePlans(Optimize.execute(
          input.select(
            $"c".cast(DecimalType(10, 2)).cast(DecimalType(24, 2)).as("casted")).analyze),
          input.select(
            $"c".cast(DecimalType(10, 2)).cast(DecimalType(24, 2)).as("casted")).analyze)
      }
    }

    comparePlans(
      Optimize.execute(
        input.select($"c".cast(TimestampType).cast(StringType).as("casted")).analyze),
      input.select($"c".cast(TimestampType).cast(StringType).as("casted")).analyze)
    comparePlans(
      Optimize.execute(
        input.select($"d".cast(LongType).cast(StringType).as("casted")).analyze),
      input.select($"d".cast(LongType).cast(StringType).as("casted")).analyze)
  }

  test("SPARK-45909: Remove the cast if it can safely up-cast in IsNotNull") {
    val input = LocalRelation($"a".int, $"b".decimal(18, 0))
    // Remove cast
    comparePlans(
      Optimize.execute(
        input.select($"a".cast(DecimalType(18, 1)).isNotNull.as("v")).analyze),
      input.select($"a".isNotNull.as("v")).analyze)
    comparePlans(
      Optimize.execute(input.select($"a".cast(LongType).isNotNull.as("v")).analyze),
      input.select($"a".isNotNull.as("v")).analyze)
    comparePlans(
      Optimize.execute(input.select($"b".cast(LongType).isNotNull.as("v")).analyze),
      input.select($"b".isNotNull.as("v")).analyze)

    // Can not remove cast
    comparePlans(
      Optimize.execute(
        input.select($"a".cast(DecimalType(2, 1)).as("v")).analyze),
      input.select($"a".cast(DecimalType(2, 1)).as("v")).analyze)
  }
}
