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

import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

class SimplifyCastsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("SimplifyCasts", FixedPoint(50), SimplifyCasts) :: Nil
  }

  test("non-nullable to non-nullable array cast") {
    val input = LocalRelation('a.array(ArrayType(IntegerType, false)))
    val plan = input.select('a.cast(ArrayType(IntegerType, false)).as("casted")).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select('a.as("casted")).analyze
    comparePlans(optimized, expected)
  }

  test("non-nullable to nullable array cast") {
    val input = LocalRelation('a.array(ArrayType(IntegerType, false)))
    val array_intPrimitive = 'a.array(ArrayType(IntegerType, false))
    val plan = input.select('a.cast(ArrayType(IntegerType, true)).as("casted")).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select('a.as("casted")).analyze
    comparePlans(optimized, expected)
  }

  test("nullable to non-nullable array cast") {
    val input = LocalRelation('a.array(ArrayType(IntegerType, true)))
    val plan = input.select('a.cast(ArrayType(IntegerType, false)).as("casted")).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }

  test("nullable to nullable array cast") {
    val input = LocalRelation('a.array(ArrayType(IntegerType, true)))
    val plan = input.select('a.cast(ArrayType(IntegerType, true)).as("casted")).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select('a.as("casted")).analyze
    comparePlans(optimized, expected)
  }

  def map(keyType: DataType, valueType: DataType, nullable: Boolean): AttributeReference =
    map(MapType(keyType, valueType, nullable))

  def map(mapType: MapType): AttributeReference =
    AttributeReference("m", mapType)()

  test("non-nullable to non-nullable map cast") {
    val input = LocalRelation('m.array(MapType(StringType, StringType)))
    val map_notNull = map(StringType, StringType, false)
    val plan = input.select(map_notNull
      .cast(MapType(StringType, StringType, false)).as('m)).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select(map_notNull.as('m)).analyze
    comparePlans(optimized, expected)
  }

  test("non-nullable to nullable map cast") {
    val input = LocalRelation('m.array(MapType(StringType, StringType)))
    val map_notNull = map(StringType, StringType, false)
    val plan = input.select(map_notNull
      .cast(MapType(StringType, StringType, true)).as('m)).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select(map_notNull.as('m)).analyze
    comparePlans(optimized, expected)
  }

  test("nullable to non-nullable map cast") {
    val input = LocalRelation('m.array(MapType(StringType, StringType)))
    val map_Null = map(StringType, StringType, true)
    val plan = input.select(map_Null
      .cast(MapType(StringType, StringType, false)).as('m)).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }

  test("nullable to nullable map cast") {
    val input = LocalRelation('m.array(MapType(StringType, StringType)))
    val map_Null = map(StringType, StringType, true)
    val plan = input.select(map_Null
      .cast(MapType(StringType, StringType, true)).as('m)).analyze
    val optimized = Optimize.execute(plan)
    val expected = input.select(map_Null.as('m)).analyze
    comparePlans(optimized, expected)
  }
}

