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

import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

class AggregateFieldExtractionPushdownSuite extends SchemaPruningTest {
  private val testRelation =
    LocalRelation(
      StructField("a", StructType(
        StructField("a1", IntegerType) :: Nil)),
      StructField("b", IntegerType),
      StructField("c", StructType(
        StructField("c1", IntegerType) :: Nil)))

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Aggregate Field Extraction Pushdown", Once,
        AggregateFieldExtractionPushdown) :: Nil
  }

  test("basic aggregate field extraction pushdown") {
    val originalQuery =
      testRelation
        .select('a)
        .groupBy('a getField "a1")('a getField "a1" as 'a1, Count("*"))
        .analyze
    val optimized = Optimizer.execute(originalQuery)
    val correctAnswer =
      testRelation
        .select('a)
        .select('a getField "a1" as 'a1)
        .groupBy('a1)('a1 as 'a1, Count("*"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not push down a field extractor whose root child output is required by the aggregate") {
    val originalQuery =
      testRelation
        .select('a, 'c)
        .groupBy('a, 'a getField "a1", 'c getField "c1")(
          'a, 'a getField "a1" as 'a1, 'c getField "c1" as 'c1, Count("*"))
        .analyze
    val optimized = Optimizer.execute(originalQuery)
    val correctAnswer =
      testRelation
        .select('a, 'c)
        .select('a, 'c getField "c1" as 'c1)
        .groupBy('a, 'a getField "a1", 'c1)('a, 'a getField "a1" as 'a1, 'c1 as 'c1, Count("*"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }
}
