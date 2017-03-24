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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class ReorderPredicatesInFilterSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("ReorderPredicatesInFilter", Once,
        ReorderPredicatesInFilter) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("Reorder Filter deterministic predicates") {
    val udf = ScalaUDF((x: Int, y: Int) => 1, IntegerType, Seq('a, 'b))

    val originalQuery1 =
      testRelation
        .select('a, 'b).where(udf > 2 && 'a > 1 && 'b > 2)
    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 =
      testRelation
        .select('a, 'b).where('a > 1 && 'b > 2 && udf > 2).analyze
    comparePlans(optimized1, correctAnswer1)

    val originalQuery2 =
      testRelation
        .select('a, 'b, 'c).where('c > 5 && udf > 2 && 'a > 1 && 'b > 2)
    val optimized2 = Optimize.execute(originalQuery2.analyze)
    val correctAnswer2 =
      testRelation
        .select('a, 'b, 'c).where('c > 5 && 'a > 1 && 'b > 2 && udf > 2).analyze
    comparePlans(optimized2, correctAnswer2)

    val originalQuery3 =
      testRelation
        .select('a, 'b, 'c).where(('c > 5 || udf > 2) && 'a > 1 && 'b > 2)
    val optimized3 = Optimize.execute(originalQuery3.analyze)
    val correctAnswer3 =
      testRelation
        .select('a, 'b, 'c).where('a > 1 && 'b > 2 && ('c > 5 || udf > 2)).analyze
    comparePlans(optimized3, correctAnswer3)
  }

  test("Reorder Filter non deterministic predicates") {
    val udf = ScalaUDF((x: Int, y: Int) => 1, IntegerType, Seq('a, 'b))
    // The UDF is before non-deterministic expression, we can't reorder it.
    val originalQuery1 =
      testRelation
        .select('a, 'b, 'c).where('c > 5 && udf > 2 && Rand(0) > 0.1 && 'a > 1 && 'b > 2)
    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 = originalQuery1.analyze
    comparePlans(optimized1, correctAnswer1)

    val originalQuery2 =
      testRelation
        .select('a, 'b).where((Rand(0) > 0.1 && udf > 2) && 'a > 1 && 'b > 2)
    val optimized2 = Optimize.execute(originalQuery2.analyze)
    val correctAnswer2 =
      testRelation
        .select('a, 'b).where(Rand(0) > 0.1 && 'a > 1 && 'b > 2 && udf > 2).analyze
    comparePlans(optimized2, correctAnswer2)

    // The UDF is in a disjunctive with a non-deterministic expression, we can't reorder it.
    val originalQuery3 =
      testRelation
        .select('a, 'b).where((Rand(0) > 0.1 || udf > 2) && 'a > 1 && 'b > 2)
    val optimized3 = Optimize.execute(originalQuery3.analyze)
    val correctAnswer3 = originalQuery3.analyze
    comparePlans(optimized3, correctAnswer3)

    // The UDF is put among non-deterministic expressions, we can't reorder it.
    val originalQuery4 =
      testRelation
        .select('a, 'b).where(Rand(0) > 0.1 && udf > 2 && Rand(1) > 0.2 && 'a > 1 && 'b > 2)
    val optimized4 = Optimize.execute(originalQuery4.analyze)
    val correctAnswer4 = originalQuery4.analyze
    comparePlans(optimized4, correctAnswer4)
  }
}
