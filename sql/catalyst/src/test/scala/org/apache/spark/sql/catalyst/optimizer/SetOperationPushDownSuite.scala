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

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class SetOperationPushDownSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
      Batch("Union Pushdown", Once,
        SetOperationPushDown) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation2 = LocalRelation('d.int, 'e.int, 'f.int)
  val testUnion = Union(testRelation, testRelation2)
  val testIntersect = Intersect(testRelation, testRelation2)
  val testExcept = Except(testRelation, testRelation2)

  test("union/intersect/except: filter to each side") {
    val unionQuery = testUnion.where('a === 1)
    val intersectQuery = testIntersect.where('b < 10)
    val exceptQuery = testExcept.where('c >= 5)

    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val intersectOptimized = Optimize.execute(intersectQuery.analyze)
    val exceptOptimized = Optimize.execute(exceptQuery.analyze)

    val unionCorrectAnswer =
      Union(testRelation.where('a === 1), testRelation2.where('d === 1)).analyze
    val intersectCorrectAnswer =
      Intersect(testRelation.where('b < 10), testRelation2.where('e < 10)).analyze
    val exceptCorrectAnswer =
      Except(testRelation.where('c >= 5), testRelation2.where('f >= 5)).analyze

    comparePlans(unionOptimized, unionCorrectAnswer)
    comparePlans(intersectOptimized, intersectCorrectAnswer)
    comparePlans(exceptOptimized, exceptCorrectAnswer)
  }

  test("union/intersect/except: project to each side") {
    val unionQuery = testUnion.select('a)
    val intersectQuery = testIntersect.select('b, 'c)
    val exceptQuery = testExcept.select('a, 'b, 'c)

    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val intersectOptimized = Optimize.execute(intersectQuery.analyze)
    val exceptOptimized = Optimize.execute(exceptQuery.analyze)

    val unionCorrectAnswer =
      Union(testRelation.select('a), testRelation2.select('d)).analyze
    val intersectCorrectAnswer =
      Intersect(testRelation.select('b, 'c), testRelation2.select('e, 'f)).analyze
    val exceptCorrectAnswer =
      Except(testRelation.select('a, 'b, 'c), testRelation2.select('d, 'e, 'f)).analyze

    comparePlans(unionOptimized, unionCorrectAnswer)
    comparePlans(intersectOptimized, intersectCorrectAnswer)
    comparePlans(exceptOptimized, exceptCorrectAnswer)  }
}
