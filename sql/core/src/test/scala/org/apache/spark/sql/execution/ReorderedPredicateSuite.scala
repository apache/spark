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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.joins.LeftSemiJoinHash
import org.apache.spark.sql.test.SharedSQLContext


class ReorderedPredicateSuite extends SharedSQLContext with PredicateHelper {

  setupTestData()

  // Verifies that (a) In the new condition, the IsNotNull operators precede rest of the operators
  // and (b) The relative sort order of IsNotNull and !IsNotNull operators is still maintained
  private def verifyStableOrder(before: Expression, after: Expression): Unit = {
    val oldPredicates = splitConjunctivePredicates(before)
    splitConjunctivePredicates(after).sliding(2).foreach { case Seq(x, y) =>
      // Verify IsNotNull operator ordering
      assert(x.isInstanceOf[IsNotNull] || !y.isInstanceOf[IsNotNull])

      // Verify stable sort order
      if ((x.isInstanceOf[IsNotNull] && y.isInstanceOf[IsNotNull]) ||
        (!x.isInstanceOf[IsNotNull] && !y.isInstanceOf[IsNotNull])) {
        assert(oldPredicates.indexOf(x) <= oldPredicates.indexOf(y))
      }
    }
  }

  test("null ordering in filter predicates") {
    val query = sql(
      """
        |SELECT * from testData
        |WHERE value != '5' AND value != '4' AND value IS NOT NULL AND key != 5
      """.stripMargin)
      .queryExecution

    val logicalPlan = query.optimizedPlan
    val physicalPlan = query.sparkPlan
    assert(logicalPlan.find(_.isInstanceOf[logical.Filter]).isDefined)
    assert(physicalPlan.find(_.isInstanceOf[execution.Filter]).isDefined)

    val logicalCondition = logicalPlan.collect {
      case logical.Filter(condition, _) =>
        condition
    }.head

    val physicalCondition = physicalPlan.collect {
      case Filter(condition, _) =>
        condition
    }.head

    verifyStableOrder(logicalCondition, physicalCondition)
  }

  test("null ordering in join predicates") {
    sqlContext.cacheManager.clearCache()
    val query = sql(
      """
        |SELECT * FROM testData t1
        |LEFT SEMI JOIN testData t2
        |ON t1.key = t2.key
        |AND t1.key + t2.key != 5
        |AND CONCAT(t1.value, t2.value) IS NOT NULL
      """.stripMargin)
      .queryExecution

    val logicalPlan = query.optimizedPlan
    val physicalPlan = query.sparkPlan
    assert(logicalPlan.find(_.isInstanceOf[Join]).isDefined)
    assert(physicalPlan.find(_.isInstanceOf[LeftSemiJoinHash]).isDefined)

    val logicalCondition = logicalPlan.collect {
      case Join(_, _, _, condition) =>
        condition.get
    }.head

    val physicalCondition = physicalPlan.collect {
      case LeftSemiJoinHash(_, _, _, _, conditionOpt) =>
        conditionOpt.get
    }.head

    verifyStableOrder(logicalCondition, physicalCondition)
  }
}
