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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull, PredicateHelper}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.test.SharedSQLContext


class ReorderedPredicateSuite extends QueryTest with SharedSQLContext with PredicateHelper {

  setupTestData()

  // Verifies that the IsNotNull operators precede rest of the operators
  private def verifyOrder(condition: Expression): Unit = {
    splitConjunctivePredicates(condition).sliding(2).foreach { case Seq(x, y) =>
      assert(x.isInstanceOf[IsNotNull] || !y.isInstanceOf[IsNotNull])
    }
  }

  test("null ordering in filter predicates") {
    val physicalPlan = sql(
      """
        |SELECT * from testData
        |WHERE value != '5' AND value IS NOT NULL
      """.stripMargin)
      .queryExecution.sparkPlan
    assert(physicalPlan.find(_.isInstanceOf[Filter]).isDefined)
    physicalPlan.collect {
      case Filter(condition, _) =>
        verifyOrder(condition)
    }
  }

  test("null ordering in join predicates") {
    sqlContext.cacheManager.clearCache()
    val physicalPlan = sql(
      """
        |SELECT * FROM testData t1
        |LEFT SEMI JOIN testData t2
        |ON t1.key = t2.key
        |AND t1.key + t2.key != 5
        |AND CONCAT(t1.value, t2.value) IS NOT NULL
      """.stripMargin)
      .queryExecution.sparkPlan
    assert(physicalPlan.find(_.isInstanceOf[LeftSemiJoinHash]).isDefined)
    physicalPlan.collect {
      case LeftSemiJoinHash(_, _, _, _, conditionOpt) =>
        verifyOrder(conditionOpt.get)
    }
  }
}
