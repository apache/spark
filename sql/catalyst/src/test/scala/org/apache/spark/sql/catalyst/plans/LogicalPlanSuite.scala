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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._

/**
 * This suite is used to test [[LogicalPlan]]'s `resolveOperators` and make sure it can correctly
 * skips sub-trees that have already been marked as analyzed.
 */
class LogicalPlanSuite extends SparkFunSuite {
  private var invocationCount = 0
  private val function: PartialFunction[LogicalPlan, LogicalPlan] = {
    case p: Project =>
      invocationCount += 1
      p
  }

  private val testRelation = LocalRelation()

  test("resolveOperator runs on operators") {
    invocationCount = 0
    val plan = Project(Nil, testRelation)
    plan resolveOperators function

    assert(invocationCount === 1)
  }

  test("resolveOperator runs on operators recursively") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, testRelation))
    plan resolveOperators function

    assert(invocationCount === 2)
  }

  test("resolveOperator skips all ready resolved plans") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, testRelation))
    plan.foreach(_.setAnalyzed())
    plan resolveOperators function

    assert(invocationCount === 0)
  }

  test("resolveOperator skips partially resolved plans") {
    invocationCount = 0
    val plan1 = Project(Nil, testRelation)
    val plan2 = Project(Nil, plan1)
    plan1.foreach(_.setAnalyzed())
    plan2 resolveOperators function

    assert(invocationCount === 1)
  }
}
