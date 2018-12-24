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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}

/**
 * Test suite for resolving Uuid expressions.
 */
class ResolvedUuidExpressionsSuite extends AnalysisTest {

  private lazy val a = 'a.int
  private lazy val r = LocalRelation(a)
  private lazy val uuid1 = Uuid().as('_uuid1)
  private lazy val uuid2 = Uuid().as('_uuid2)
  private lazy val uuid3 = Uuid().as('_uuid3)
  private lazy val uuid1Ref = uuid1.toAttribute

  private val tracker = new QueryPlanningTracker
  private val analyzer = getAnalyzer(caseSensitive = true)

  private def getUuidExpressions(plan: LogicalPlan): Seq[Uuid] = {
    plan.flatMap {
      case p =>
        p.expressions.flatMap(_.collect {
          case u: Uuid => u
        })
    }
  }

  test("analyzed plan sets random seed for Uuid expression") {
    val plan = r.select(a, uuid1)
    val resolvedPlan = analyzer.executeAndCheck(plan, tracker)
    getUuidExpressions(resolvedPlan).foreach { u =>
      assert(u.resolved)
      assert(u.randomSeed.isDefined)
    }
  }

  test("Uuid expressions should have different random seeds") {
    val plan = r.select(a, uuid1).groupBy(uuid1Ref)(uuid2, uuid3)
    val resolvedPlan = analyzer.executeAndCheck(plan, tracker)
    assert(getUuidExpressions(resolvedPlan).map(_.randomSeed.get).distinct.length == 3)
  }

  test("Different analyzed plans should have different random seeds in Uuids") {
    val plan = r.select(a, uuid1).groupBy(uuid1Ref)(uuid2, uuid3)
    val resolvedPlan1 = analyzer.executeAndCheck(plan, tracker)
    val resolvedPlan2 = analyzer.executeAndCheck(plan, tracker)
    val uuids1 = getUuidExpressions(resolvedPlan1)
    val uuids2 = getUuidExpressions(resolvedPlan2)
    assert(uuids1.distinct.length == 3)
    assert(uuids2.distinct.length == 3)
    assert(uuids1.intersect(uuids2).length == 0)
  }
}
