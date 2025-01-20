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

package org.apache.spark.sql.catalyst

import org.mockito.Mockito.{times, verify}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class QueryPlanningTrackerSuite extends SparkFunSuite {

  test("phases") {
    val t = new QueryPlanningTracker
    t.measurePhase("p1") {
      Thread.sleep(1)
    }

    assert(t.phases("p1").durationMs > 0)
    assert(!t.phases.contains("p2"))
  }

  test("multiple measurePhase call") {
    val t = new QueryPlanningTracker
    t.measurePhase("p1") { Thread.sleep(1) }
    val s1 = t.phases("p1")
    assert(s1.durationMs > 0)

    t.measurePhase("p1") { Thread.sleep(1) }
    val s2 = t.phases("p1")
    assert(s2.durationMs > s1.durationMs)
  }

  test("rules") {
    val t = new QueryPlanningTracker
    t.recordRuleInvocation("r1", 1, effective = false)
    t.recordRuleInvocation("r2", 2, effective = true)
    t.recordRuleInvocation("r3", 1, effective = false)
    t.recordRuleInvocation("r3", 2, effective = true)

    val rules = t.rules

    assert(rules("r1").totalTimeNs == 1)
    assert(rules("r1").numInvocations == 1)
    assert(rules("r1").numEffectiveInvocations == 0)

    assert(rules("r2").totalTimeNs == 2)
    assert(rules("r2").numInvocations == 1)
    assert(rules("r2").numEffectiveInvocations == 1)

    assert(rules("r3").totalTimeNs == 3)
    assert(rules("r3").numInvocations == 2)
    assert(rules("r3").numEffectiveInvocations == 1)
  }

  test("topRulesByTime") {
    val t = new QueryPlanningTracker

    // Return empty seq when k = 0
    assert(t.topRulesByTime(0) == Seq.empty)
    assert(t.topRulesByTime(1) == Seq.empty)

    t.recordRuleInvocation("r2", 2, effective = true)
    t.recordRuleInvocation("r4", 4, effective = true)
    t.recordRuleInvocation("r1", 1, effective = false)
    t.recordRuleInvocation("r3", 3, effective = false)

    // k <= total size
    assert(t.topRulesByTime(0) == Seq.empty)
    val top = t.topRulesByTime(2)
    assert(top.size == 2)
    assert(top(0)._1 == "r4")
    assert(top(1)._1 == "r3")

    // k > total size
    assert(t.topRulesByTime(10).size == 4)
  }

  test("test ready for execution callback") {
    val mockCallback = mock[QueryPlanningTrackerCallback]
    val mockPlan1 = mock[LogicalPlan]
    val mockPlan2 = mock[LogicalPlan]
    val mockPlan3 = mock[LogicalPlan]
    val mockPlan4 = mock[LogicalPlan]
    val t = new QueryPlanningTracker(Some(mockCallback))
    t.setAnalysisFailed(mockPlan3)
    verify(mockCallback, times(1)).analysisFailed(t, mockPlan3)
    t.setAnalysisFailed(mockPlan4)
    verify(mockCallback, times(1)).analysisFailed(t, mockPlan4)
    t.setAnalyzed(mockPlan1)
    verify(mockCallback, times(1)).analyzed(t, mockPlan1)
    t.setAnalyzed(mockPlan2)
    verify(mockCallback, times(1)).analyzed(t, mockPlan2)
    t.setReadyForExecution()
    verify(mockCallback, times(1)).readyForExecution(t)
    t.setReadyForExecution()
    verify(mockCallback, times(1)).readyForExecution(t)
  }
}
