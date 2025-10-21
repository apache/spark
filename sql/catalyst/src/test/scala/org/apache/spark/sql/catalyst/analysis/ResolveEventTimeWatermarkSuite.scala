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

import org.apache.spark.sql.catalyst.analysis.TestRelations.streamingRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.unsafe.types.CalendarInterval

class ResolveEventTimeWatermarkSuite extends AnalysisTest {
  override protected def extendedAnalysisRules: Seq[Rule[LogicalPlan]] = {
    ResolveEventTimeWatermark +: super.extendedAnalysisRules
  }

  test("event time column expr refers to the column in child") {
    val planBeforeRule = streamingRelation
      .watermarkUnresolved($"ts", new CalendarInterval(0, 0, 1000))

    val analyzed = getAnalyzer.execute(planBeforeRule)

    // EventTimeWatermark node has UUID, hence we can't simply compare the plan
    // with expected shape of plan as a whole.
    val uuid = java.util.UUID.randomUUID()

    val uuidInjectedAnalyzed = analyzed.transform {
      case e: EventTimeWatermark => e.copy(nodeId = uuid)
    }

    comparePlans(
      uuidInjectedAnalyzed,
      EventTimeWatermark(
        uuid,
        streamingRelation.output.find(_.name == "ts").head,
        new CalendarInterval(0, 0, 1000),
        streamingRelation
      )
    )
  }

  test("event time column expr deduces a new column from alias") {
    val planBeforeRule = streamingRelation
      .watermarkUnresolved(
        Alias(
          UnresolvedFunction(
            Seq("timestamp_seconds"), Seq(UnresolvedAttribute("a")), isDistinct = false),
          "event_time"
        )(),
        new CalendarInterval(0, 0, 1000))

    val analyzed = getAnalyzer.execute(planBeforeRule)

    // EventTimeWatermark node has UUID, hence we can't simply compare the plan
    // with expected shape of plan as a whole.
    val uuid = java.util.UUID.randomUUID()

    val uuidInjectedAnalyzed = analyzed.transform {
      case e: EventTimeWatermark => e.copy(nodeId = uuid)
    }

    val selectAsResolved = getAnalyzer.execute(
      streamingRelation
        .select(
          Alias(
            UnresolvedFunction(
              Seq("timestamp_seconds"), Seq(UnresolvedAttribute("a")), isDistinct = false),
            "event_time"
          )(),
          // `*` will be resolved to `a`, `ts`
          streamingRelation.output.head,
          streamingRelation.output(1)
        )
    )

    val expectedLogicalPlan = EventTimeWatermark(
      uuid,
      selectAsResolved.output.find(_.name == "event_time").head,
      new CalendarInterval(0, 0, 1000),
      selectAsResolved
    )
    val expectedAnalyzed = getAnalyzer.execute(expectedLogicalPlan)

    comparePlans(
      uuidInjectedAnalyzed,
      expectedAnalyzed
    )
  }
}
