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

import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, CurrentDate, Literal, MakeTimestampNTZ}
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.types.{TimestampNTZType, TimeType}

class RewriteTimeCastToTimestampNTZSuite extends AnalysisTest {
  test("SPARK-52617: RewriteTimeCastToTimestampNTZ rewrites") {

    // TIME: 15:30:00 => nanos = 15 * 3600 + 30 * 60 = 55800s => nanos
    val nanos = 55800L * 1_000_000_000L
    val timeLiteral = Literal(nanos, TimeType(6))

    val originalPlan =
      Project(Seq(Alias(Cast(timeLiteral, TimestampNTZType), "ts")()), OneRowRelation())
    val expectedPlan = Project(
      Seq(
        Alias(MakeTimestampNTZ(CurrentDate(), Literal.create(timeLiteral, TimeType(6))), "ts")()),
      OneRowRelation())

    val rewrittenPlan = RewriteTimeCastToTimestampNTZ(originalPlan)
    comparePlans(rewrittenPlan, expectedPlan)
  }
}
