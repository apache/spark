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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

class NormalizePlanSuite extends SparkFunSuite{

  test("Normalize Project") {
    val baselineCol1 = $"col1".int
    val testCol1 = baselineCol1.newInstance()
    val baselinePlan = LocalRelation(baselineCol1).select(baselineCol1)
    val testPlan = LocalRelation(testCol1).select(testCol1)

    assert(baselinePlan != testPlan)
    assert(NormalizePlan(baselinePlan) == NormalizePlan(testPlan))
  }

  test("Normalize ordering in a project list of an inner Project") {
    val baselinePlan =
      LocalRelation($"col1".int, $"col2".string).select($"col1", $"col2").select($"col1")
    val testPlan =
      LocalRelation($"col1".int, $"col2".string).select($"col2", $"col1").select($"col1")

    assert(baselinePlan != testPlan)
    assert(NormalizePlan(baselinePlan) == NormalizePlan(testPlan))
  }
}
