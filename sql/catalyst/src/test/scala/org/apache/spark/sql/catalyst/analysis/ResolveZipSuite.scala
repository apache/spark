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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ResolveZipSuite extends AnalysisTest {

  private val base = LocalRelation($"a".int, $"b".int, $"c".int)

  object Resolve extends RuleExecutor[LogicalPlan] {
    override val batches: Seq[Batch] = Seq(
      Batch("ResolveZip", Once, ResolveZip))
  }

  test("resolve Zip: both sides have Project over same base") {
    val left = Project(Seq(base.output(0)), base)
    val right = Project(Seq(base.output(1)), base)
    val zip = Zip(left, right)

    val resolved = Resolve.execute(zip)
    val expected = Project(Seq(base.output(0), base.output(1)), base)
    comparePlans(resolved, expected)
  }

  test("resolve Zip: left is bare plan, right has Project") {
    val right = Project(Seq(base.output(0)), base)
    val zip = Zip(base, right)

    val resolved = Resolve.execute(zip)
    val expected = Project(base.output ++ Seq(base.output(0)), base)
    comparePlans(resolved, expected)
  }

  test("resolve Zip: both sides are bare same plan") {
    val zip = Zip(base, base)

    val resolved = Resolve.execute(zip)
    val expected = Project(base.output ++ base.output, base)
    comparePlans(resolved, expected)
  }

  test("resolve Zip: both sides have expressions over same base") {
    val left = base.select(($"a" + 1).as("a_plus_1"))
    val right = base.select(($"b" * 2).as("b_times_2"))
    val zip = Zip(left.analyze, right.analyze)

    val resolved = Resolve.execute(zip)
    assert(!resolved.isInstanceOf[Zip], "Zip should have been resolved to a Project")
    assert(resolved.isInstanceOf[Project])
    assert(resolved.output.length == 2)
    assert(resolved.output(0).name == "a_plus_1")
    assert(resolved.output(1).name == "b_times_2")
  }

  test("resolve Zip: different base plans - Zip remains unresolved") {
    val base2 = LocalRelation($"x".int, $"y".int, $"z".int, $"w".int)
    val left = Project(Seq(base.output(0)), base)
    val right = Project(Seq(base2.output(0)), base2)
    val zip = Zip(left, right)

    val resolved = Resolve.execute(zip)
    // ResolveZip cannot merge, so Zip stays
    assert(resolved.isInstanceOf[Zip])
  }

  test("CheckAnalysis: different base plans throws ZIP_PLANS_NOT_MERGEABLE") {
    val base2 = LocalRelation($"x".int, $"y".int, $"z".int, $"w".int)
    val left = Project(Seq(base.output(0)), base)
    val right = Project(Seq(base2.output(0)), base2)
    val zip = Zip(left, right)

    assertAnalysisErrorCondition(
      zip,
      expectedErrorCondition = "ZIP_PLANS_NOT_MERGEABLE",
      expectedMessageParameters = Map.empty
    )
  }
}
