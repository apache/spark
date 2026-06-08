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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

/**
 * Test suite for
 * [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveTimestampNanosExpressions]].
 */
class ResolveTimestampNanosExpressionsSuite extends PlanTest {
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private object Resolve extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Resolve", FixedPoint(4), SimpleAnalyzer.ResolveTimestampNanosExpressions) :: Nil
  }

  private val ntzNanos = AttributeReference("ntz", TimestampNTZNanosType(9))()
  private val ltzNanos = AttributeReference("ltz", TimestampLTZNanosType(9))()
  private val microNtz = AttributeReference("mntz", TimestampNTZType)()
  private val rel = LocalRelation(Seq(ntzNanos, ltzNanos, microNtz))

  // Applies only the rule under test and returns the (possibly) rewritten top expression.
  private def rewrite(e: Expression): Expression = {
    val rewritten = Resolve.execute(rel.select(e.as("res")))
    rewritten.asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
  }

  private def checkRewrite(in: Expression, expected: Expression): Unit = {
    assert(rewrite(in) == expected)
  }

  test("TIMESTAMP_NTZ nanos child is cast to TIMESTAMP_NTZ") {
    checkRewrite(Hour(ntzNanos), Hour(Cast(ntzNanos, TimestampNTZType)))
    checkRewrite(Minute(ntzNanos), Minute(Cast(ntzNanos, TimestampNTZType)))
    checkRewrite(Second(ntzNanos), Second(Cast(ntzNanos, TimestampNTZType)))
  }

  test("TIMESTAMP_LTZ nanos child is cast to TIMESTAMP") {
    checkRewrite(Hour(ltzNanos), Hour(Cast(ltzNanos, TimestampType)))
    checkRewrite(Minute(ltzNanos), Minute(Cast(ltzNanos, TimestampType)))
    checkRewrite(Second(ltzNanos), Second(Cast(ltzNanos, TimestampType)))
  }

  test("microsecond timestamp inputs are left unchanged") {
    checkRewrite(Hour(microNtz), Hour(microNtz))
  }

  test("SecondWithFraction is not rewritten because it depends on sub-microsecond digits") {
    checkRewrite(SecondWithFraction(ntzNanos), SecondWithFraction(ntzNanos))
    checkRewrite(SecondWithFraction(ltzNanos), SecondWithFraction(ltzNanos))
  }
}
