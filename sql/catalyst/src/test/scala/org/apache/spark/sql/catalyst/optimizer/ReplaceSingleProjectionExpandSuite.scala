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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ReplaceSingleProjectionExpandSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("ReplaceSingleProjectionExpand", Once,
        ReplaceSingleProjectionExpand) :: Nil
  }

  test("Replace single projection expand with project") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = Expand(
        Seq(Seq('a, 'b, Literal(4))),
        Seq('a, 'b, 'c.int),
        relation
      ).analyze
    val expected = relation
      .select('a, 'b, Literal(4) as "c")
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Do not change expand with multiple projections") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = Expand(
      Seq(
        Seq('a, 'b, Literal(4)),
        Seq('b, 'a, Literal(5))
      ),
      Seq('a, 'b, 'c.int),
      relation
    ).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

}
