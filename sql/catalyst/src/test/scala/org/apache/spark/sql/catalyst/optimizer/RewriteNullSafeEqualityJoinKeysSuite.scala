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
import org.apache.spark.sql.catalyst.expressions.{And, Literal, Or}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class RewriteNullSafeEqualityJoinKeysSuite extends PlanTest with JoinSelectionHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Rewrite Null Safe Equality Join Keys", Once, RewriteNullSafeEqualityJoinKeys) :: Nil
  }

  val t1 = LocalRelation($"a".int, $"b".int)
  val t2 = LocalRelation($"x".int, $"y".int)

  test("Rewrite for equi-join") {
    val plan = t1.join(t2, condition = Some(And($"a" <=> $"x", $"b" === $"y"))).analyze
    val rewrittenCondition = And(
      And(
        coalesce($"a", Literal.default(IntegerType)) ===
          coalesce($"x", Literal.default(IntegerType)),
        $"a".isNull === $"x".isNull
      ),
      $"b" === $"y"
    )
    val expected = t1.join(t2, condition = Some(rewrittenCondition)).analyze
    comparePlans(Optimize.execute(plan), expected)
  }

  test("Do not rewrite for non-equi-join") {
    val plan = t1.join(t2, condition = Some(Or($"a" <=> $"x", $"b" > $"y"))).analyze
    comparePlans(Optimize.execute(plan), plan)
  }
}
