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
import org.apache.spark.sql.catalyst.expressions.{CheckOverflow, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.DecimalType

class NullPropagationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("NullPropagation", FixedPoint(50), NullPropagation) :: Nil
  }

  test("SPARK-59719: fold a null-intolerant CheckOverflow over a null literal to null") {
    // With only NullPropagation in the batch, the fold happens because CheckOverflow is
    // null-intolerant: a null child implies a null result.
    val dt = DecimalType(18, 0)
    val relation = LocalRelation($"a".int)
    val query = relation
      .select(CheckOverflow(Literal.create(null, dt), dt, nullOnOverflow = true).as("c"))
      .analyze
    val optimized = Optimize.execute(query)
    val correctAnswer = relation.select(Literal.create(null, dt).as("c")).analyze
    comparePlans(optimized, correctAnswer)
  }
}
