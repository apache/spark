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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType


class UpdateNullabilityInAttributeReferencesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("InferAndPushDownFilters", FixedPoint(100),
        InferFiltersFromConstraints) ::
      Batch("UpdateAttributeReferences", Once,
        UpdateNullabilityInAttributeReferences) :: Nil
  }

  test("update nullability when inferred constraints applied")  {
    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "true") {
      val testRelation = LocalRelation('a.int, 'b.int)
      val logicalPlan = testRelation.where('a =!= 2).select('a).analyze
      var expectedSchema = new StructType().add("a", "INT", nullable = true)
      assert(StructType.fromAttributes(logicalPlan.output) === expectedSchema)
      val optimizedPlan = Optimize.execute(logicalPlan)
      expectedSchema = new StructType().add("a", "INT", nullable = false)
      assert(StructType.fromAttributes(optimizedPlan.output) === expectedSchema)
    }
  }
}
