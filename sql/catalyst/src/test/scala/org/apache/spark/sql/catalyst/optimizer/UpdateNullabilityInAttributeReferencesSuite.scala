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
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateArray, GetArrayItem}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor


class UpdateNullabilityInAttributeReferencesSuite extends PlanTest {

  object Optimizer1 extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Constant Folding", FixedPoint(10),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        SimplifyConditionals,
        SimplifyBinaryComparison,
        SimplifyExtractValueOps) ::
      Batch("UpdateAttributeReferences", Once,
        UpdateNullabilityInAttributeReferences) :: Nil
  }

  object Optimizer2 extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Predicate Handling", FixedPoint(10),
        PushPredicateThroughJoin,
        InferFiltersFromConstraints) ::
      Batch("UpdateAttributeReferences", Once,
        UpdateNullabilityInAttributeReferences) :: Nil
  }

  test("update nullability in AttributeReference") {
    val rel = LocalRelation('a.long.notNull)
    // In the 'original' plans below, the Aggregate node produced by groupBy() has a
    // nullable AttributeReference to `b`, because both array indexing and map lookup are
    // nullable expressions. After optimization, the same attribute is now non-nullable,
    // but the AttributeReference is not updated to reflect this. So, we need to update nullability
    // by the `UpdateNullabilityInAttributeReferences` rule.
    val original = rel
      .select(GetArrayItem(CreateArray(Seq('a, 'a + 1L)), 0) as "b")
      .groupBy($"b")("1")
    val expected = rel.select('a as "b").groupBy($"b")("1").analyze
    val optimized = Optimizer1.execute(original.analyze)
    comparePlans(optimized, expected)
  }

  test("SPARK-XXXXX update nullability in Join output") {
    val r1 = LocalRelation('k1.int, 'v1.int)
    val r2 = LocalRelation('k2.int, 'v2.int)
    val joined = r1
      .join(r2, Inner, Some($"k1" === $"k2"))
      .where($"v1" + $"v2" > 0)
      .select($"v1", $"v2")
    val optimized = Optimizer2.execute(joined.analyze)
    val expected = r1
      .where($"k1".isNotNull && $"v1".isNotNull)
      .join(r2.where($"k2".isNotNull && $"v2".isNotNull), Inner,
        Some($"k1" === $"k2" && $"v1" + $"v2" > 0))
      .select($"v1", $"v2")
      .analyze match {
        // `projectList` should have the not-null `v1` and `v2`
        case p @ Project(projectList, _) =>
          p.copy(projectList = projectList.map {
            case a: Attribute => a.withNullability(false)
          })
      }
    comparePlans(optimized, expected)
  }
}
