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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.MetadataBuilder

class RemoveAliasOnlyProjectSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("RemoveAliasOnlyProject", FixedPoint(50), RemoveAliasOnlyProject) :: Nil
  }

  test("all expressions in project list are aliased child output") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('a as 'a, 'b as 'b).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("all expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('b as 'b, 'a as 'a).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("some expressions in project list are aliased child output") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('a as 'a, 'b).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("some expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('b as 'b, 'a).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("some expressions in project list are not Alias or Attribute") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('a as 'a, 'b + 1).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("some expressions in project list are aliased child output but with metadata") {
    val relation = LocalRelation('a.int, 'b.int)
    val metadata = new MetadataBuilder().putString("x", "y").build()
    val aliasWithMeta = Alias('a, "a")(explicitMetadata = Some(metadata))
    val query = relation.select(aliasWithMeta, 'b).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }
}
