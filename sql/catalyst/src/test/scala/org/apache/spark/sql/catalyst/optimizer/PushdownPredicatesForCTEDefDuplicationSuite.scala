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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.{Union, WithCTE}
import org.apache.spark.sql.types.LongType

/**
 * Tests for distinct CTE definitions that share an ID after InlineCTE copies an enclosing
 * definition. Attribute deduplication gives the copies different expression IDs, so predicate
 * pushdown and column pruning state gathered for one copy cannot be applied to the other.
 */
class PushdownPredicatesForCTEDefDuplicationSuite extends PlanTest {

  test("SPARK-61957: duplicated CTE definitions do not share predicates") {
    // The two inner definitions share cteId 1 but have different attributes.
    // Each reference's predicate points at the outer r (cteId 0). A predicate
    // gathered for one copy cannot be pushed into the other, so the rule must
    // leave both copies unchanged.
    val rId = AttributeReference("id", LongType, nullable = false)()
    val rCteId = 0L
    val rDef = CTERelationDef(LocalRelation(rId), rCteId, materialized = Some(true))
    val plan = WithCTE(duplicatedDefsPlan(Seq(rId)) { ref =>
      val rRef = CTERelationRef(rCteId, true, Seq(rId.newInstance()), false)
      val maxR = Aggregate(
        Nil,
        Seq(Alias(Max(rRef.output.head).toAggregateExpression(), "max")()),
        rRef)
      Project(ref.output, Filter(GreaterThan(ref.output.head, ScalarSubquery(maxR)), ref))
    }, Seq(rDef))

    comparePlans(plan, PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan))
  }

  test("SPARK-61957: duplicated CTE definitions do not share pruning attributes") {
    // Each reference projects only d. Since the two definitions have different attributes, the
    // pruning state gathered for one cannot be used to rewrite the other. The rule must leave
    // both copies unchanged.
    val id = AttributeReference("id", LongType, nullable = false)()
    val k = AttributeReference("k", LongType, nullable = false)()
    val d = AttributeReference("d", LongType, nullable = false)()
    val plan = duplicatedDefsPlan(Seq(id, k, d)) { ref =>
      Project(Seq(ref.output(2)), ref)
    }

    comparePlans(plan, PushdownPredicatesAndPruneColumnsForCTEDef.apply(plan))
  }

  /**
   * Builds the post-InlineCTE shape with two materialized definitions sharing `cteId` but owning
   * different attributes. Each reference in turn has fresh attributes, as after self-join
   * deduplication. The callback builds the query above each reference.
   */
  private def duplicatedDefsPlan(
      output: Seq[Attribute])(query: CTERelationRef => LogicalPlan): LogicalPlan = {
    val cteId = 1L
    val branches = Seq(output.map(_.newInstance()), output.map(_.newInstance())).map { defOutput =>
      val cteDef = CTERelationDef(LocalRelation(defOutput), cteId, materialized = Some(true))
      val ref = CTERelationRef(cteId, true, defOutput.map(_.newInstance()), false)
      WithCTE(query(ref), Seq(cteDef))
    }
    Union(branches)
  }
}
