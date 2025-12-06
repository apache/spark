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

package org.apache.spark.sql.catalyst.normalizer

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{CacheTableAsSelect, CTERelationRef, LogicalPlan, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}

object WithCTENormalized extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val curId = new java.util.concurrent.atomic.AtomicLong()
    plan transformDown {

      case ctas @ CacheTableAsSelect(_, plan, _, _, _, _, _) =>
        ctas.copy(plan = apply(plan))

      case withCTE @ WithCTE(plan, cteDefs) =>
        val defIdToNewId = withCTE.cteDefs.map(_.id).map((_, curId.getAndIncrement())).toMap
        val normalizedPlan = canonicalizeCTE(plan, defIdToNewId)
        val newCteDefs = cteDefs.map { cteDef =>
          val normalizedCteDef = canonicalizeCTE(cteDef.child, defIdToNewId)
          cteDef.copy(child = normalizedCteDef, id = defIdToNewId(cteDef.id))
        }
        withCTE.copy(plan = normalizedPlan, cteDefs = newCteDefs)
    }
  }

  def canonicalizeCTE(plan: LogicalPlan, defIdToNewId: Map[Long, Long]): LogicalPlan = {
    plan.transformDownWithPruning(
      _.containsAnyPattern(CTE, PLAN_EXPRESSION)) {
      // For nested WithCTE, if defIndex didn't contain the cteId,
      // means it's not current WithCTE's ref.
      case ref: CTERelationRef if defIdToNewId.contains(ref.cteId) =>
        ref.copy(cteId = defIdToNewId(ref.cteId))
      case other =>
        other.transformExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
          case e: SubqueryExpression => e.withNewPlan(canonicalizeCTE(e.plan, defIdToNewId))
        }
    }
  }
}
