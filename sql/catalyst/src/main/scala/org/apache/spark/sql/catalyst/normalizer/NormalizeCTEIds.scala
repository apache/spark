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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.logical.{CacheTableAsSelect, CTERelationRef, LogicalPlan, UnionLoop, UnionLoopRef, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule

object NormalizeCTEIds extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val curId = new java.util.concurrent.atomic.AtomicLong()
    val cteIdToNewId = mutable.Map.empty[Long, Long]
    // Pre-collect all CTERelationRef.cteId in encounter order so that orphan refs
    // (those without an enclosing WithCTE in the current sub-tree) also get a
    // deterministic, parse-independent id. Without this pass, sameResult comparisons
    // performed by CacheManager.lookupCachedData on partial CTE bodies fail to
    // match across parses.
    plan.foreachWithSubqueries {
      case ref: CTERelationRef =>
        cteIdToNewId.getOrElseUpdate(ref.cteId, curId.getAndIncrement())
      case _ =>
    }
    applyInternal(plan, curId, cteIdToNewId)
  }

  private def applyInternal(
      plan: LogicalPlan,
      curId: AtomicLong,
      cteIdToNewId: mutable.Map[Long, Long]): LogicalPlan = {
    plan transformDownWithSubqueries {
      case ctas @ CacheTableAsSelect(_, plan, _, _, _, _, _) =>
        ctas.copy(plan = applyInternal(plan, curId, cteIdToNewId))

      case withCTE @ WithCTE(plan, cteDefs) =>
        val newCteDefs = cteDefs.map { cteDef =>
          cteIdToNewId.getOrElseUpdate(cteDef.id, curId.getAndIncrement())
          val normalizedCteDef = canonicalizeCTE(cteDef.child, cteIdToNewId)
          cteDef.copy(child = normalizedCteDef, id = cteIdToNewId(cteDef.id))
        }
        val normalizedPlan = canonicalizeCTE(plan, cteIdToNewId)
        withCTE.copy(plan = normalizedPlan, cteDefs = newCteDefs)

      // Handle orphan CTERelationRef sub-trees not enclosed by WithCTE.
      // The pre-pass in `apply` populates cteIdToNewId for all such refs.
      case ref: CTERelationRef if cteIdToNewId.contains(ref.cteId) =>
        ref.copy(cteId = cteIdToNewId(ref.cteId))
    }
  }

  private def canonicalizeCTE(
      plan: LogicalPlan,
      defIdToNewId: mutable.Map[Long, Long]): LogicalPlan = {
    plan.transformDownWithSubqueries {
      // For nested WithCTE, if defIndex didn't contain the cteId,
      // means it's not current WithCTE's ref.
      case ref: CTERelationRef if defIdToNewId.contains(ref.cteId) =>
        ref.copy(cteId = defIdToNewId(ref.cteId))
      case unionLoop: UnionLoop if defIdToNewId.contains(unionLoop.id) =>
        unionLoop.copy(id = defIdToNewId(unionLoop.id))
      case unionLoopRef: UnionLoopRef if defIdToNewId.contains(unionLoopRef.loopId) =>
        unionLoopRef.copy(loopId = defIdToNewId(unionLoopRef.loopId))
    }
  }
}
