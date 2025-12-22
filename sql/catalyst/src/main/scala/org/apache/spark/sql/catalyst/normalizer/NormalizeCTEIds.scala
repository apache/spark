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

import java.util.HashMap
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.plans.logical.{CacheTableAsSelect, CTERelationRef, LogicalPlan, UnionLoop, UnionLoopRef, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule

object NormalizeCTEIds extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val curId = new java.util.concurrent.atomic.AtomicLong()
    val defIdToNewId = new HashMap[Long, Long]()
    applyInternal(plan, curId, defIdToNewId)
  }

  private def applyInternal(
      plan: LogicalPlan,
      curId: AtomicLong,
      defIdToNewId: HashMap[Long, Long]): LogicalPlan = {
    plan transformDownWithSubqueries {
      case ctas @ CacheTableAsSelect(_, plan, _, _, _, _, _) =>
        ctas.copy(plan = applyInternal(plan, curId, defIdToNewId))

      case withCTE @ WithCTE(plan, cteDefs) =>
        val newCteDefs = cteDefs.map { cteDef =>
          if (!defIdToNewId.containsKey(cteDef.id)) {
            defIdToNewId.put(cteDef.id, curId.getAndIncrement())
          }
          val normalizedCteDef = canonicalizeCTE(cteDef.child, defIdToNewId)
          cteDef.copy(child = normalizedCteDef, id = defIdToNewId.get(cteDef.id))
        }
        val normalizedPlan = canonicalizeCTE(plan, defIdToNewId)
        withCTE.copy(plan = normalizedPlan, cteDefs = newCteDefs)
    }
  }

  private def canonicalizeCTE(plan: LogicalPlan, defIdToNewId: HashMap[Long, Long]): LogicalPlan = {
    plan.transformDownWithSubqueries {
      // For nested WithCTE, if defIndex didn't contain the cteId,
      // means it's not current WithCTE's ref.
      case ref: CTERelationRef if defIdToNewId.containsKey(ref.cteId) =>
        ref.copy(cteId = defIdToNewId.get(ref.cteId))
      case unionLoop: UnionLoop if defIdToNewId.containsKey(unionLoop.id) =>
        unionLoop.copy(id = defIdToNewId.get(unionLoop.id))
      case unionLoopRef: UnionLoopRef if defIdToNewId.containsKey(unionLoopRef.loopId) =>
        unionLoopRef.copy(loopId = defIdToNewId.get(unionLoopRef.loopId))
    }
  }
}
