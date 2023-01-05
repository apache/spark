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

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.columnar.InMemoryRelation

/**
 * [[InMemoryRelation]] is at sql/core module, so this helper trait also places in sql/core.
 */
trait SelectivePredicateHelper extends PredicateHelper {

  /**
   * Search a filtering predicate in a given logical plan
   */
  def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.exists {
      // We do not actually need a selective predicate if the `InMemoryRelation` is materialized,
      // because its statistics is correct enough. The call side would validate if the statistics
      // is bigger than requirements.
      case relation: InMemoryRelation if relation.isMaterialized => true
      case relation: InMemoryRelation =>
        relation.cachedPlan.exists {
          case f: FilterExec => isLikelySelective(f.condition)
          case _ => false
        }
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }
  }
}
