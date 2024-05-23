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

import org.apache.spark.sql.catalyst.expressions.{And, EqualNullSafe, EqualTo, IsNull, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{JOIN, OR}

/**
 * Replaces `t1.id is null and t2.id is null or t1.id = t2.id` to `t1.id <=> t2.id`
 * in join condition for better performance.
 */
object OptimizeJoinCondition extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(JOIN), ruleId) {
    case j @ Join(_, _, _, condition, _) if condition.nonEmpty =>
      val newCondition = condition.map(_.transformWithPruning(_.containsPattern(OR), ruleId) {
        case Or(EqualTo(l, r), And(IsNull(c1), IsNull(c2)))
          if (l.semanticEquals(c1) && r.semanticEquals(c2))
            || (l.semanticEquals(c2) && r.semanticEquals(c1)) =>
          EqualNullSafe(l, r)
        case Or(And(IsNull(c1), IsNull(c2)), EqualTo(l, r))
          if (l.semanticEquals(c1) && r.semanticEquals(c2))
            || (l.semanticEquals(c2) && r.semanticEquals(c1)) =>
          EqualNullSafe(l, r)
      })
      j.copy(condition = newCondition)
  }
}
