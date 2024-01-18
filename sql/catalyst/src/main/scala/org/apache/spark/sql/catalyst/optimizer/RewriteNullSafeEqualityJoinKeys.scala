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

import org.apache.spark.sql.catalyst.expressions.{And, Coalesce, EqualNullSafe, EqualTo, IsNull, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

/**
 * Rewrite the null-safe equality join keys.
 *
 * Input query:
 * {{{
 *    SELECT * FROM l JOIN r on l.c <=> r.c
 * }}}
 *
 * Rewritten query:
 * {{{
 *    SELECT * FROM l JOIN r ON
 *      COALESCE(l.c, default) = COALESCE(r.c, default) AND ISNULL(l.c) = ISNULL(r.c)
 * }}}
 */
object RewriteNullSafeEqualityJoinKeys extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(_.containsPattern(JOIN)) {
      case join @ Join(left, right, _, Some(condition), _) =>
        val predicates = splitConjunctivePredicates(condition)
        val newPredicates = predicates.map {
          // Replace null with default value for joining key, then those rows with null in it could
          // be joined together
          case EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, right) =>
            // (coalesce(l, default) = coalesce(r, default)) and (isnull(l) = isnull(r))
            And(
              EqualTo(
                Coalesce(Seq(l, Literal.default(l.dataType))),
                  Coalesce(Seq(r, Literal.default(r.dataType)))),
              EqualTo(IsNull(l), IsNull(r))
            )
          // Same as above with left/right reversed.
          case EqualNullSafe(l, r) if canEvaluate(l, right) && canEvaluate(r, left) =>
            And(
              EqualTo(
                Coalesce(Seq(r, Literal.default(r.dataType))),
                Coalesce(Seq(l, Literal.default(l.dataType)))),
              EqualTo(IsNull(r), IsNull(l))
            )
          case other => other
        }
        if (predicates == newPredicates) {
          join
        } else {
          join.copy(condition = Some(newPredicates.reduce(And)))
        }
    }
  }
}
