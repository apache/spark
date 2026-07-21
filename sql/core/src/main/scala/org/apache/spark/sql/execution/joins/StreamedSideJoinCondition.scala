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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, JoinType, LeftAnti, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.SparkPlan

/**
 * Helper for splitting a join condition into a streamed-side-only part and the remaining part,
 * for join types that preserve all streamed rows.
 *
 * The split is valid because for these join types, if a streamed-only predicate S is FALSE/NULL
 * for a streamed row, the full join condition S AND other(S, B) is FALSE/NULL for ANY buffered
 * row B. The streamed row outcome is therefore already determined before any probe: it is
 * emitted for outer/existence joins, and emitted for left anti joins when no match exists.
 */
private[joins] object StreamedSideJoinCondition extends PredicateHelper {

  /**
   * Splits `condition` into conjuncts that reference only the streamed side and the remaining
   * conjuncts, when `splitEnabled` is true and the join type preserves all streamed rows.
   * The streamed-only part can be evaluated once per streamed row before probing/walking the
   * buffered matches. Returns `(None, condition)` unchanged otherwise.
   *
   * Only deterministic, non-throwable conjuncts are hoisted: the hoisted part runs for every
   * streamed row, including rows that have no buffered match and would never evaluate the
   * conjunct otherwise, so hoisting a throwable conjunct could turn a valid outer/anti
   * result into an exception, and hoisting a non-deterministic conjunct would change how
   * many times it is evaluated per streamed row. This mirrors the guard used for the
   * analogous relocation in the optimizer rule PushPredicateThroughJoin.
   */
  def split(
      condition: Option[Expression],
      joinType: JoinType,
      streamedPlan: SparkPlan,
      splitEnabled: Boolean): (Option[Expression], Option[Expression]) = {
    val supported = joinType match {
      case LeftAnti | LeftOuter | RightOuter | _: ExistenceJoin => true
      case _ => false
    }
    if (condition.isDefined && splitEnabled && supported) {
      val conjuncts = splitConjunctivePredicates(condition.get)
      val (streamedOnly, rest) = conjuncts.partition(p =>
        p.deterministic && !p.throwable && p.references.subsetOf(streamedPlan.outputSet))
      (streamedOnly.reduceOption(And), rest.reduceOption(And))
    } else {
      (None, condition)
    }
  }
}
