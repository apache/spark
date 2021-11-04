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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Alias, And, EqualTo, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

/**
 * This rule ensures that [[Join]] keys doesn't contain complex expressions in the
 * optimization phase.
 *
 * Complex expressions are pulled out to a [[Project]] node under [[Join]] and are
 * referenced in join condition.
 *
 * {{{
 *   SELECT * FROM t1 JOIN t2 ON t1.a + 10 = t2.x ==>
 *   Project [a#0, b#1, x#2, y#3]
 *   +- Join Inner, ((spark_catalog.default.t1.a + 10)#8 = x#2)
 *      :- Project [a#0, b#1, (a#0 + 10) AS (spark_catalog.default.t1.a + 10)#8]
 *      :  +- Filter isnotnull((a#0 + 10))
 *      :     +- Relation default.t1[a#0,b#1] parquet
 *      +- Filter isnotnull(x#2)
 *         +- Relation default.t2[x#2,y#3] parquet
 * }}}
 */
object PullOutJoinCondition extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsPattern(JOIN)) {
    case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, otherPredicates, _, left, right, _)
        if j.resolved =>
      val complexLeftJoinKeys = new ArrayBuffer[NamedExpression]()
      val complexRightJoinKeys = new ArrayBuffer[NamedExpression]()

      val newLeftJoinKeys = leftKeys.map { expr =>
        if (!expr.foldable && expr.children.nonEmpty) {
          val ne = Alias(expr, expr.sql)()
          complexLeftJoinKeys += ne
          ne.toAttribute
        } else {
          expr
        }
      }

      val newRightJoinKeys = rightKeys.map { expr =>
        if (!expr.foldable && expr.children.nonEmpty) {
          val ne = Alias(expr, expr.sql)()
          complexRightJoinKeys += ne
          ne.toAttribute
        } else {
          expr
        }
      }

      if (complexLeftJoinKeys.nonEmpty || complexRightJoinKeys.nonEmpty) {
        val newLeft = Project(left.output ++ complexLeftJoinKeys, left)
        val newRight = Project(right.output ++ complexRightJoinKeys, right)
        val newCond = (newLeftJoinKeys.zip(newRightJoinKeys)
          .map { case (l, r) => EqualTo(l, r) } ++ otherPredicates)
          .reduceLeftOption(And)
        Project(j.output, j.copy(left = newLeft, right = newRight, condition = newCond))
      } else {
        j
      }
  }
}
