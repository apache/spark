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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AND, FILTER, WITH_EXPRESSION}

/**
 * Before rewrite with expression, merge with expression which has same common expression for
 * avoid extra expression duplication.
 */
object MergeWithExpression extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithSubqueriesAndPruning(_.containsPattern(FILTER)) {
      case f @ Filter(cond, _) =>
        val newCond = cond.transformUpWithPruning(_.containsAllPatterns(AND, WITH_EXPRESSION)) {
          case And(left @ With(_, _), right @ With(_, _)) =>
            mergeWith(left, right)
          case And(left @ With(_, _), right) =>
            With(And(left.child, right), left.defs)
          case And(left, right @ With(_, _)) =>
            With(And(left, right.child), right.defs)
        }
        f.copy(condition = newCond)
    }
  }

  private def mergeWith(left: With, right: With): Expression = {
    val newDefs = left.defs.toBuffer
    val replaceMap = mutable.HashMap.empty[CommonExpressionId, CommonExpressionRef]
    right.defs.foreach {rDef =>
      val index = left.defs.indexWhere(lDef => rDef.child.fastEquals(lDef.child))
      if (index == -1) {
        newDefs.append(rDef)
      } else {
        replaceMap.put(rDef.id, new CommonExpressionRef(left.defs(index)))
      }
    }
    val newChild = if (replaceMap.nonEmpty) {
      val newRightChild = right.child.transform {
        case r: CommonExpressionRef if replaceMap.contains(r.id) =>
          replaceMap(r.id)
      }
      And(left.child, newRightChild)
    } else {
      And(left.child, right.child)
    }
    With(newChild, newDefs.toSeq)
  }
}
