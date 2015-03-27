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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class PreAnalyzer(caseSensitive: Boolean = true,
                  maxIterations: Int = 100) extends RuleExecutor[LogicalPlan] {

  val resolver = if (caseSensitive) caseSensitiveResolution else caseInsensitiveResolution

  val fixedPoint = FixedPoint(maxIterations)

  lazy val batches: Seq[Batch] = Seq(
    Batch("Resolution", fixedPoint, ResolveConflictingAttributes)
  )

  /**
   * Handling the cases in which the attributes of nodes are conflicting
   */
  object ResolveConflictingAttributes extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case p: LogicalPlan if !p.childrenResolved => p

      // Special handling for cases when self-join introduce duplicate expression ids
      case j @ Join(left, right, _, _) if left.outputSet.intersect(right.outputSet).nonEmpty =>
        val conflictingAttributes = left.outputSet.intersect(right.outputSet)
        logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} in $j")

        val (oldRelation, newRelation) = right.collect {
          // Handle base relations that might appear more than once.
          case oldVersion: MultiInstanceRelation
              if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
            val newVersion = oldVersion.newInstance()
            (oldVersion, newVersion)

          // Handle projects that create conflicting aliases.
          case oldVersion @ Project(projectList, _)
              if findAliases(projectList).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(projectList = newAliases(projectList)))

          case oldVersion @ Aggregate(_, aggregateExpressions, _)
              if findAliases(aggregateExpressions).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(aggregateExpressions = newAliases(aggregateExpressions)))
        }.head // Only handle first case found, others will be fixed on the next pass.

        val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
        val newRight = right transformUp {
          case r if r == oldRelation => newRelation
        } transformUp {
          case other => other transformExpressions {
            case a: Attribute => attributeRewrites.get(a).getOrElse(a)
          }
        }
        j.copy(right = newRight)
    }

    def newAliases(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
      expressions.map {
        case a: Alias => Alias(a.child, a.name)()
        case other => other
      }
    }

    def findAliases(projectList: Seq[NamedExpression]): AttributeSet = {
      AttributeSet(projectList.collect { case a: Alias => a.toAttribute })
    }
  }
}

