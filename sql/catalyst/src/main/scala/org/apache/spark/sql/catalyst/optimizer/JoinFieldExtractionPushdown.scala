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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}

/**
 * Pushes down aliases to [[expressions.GetStructField]] expressions in a projection over a join
 * and its join condition. The original [[expressions.GetStructField]] expressions are replaced
 * with references to the pushed down aliases.
 */
object JoinFieldExtractionPushdown extends FieldExtractionPushdown {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      case op @ PhysicalOperation(projects, Seq(),
          join @ Join(left, right, joinType, Some(joinCondition))) =>
        val fieldExtractors = (projects :+ joinCondition).flatMap(getFieldExtractors).distinct

        if (fieldExtractors.nonEmpty) {
          val (aliases, substituteAttributes) = constructAliasesAndSubstitutions(fieldExtractors)

          // Construct the new projections and join condition by substituting each GetStructField
          // expression with a reference to its alias
          val newProjects =
            projects.map(substituteAttributes).collect { case named: NamedExpression => named }
          val newJoinCondition = substituteAttributes(joinCondition)

          // Prune left and right output attributes according to whether they're needed by the
          // new projections or join conditions
          val aliasAttributes = AttributeSet(aliases.map(_.toAttribute))
          val neededAttributes = AttributeSet((newProjects :+ newJoinCondition)
            .flatMap(_.collect { case att: Attribute => att })) -- aliasAttributes
          val leftAtts = left.output.filter(neededAttributes.contains)
          val rightAtts = right.output.filter(neededAttributes.contains)

          // Construct the left and right side aliases by partitioning the aliases according to
          // whether they reference attributes in the left side or the right side
          val (leftAliases, rightAliases) =
            aliases.partition(_.references.intersect(left.outputSet).nonEmpty)

          val newLeft = Project(leftAtts.toSeq ++ leftAliases, left)
          val newRight = Project(rightAtts.toSeq ++ rightAliases, right)

          Project(newProjects, Join(newLeft, newRight, joinType, Some(newJoinCondition)))
        } else {
          op
        }
    }
}
