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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}

/**
 * Pushes down aliases to [[expressions.GetStructField]] expressions in an aggregate's grouping and
 * aggregate expressions into a projection over its children. The original
 * [[expressions.GetStructField]] expressions are replaced with references to the pushed down
 * aliases.
 */
object AggregateFieldExtractionPushdown extends FieldExtractionPushdown {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      case agg @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val expressions = groupingExpressions ++ aggregateExpressions
        val attributes = AttributeSet(expressions.collect { case att: Attribute => att })
        val childAttributes = AttributeSet(child.expressions)
        val fieldExtractors0 =
          expressions
            .flatMap(getFieldExtractors)
            .distinct
        val fieldExtractors1 =
          fieldExtractors0
            .filter(_.collectFirst { case att: Attribute => att }
              .filter(attributes.contains).isEmpty)
        val fieldExtractors =
          fieldExtractors1
            .filter(_.collectFirst { case att: Attribute => att }
              .filter(childAttributes.contains).nonEmpty)

        if (fieldExtractors.nonEmpty) {
          val (aliases, substituteAttributes) = constructAliasesAndSubstitutions(fieldExtractors)

          // Construct the new grouping and aggregate expressions by substituting
          // each GetStructField expression with a reference to its alias
          val newAggregateExpressions =
            aggregateExpressions.map(substituteAttributes)
              .collect { case named: NamedExpression => named }
          val newGroupingExpressions = groupingExpressions.map(substituteAttributes)

          // We need to push down the aliases we've created. We do this with a new projection over
          // this aggregate's child consisting of the aliases and original child's output sans
          // attributes referenced by the aliases

          // None of these attributes are required by this aggregate because we filtered out the
          // GetStructField instances which referred to attributes that were required
          val unnecessaryAttributes = aliases.map(_.child.references).reduce(_ ++ _)
          // The output we require from this aggregate is the child's output minus the unnecessary
          // attributes
          val requiredChildOutput = child.output.filterNot(unnecessaryAttributes.contains)
          val projects = requiredChildOutput ++ aliases
          val newProject = Project(projects, child)

          Aggregate(newGroupingExpressions, newAggregateExpressions, newProject)
        } else {
          agg
        }
    }
}
