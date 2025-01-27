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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, ResolvedInlineTable}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{
  Filter,
  GlobalLimit,
  LocalLimit,
  LocalRelation,
  LogicalPlan,
  OneRowRelation,
  Project,
  SubqueryAlias
}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.BooleanType

/**
 * The [[ResolutionValidator]] performs the validation work after the logical plan tree is
 * resolved by the [[Resolver]]. Each `resolve*` method in the [[Resolver]] must
 * have its `validate*` counterpart in the [[ResolutionValidator]]. The validation code asserts the
 * conditions that must never be false no matter which SQL query or DataFrame program was provided.
 * The validation approach is single-pass, post-order, complementary to the resolution process.
 */
class ResolutionValidator {
  private val expressionResolutionValidator = new ExpressionResolutionValidator(this)

  private[resolver] var attributeScopeStack = new AttributeScopeStack

  /**
   * Validate the resolved logical `plan` - assert invariants that should never be false no
   * matter which SQL query or DataFrame program was provided. New operators must be added here as
   * soon as [[Resolver]] supports them. We check this by throwing an exception for
   * unknown operators.
   */
  def validatePlan(plan: LogicalPlan): Unit = wrapErrors(plan) {
    validate(plan)
  }

  private def validate(operator: LogicalPlan): Unit = {
    operator match {
      case project: Project =>
        validateProject(project)
      case filter: Filter =>
        validateFilter(filter)
      case subqueryAlias: SubqueryAlias =>
        validateSubqueryAlias(subqueryAlias)
      case globalLimit: GlobalLimit =>
        validateGlobalLimit(globalLimit)
      case localLimit: LocalLimit =>
        validateLocalLimit(localLimit)
      case inlineTable: ResolvedInlineTable =>
        validateInlineTable(inlineTable)
      case localRelation: LocalRelation =>
        validateRelation(localRelation)
      case oneRowRelation: OneRowRelation =>
        validateRelation(oneRowRelation)
      // [[LogicalRelation]], [[HiveTableRelation]] and other specific relations can't be imported
      // because of a potential circular dependency, so we match a generic Catalyst
      // [[MultiInstanceRelation]] instead.
      case multiInstanceRelation: MultiInstanceRelation =>
        validateRelation(multiInstanceRelation)
    }
  }

  private def validateProject(project: Project): Unit = {
    attributeScopeStack.withNewScope {
      validate(project.child)
      expressionResolutionValidator.validateProjectList(project.projectList)
    }

    handleOperatorOutput(project)
  }

  private def validateFilter(filter: Filter): Unit = {
    validate(filter.child)

    assert(
      filter.condition.dataType == BooleanType,
      s"Output type of a filter must be a boolean, but got: ${filter.condition.dataType.typeName}"
    )
    expressionResolutionValidator.validate(filter.condition)
  }

  private def validateSubqueryAlias(subqueryAlias: SubqueryAlias): Unit = {
    validate(subqueryAlias.child)

    handleOperatorOutput(subqueryAlias)
  }

  private def validateGlobalLimit(globalLimit: GlobalLimit): Unit = {
    validate(globalLimit.child)
    expressionResolutionValidator.validate(globalLimit.limitExpr)
  }

  private def validateLocalLimit(localLimit: LocalLimit): Unit = {
    validate(localLimit.child)
    expressionResolutionValidator.validate(localLimit.limitExpr)
  }

  private def validateInlineTable(inlineTable: ResolvedInlineTable): Unit = {
    inlineTable.rows.foreach(row => {
      row.foreach(expression => {
        expressionResolutionValidator.validate(expression)
      })
    })

    handleOperatorOutput(inlineTable)
  }

  private def validateRelation(relation: LogicalPlan): Unit = {
    handleOperatorOutput(relation)
  }

  private def handleOperatorOutput(operator: LogicalPlan): Unit = {
    attributeScopeStack.overwriteTop(operator.output)

    operator.output.foreach(attribute => {
      assert(
        attribute.isInstanceOf[AttributeReference],
        s"Output of an operator must be a reference to an attribute, but got: " +
        s"${attribute.getClass.getSimpleName}"
      )
      expressionResolutionValidator.validate(attribute)
    })
  }

  private def wrapErrors[R](plan: LogicalPlan)(body: => R): Unit = {
    try {
      body
    } catch {
      case ex: Throwable =>
        throw QueryCompilationErrors.resolutionValidationError(ex, plan)
    }
  }
}
