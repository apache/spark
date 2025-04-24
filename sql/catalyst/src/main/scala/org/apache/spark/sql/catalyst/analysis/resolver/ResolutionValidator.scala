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

import java.util.HashSet

import org.apache.spark.sql.catalyst.analysis.{
  GetViewColumnByNameAndOrdinal,
  MultiInstanceRelation,
  ResolvedInlineTable,
  SchemaBinding
}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.AUTO_GENERATED_ALIAS
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{BooleanType, MetadataBuilder, StructType}

/**
 * The [[ResolutionValidator]] performs the validation work after the logical plan tree is
 * resolved by the [[Resolver]]. Each `resolve*` method in the [[Resolver]] must
 * have its `validate*` counterpart in the [[ResolutionValidator]]. The validation code asserts the
 * conditions that must never be false no matter which SQL query or DataFrame program was provided.
 * The validation approach is single-pass, post-order, complementary to the resolution process.
 */
class ResolutionValidator {
  private val attributeScopeStack = new AttributeScopeStack

  private val expressionResolutionValidator = new ExpressionResolutionValidator(this)

  def getAttributeScopeStack: AttributeScopeStack = attributeScopeStack

  /**
   * Validate the resolved logical `plan` - assert invariants that should never be false no
   * matter which SQL query or DataFrame program was provided. New operators must be added here as
   * soon as [[Resolver]] supports them. We check this by throwing an exception for
   * unknown operators.
   */
  def validatePlan(plan: LogicalPlan): Unit = wrapErrors(plan) {
    validate(plan)
  }

  /**
   * Validate a specific `operator`. This is an internal entry point for the recursive validation.
   * Also, [[ExpressionResolutionValidator]] calls it to validate [[SubqueryExpression]] plans.
   */
  def validate(operator: LogicalPlan): Unit = {
    operator match {
      case withCte: WithCTE =>
        validateWith(withCte)
      case cteRelationDef: CTERelationDef =>
        validateCteRelationDef(cteRelationDef)
      case cteRelationRef: CTERelationRef =>
        validateCteRelationRef(cteRelationRef)
      case aggregate: Aggregate =>
        validateAggregate(aggregate)
      case project: Project =>
        validateProject(project)
      case filter: Filter =>
        validateFilter(filter)
      case subqueryAlias: SubqueryAlias =>
        validateSubqueryAlias(subqueryAlias)
      case view: View =>
        validateView(view)
      case globalLimit: GlobalLimit =>
        validateGlobalLimit(globalLimit)
      case localLimit: LocalLimit =>
        validateLocalLimit(localLimit)
      case offset: Offset =>
        validateOffset(offset)
      case tail: Tail =>
        validateTail(tail)
      case distinct: Distinct =>
        validateDistinct(distinct)
      case inlineTable: ResolvedInlineTable =>
        validateInlineTable(inlineTable)
      case localRelation: LocalRelation =>
        validateRelation(localRelation)
      case oneRowRelation: OneRowRelation =>
        validateRelation(oneRowRelation)
      case range: Range =>
        validateRelation(range)
      case setOperationLike @ (_: Union | _: SetOperation) =>
        validateSetOperationLike(setOperationLike)
      case sort: Sort =>
        validateSort(sort)
      case join: Join =>
        validateJoin(join)
      case repartition: Repartition =>
        validateRepartition(repartition)
      // [[LogicalRelation]], [[HiveTableRelation]] and other specific relations can't be imported
      // because of a potential circular dependency, so we match a generic Catalyst
      // [[MultiInstanceRelation]] instead.
      case multiInstanceRelation: MultiInstanceRelation =>
        validateRelation(multiInstanceRelation)
    }

    operator match {
      case withCte: WithCTE =>
      case _ =>
        ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(
          operator.children.map(_.output)
        )
    }
  }

  private def validateWith(withCte: WithCTE): Unit = {
    val knownCteDefIds = new HashSet[Long](withCte.cteDefs.length)

    for (cteDef <- withCte.cteDefs) {
      assert(
        !knownCteDefIds.contains(cteDef.id),
        s"Duplicate CTE definition id: ${cteDef.id}"
      )

      validate(cteDef)

      knownCteDefIds.add(cteDef.id)
    }

    validate(withCte.plan)
  }

  private def validateCteRelationDef(cteRelationDef: CTERelationDef): Unit = {
    validate(cteRelationDef.child)
  }

  private def validateCteRelationRef(cteRelationRef: CTERelationRef): Unit = {
    handleOperatorOutput(cteRelationRef)
  }

  private def validateAggregate(aggregate: Aggregate): Unit = {
    attributeScopeStack.withNewScope() {
      validate(aggregate.child)
      expressionResolutionValidator.validateProjectList(aggregate.aggregateExpressions)
      aggregate.groupingExpressions.foreach(expressionResolutionValidator.validate)
    }

    handleOperatorOutput(aggregate)
  }

  private def validateProject(project: Project): Unit = {
    attributeScopeStack.withNewScope() {
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

  private def validateView(view: View): Unit = {
    validate(view.child)

    if (view.desc.viewSchemaMode == SchemaBinding) {
      assert(
        schemaWithExplicitMetadata(view.schema) == schemaWithExplicitMetadata(view.desc.schema),
        "View output schema does not match the view description schema. " +
        s"View schema: ${view.schema}, description schema: ${view.desc.schema}"
      )
    }
    view.child match {
      case project: Project =>
        assert(
          !project.projectList
            .exists(expression => expression.isInstanceOf[GetViewColumnByNameAndOrdinal]),
          "Resolved Project operator under a view cannot contain GetViewColumnByNameAndOrdinal"
        )
      case _ =>
    }

    handleOperatorOutput(view)
  }

  private def validateGlobalLimit(globalLimit: GlobalLimit): Unit = {
    validate(globalLimit.child)
    expressionResolutionValidator.validate(globalLimit.limitExpr)
  }

  private def validateLocalLimit(localLimit: LocalLimit): Unit = {
    validate(localLimit.child)
    expressionResolutionValidator.validate(localLimit.limitExpr)
  }

  private def validateOffset(offset: Offset): Unit = {
    validate(offset.child)
    expressionResolutionValidator.validate(offset.offsetExpr)
  }

  private def validateTail(tail: Tail): Unit = {
    validate(tail.child)
    expressionResolutionValidator.validate(tail.limitExpr)
  }

  private def validateDistinct(distinct: Distinct): Unit = {
    validate(distinct.child)
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

  private def validateSetOperationLike(plan: LogicalPlan): Unit = {
    plan.children.foreach(validate)

    assert(
      plan.children.length > 1,
      s"${plan.nodeName} operator has to have at least 2 children"
    )
    val firstChildOutput = plan.children.head.output
    for (child <- plan.children.tail) {
      val childOutput = child.output
      assert(
        childOutput.length == firstChildOutput.length,
        s"Unexpected output length for ${plan.nodeName} child $child"
      )
    }

    handleOperatorOutput(plan)
  }

  private def validateSort(sort: Sort): Unit = {
    validate(sort.child)
    for (sortOrder <- sort.order) {
      expressionResolutionValidator.validate(sortOrder.child)
    }
  }

  private def validateRepartition(repartition: Repartition): Unit = {
    validate(repartition.child)
  }

  private def validateJoin(join: Join) = {
    attributeScopeStack.withNewScope() {
      attributeScopeStack.withNewScope() {
        validate(join.left)
        validate(join.right)
        assert(join.left.outputSet.intersect(join.right.outputSet).isEmpty)
      }

      attributeScopeStack.overwriteCurrent(join.left.output ++ join.right.output)

      join.condition match {
        case Some(condition) => expressionResolutionValidator.validate(condition)
        case None =>
      }
    }

    handleOperatorOutput(join)
  }

  private def handleOperatorOutput(operator: LogicalPlan): Unit = {
    attributeScopeStack.overwriteCurrent(operator.output)

    operator.output.foreach(attribute => {
      assert(
        attribute.isInstanceOf[AttributeReference],
        s"Output of an operator must be a reference to an attribute, but got: " +
        s"${attribute.getClass.getSimpleName}"
      )
      expressionResolutionValidator.validate(attribute)
    })
  }

  private def schemaWithExplicitMetadata(schema: StructType): StructType = {
    StructType(schema.map { structField =>
      val metadataBuilder = new MetadataBuilder().withMetadata(structField.metadata)
      metadataBuilder.remove(AUTO_GENERATED_ALIAS)
      structField.copy(
        metadata = metadataBuilder.build()
      )
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
