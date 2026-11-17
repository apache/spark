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
  ResolvedInlineTable
}
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  PipeOperator,
  VariableReference
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.BooleanType

/**
 * The [[ResolutionValidator]] performs the validation work after the logical plan tree is
 * resolved by the [[Resolver]]. Each `resolve*` method in the [[Resolver]] must
 * have its `validate*` counterpart in the [[ResolutionValidator]]. The validation code asserts the
 * conditions that must never be false no matter which SQL query or DataFrame program was provided.
 * The validation approach is single-pass, post-order, complementary to the resolution process.
 */
class ResolutionValidator(
    validatorExtensions: Seq[ResolutionValidatorExtension] = Seq.empty) {
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
      case window: Window =>
        validateWindow(window)
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
      case unionLoop: UnionLoop =>
        validateUnionLoop(unionLoop)
      case sort: Sort =>
        validateSort(sort)
      case join: Join =>
        validateJoin(join)
      case asOfJoin: AsOfJoin =>
        validateAsOfJoin(asOfJoin)
      case repartition: Repartition =>
        validateRepartition(repartition)
      case repartitionByExpression: RepartitionByExpression =>
        validateRepartitionByExpression(repartitionByExpression)
      case rebalancePartitions: RebalancePartitions =>
        validateRebalancePartitions(rebalancePartitions)
      case sample: Sample =>
        validateSample(sample)
      case generate: Generate =>
        validateGenerate(generate)
      case expand: Expand =>
        validateExpand(expand)
      // [[LogicalRelation]], [[HiveTableRelation]] and other specific relations can't be imported
      // because of a potential circular dependency, so we match a generic Catalyst
      // [[MultiInstanceRelation]] instead.
      case multiInstanceRelation: MultiInstanceRelation =>
        validateRelation(multiInstanceRelation)
      case supervisingCommand: SupervisingCommand =>
        validateSupervisingCommand(supervisingCommand)
      case lateralJoin: LateralJoin =>
        validateLateralJoin(lateralJoin)
      case pipe: PipeOperator =>
        validatePipeOperator(pipe)
      case binBy: BinBy =>
        validateBinBy(binBy)
      case setVariable: SetVariable =>
        validateSetVariable(setVariable)
      case resolvedHint: ResolvedHint =>
        validateResolvedHint(resolvedHint)
      case operator =>
        tryDelegateToValidatorExtension(operator)
    }

    operator match {
      case _: WithCTE =>
      case _: LateralJoin =>
      case _ =>
        ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(
          operator.children.map(_.output)
        )
    }
  }

  private def tryDelegateToValidatorExtension(operator: LogicalPlan): Unit = {
    validatorExtensions.foreach(_.validateOperator(operator, this))
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

  private def validateWindow(window: Window): Unit = {
    attributeScopeStack.pushScope()
    try {
      validate(window.child)
      window.partitionSpec.foreach(expressionResolutionValidator.validate)
      window.orderSpec.foreach(expressionResolutionValidator.validate)
      expressionResolutionValidator.validateProjectList(window.windowExpressions)
    } finally {
      attributeScopeStack.popScope()
    }

    handleOperatorOutput(window)
  }

  private def validateCteRelationDef(cteRelationDef: CTERelationDef): Unit = {
    validate(cteRelationDef.child)
  }

  private def validateCteRelationRef(cteRelationRef: CTERelationRef): Unit = {
    handleOperatorOutput(cteRelationRef)
  }

  private def validateAggregate(aggregate: Aggregate): Unit = {
    attributeScopeStack.pushScope()
    try {
      validate(aggregate.child)
      expressionResolutionValidator.validateProjectList(aggregate.aggregateExpressions)
      aggregate.groupingExpressions.foreach(expressionResolutionValidator.validate)
    } finally {
      attributeScopeStack.popScope()
    }

    handleOperatorOutput(aggregate)
  }

  private def validateProject(project: Project): Unit = {
    attributeScopeStack.pushScope()
    try {
      validate(project.child)
      expressionResolutionValidator.validateProjectList(project.projectList)
    } finally {
      attributeScopeStack.popScope()
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

  private def validateUnionLoop(plan: UnionLoop): Unit = {
    plan.children.foreach(validate)

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

  private def validateRepartitionByExpression(
      repartitionByExpression: RepartitionByExpression): Unit = {
    validate(repartitionByExpression.child)
    repartitionByExpression.partitionExpressions.foreach(
      expression => expressionResolutionValidator.validate(expression)
    )
  }

  private def validateRebalancePartitions(
      rebalancePartitions: RebalancePartitions): Unit = {
    validate(rebalancePartitions.child)
    rebalancePartitions.partitionExpressions.foreach(
      expression => expressionResolutionValidator.validate(expression)
    )
  }

  private def validateSample(sample: Sample): Unit = {
    validate(sample.child)
  }

  private def validateBinBy(binBy: BinBy): Unit = {
    validate(binBy.child)

    expressionResolutionValidator.validate(binBy.rangeStart)
    expressionResolutionValidator.validate(binBy.rangeEnd)
    binBy.distributeColumns.foreach(expressionResolutionValidator.validate)

    handleOperatorOutput(binBy)
  }

  private def validateSetVariable(setVariable: SetVariable): Unit = {
    validate(setVariable.sourceQuery)
    setVariable.targetVariables.foreach {
      case variableReference: VariableReference =>
        expressionResolutionValidator.validate(variableReference)
      case other =>
        assert(assertion = false, s"Unexpected SET VARIABLE target: $other")
    }
    handleOperatorOutput(setVariable)
  }

  private def validateResolvedHint(resolvedHint: ResolvedHint): Unit = {
    validate(resolvedHint.child)
  }

  private def validatePipeOperator(pipe: PipeOperator): Unit = {
    validate(pipe.child)
  }

  private def validateLateralJoin(lateralJoin: LateralJoin): Unit = {
    attributeScopeStack.pushScope()
    try {
      validate(lateralJoin.left)

      attributeScopeStack.pushScope(isSubqueryRoot = true)
      try {
        validate(lateralJoin.right.plan)
      } finally {
        attributeScopeStack.popScope()
      }

      attributeScopeStack.overwriteCurrent(
        lateralJoin.left.output ++ lateralJoin.right.plan.output
      )

      lateralJoin.condition match {
        case Some(condition) => expressionResolutionValidator.validate(condition)
        case None =>
      }
    } finally {
      attributeScopeStack.popScope()
    }

    ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(
      Seq(lateralJoin.left.output, lateralJoin.right.plan.output)
    )

    handleOperatorOutput(lateralJoin)
  }

  private def validateGenerate(generate: Generate): Unit = {
    validate(generate.child)
    expressionResolutionValidator.validate(generate.generator)

    handleOperatorOutput(generate)
  }

  private def validateExpand(expand: Expand): Unit = {
    attributeScopeStack.pushScope()
    try {
      validate(expand.child)
      expand.projections.foreach(_.foreach(expressionResolutionValidator.validate))
    } finally {
      attributeScopeStack.popScope()
    }

    handleOperatorOutput(expand)
  }

  private def validateJoin(join: Join) = {
    attributeScopeStack.pushScope()
    try {
      attributeScopeStack.pushScope()
      try {
        validate(join.left)
        validate(join.right)
        assert(join.left.outputSet.intersect(join.right.outputSet).isEmpty)
      } finally {
        attributeScopeStack.popScope()
      }

      attributeScopeStack.overwriteCurrent(join.left.output ++ join.right.output)

      join.condition match {
        case Some(condition) => expressionResolutionValidator.validate(condition)
        case None =>
      }
    } finally {
      attributeScopeStack.popScope()
    }

    handleOperatorOutput(join)
  }

  private def validateAsOfJoin(asOfJoin: AsOfJoin): Unit = {
    // The inner scope keeps the per-child output overwrites done by `handleOperatorOutput` out
    // of the outer scope, which holds the combined join output the join expressions resolve
    // against.
    attributeScopeStack.pushScope()
    try {
      attributeScopeStack.pushScope()
      try {
        validate(asOfJoin.left)
        validate(asOfJoin.right)
        assert(asOfJoin.left.outputSet.intersect(asOfJoin.right.outputSet).isEmpty)
      } finally {
        attributeScopeStack.popScope()
      }

      attributeScopeStack.overwriteCurrent(asOfJoin.left.output ++ asOfJoin.right.output)

      expressionResolutionValidator.validate(asOfJoin.asOfCondition)
      asOfJoin.condition.foreach(expressionResolutionValidator.validate)
      asOfJoin.tolerance.foreach(expressionResolutionValidator.validate)
      asOfJoin.orderExpression.foreach(expressionResolutionValidator.validate)
      asOfJoin.leftSortExprs.foreach(expressionResolutionValidator.validate)
      asOfJoin.rightSortExprs.foreach(expressionResolutionValidator.validate)
    } finally {
      attributeScopeStack.popScope()
    }

    handleOperatorOutput(asOfJoin)
  }

  private def validateSupervisingCommand(supervisingCommand: SupervisingCommand): Unit = {}

  def handleOperatorOutput(operator: LogicalPlan): Unit = {
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

  private def wrapErrors[R](plan: LogicalPlan)(body: => R): Unit = {
    try {
      body
    } catch {
      case ex: Throwable =>
        throw QueryCompilationErrors.resolutionValidationError(ex, plan)
    }
  }
}
