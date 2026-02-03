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

import java.util.LinkedHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{AnalysisErrorAt, NameParameterizedQuery}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  CreateArray,
  CreateMap,
  CreateNamedStruct,
  Expression,
  Literal,
  MapFromArrays,
  MapFromEntries,
  VariableReference
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SupervisingCommand}

/**
 * Resolver class that resolves [[NameParameterizedQuery]] operators.
 */
class NameParameterizedQueryResolver(
    operatorResolver: Resolver,
    expressionResolver: ExpressionResolver) {
  private val operatorResolutionContextStack = operatorResolver.getOperatorResolutionContextStack
  private val scopes = operatorResolver.getNameScopes

  /**
   * Resolves a [[NameParameterizedQuery]] operator. It handles two cases:
   * 1. If the child of the [[NameParameterizedQuery]] is a [[SupervisingCommand]], it moves the
   *    [[NameParameterizedQuery]] below the [[SupervisingCommand]]. It is resolved later when
   *    `SupervisingCommand.run` is called.
   * 2. Otherwise, it resolves the [[NameParameterizedQuery]] by resolving its parameter
   *    expressions and replacing the parameter names with their corresponding values in the
   *    [[ExpressionResolver]].
   */
  def resolve(nameParameterizedQuery: NameParameterizedQuery): LogicalPlan = {
    nameParameterizedQuery match {
      case nameParameterizedQuery @ NameParameterizedQuery(command: SupervisingCommand, _, _) =>
        pushParameterizedQueryBelowCommand(nameParameterizedQuery, command)
      case nameParameterizedQuery =>
        resolveNameParameterizedQuery(nameParameterizedQuery)
    }
  }

  /**
   * Moves the [[NameParameterizedQuery]] below the given [[SupervisingCommand]].
   * This is done to ensure that the parameters are resolved in the context of the actual plan,
   * whereas the [[SupervisingCommand]] is expected to be a top-level node.
   * [[NameParameterizedQuery]] should be pushed down through any nested [[SupervisingCommand]]s.
   * Examples:
   *
   * 1. Parameters below EXPLAIN command:
   * {{{
   *  EXPLAIN SELECT :first;
   *
   *  -- Analyzed plan
   *  -- ExplainCommand 'NameParameterizedQuery [first], [1], SimpleMode
   * }}}
   *
   * 2. Parameters below DESCRIBE command:
   * {{{
   *  DESCRIBE QUERY SELECT :first;
   *
   *  -- Analyzed plan
   *  -- DescribeQueryCommand 'NameParameterizedQuery [first], [1]
   * }}}
   *
   * 3. Parameters below nested commands:
   * {{{
   *  EXPLAIN EXPLAIN SELECT :first;
   *
   *  -- Analyzed plan
   *  -- ExplainCommand ExplainCommand 'NameParameterizedQuery [a], [1], SimpleMode, SimpleMode
   * }}}
   *
   */
  private def pushParameterizedQueryBelowCommand(
      nameParameterizedQuery: NameParameterizedQuery,
      command: SupervisingCommand): LogicalPlan = {
    command.withTransformedSupervisedPlan {
      case nestedCommand: SupervisingCommand =>
        pushParameterizedQueryBelowCommand(nameParameterizedQuery, nestedCommand)
      case supervisedPlan =>
        nameParameterizedQuery.copy(child = supervisedPlan)
    }
  }

  /**
   * Resolves a [[NameParameterizedQuery]] operator. It's done in the following steps:
   *
   * 1. Resolve the parameter expressions using the
   *    [[ExpressionResolver.resolveExpressionTreeInOperator]]. Resolution is done in a new scope
   *    to avoid polluting the current scope with references from the parameter expressions.
   * 2. Validate that the number of parameter names matches the number of parameter values.
   * 3. Create a mapping of parameter names to their corresponding resolved values and set it in
   *    [[OperatorResolutionContext]] in order to use it for [[NamedParameter]] resolution later.
   * 4. Check that all parameter values are of allowed types.
   * 5. Resolve the child operator of the [[NameParameterizedQuery]].
   */
  private def resolveNameParameterizedQuery(
      nameParameterizedQuery: NameParameterizedQuery): LogicalPlan = {
    val parameterNames = nameParameterizedQuery.argNames
    val parameterValues = nameParameterizedQuery.argValues

    scopes.pushScope()
    val resolvedParameterValues = try {
      parameterValues.map { parameterValue =>
        expressionResolver.resolveExpressionTreeInOperator(
          unresolvedExpression = parameterValue,
          parentOperator = nameParameterizedQuery
        )
      }
    } finally {
      scopes.popScope()
      scopes.current.clearAvailableAliases()
    }

    validateParameterNamesAndValuesSizes(parameterNames, parameterValues)

    operatorResolutionContextStack.current.parameterNamesToValues = Some(
      new LinkedHashMap[String, Expression]
    )

    parameterNames.zip(resolvedParameterValues).foreach {
      case (name, value) if isNotAllowed(value) =>
        value.failAnalysis(
          errorClass = "INVALID_SQL_ARG",
          messageParameters = Map("name" -> name)
        )
      case (name, value) =>
        operatorResolutionContextStack.current.parameterNamesToValues.get.put(name, value)
    }

    operatorResolver.resolve(nameParameterizedQuery.child)
  }

  private def validateParameterNamesAndValuesSizes(
      parameterNames: Seq[String],
      parameterValues: Seq[Expression]): Unit = {
    if (parameterNames.length != parameterValues.length) {
      throw SparkException.internalError(
        s"The number of argument names ${parameterNames.length} " +
        s"must be equal to the number of argument values ${parameterValues.length}."
      )
    }
  }

  private def isNotAllowed(expression: Expression): Boolean = expression.exists {
    case _: Literal | _: CreateArray | _: CreateNamedStruct | _: CreateMap | _: MapFromArrays |
        _: MapFromEntries | _: VariableReference | _: Alias =>
      false
    case _ => true
  }
}
