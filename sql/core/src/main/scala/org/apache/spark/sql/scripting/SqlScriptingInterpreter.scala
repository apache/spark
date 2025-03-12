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

package org.apache.spark.sql.scripting

import scala.collection.mutable.HashMap

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, CompoundPlanStatement, ExceptionHandlerType, ForStatement, IfElseStatement, IterateStatement, LeaveStatement, LoopStatement, OneRowRelation, Project, RepeatStatement, SearchedCaseStatement, SimpleCaseStatement, SingleStatement, WhileStatement}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.errors.SqlScriptingErrors

/**
 * SQL scripting interpreter - builds SQL script execution plan.
 *
 * @param session
 *   Spark session that SQL script is executed within.
 */
case class SqlScriptingInterpreter(session: SparkSession) {

  /**
   * Build execution plan and return statements that need to be executed,
   *   wrapped in the execution node.
   *
   * @param compound
   *   CompoundBody for which to build the plan.
   * @param args
   *   A map of parameter names to SQL literal expressions.
   * @param context
   *   SqlScriptingExecutionContext keeps the execution state of current script.
   * @return
   *   Top level CompoundBodyExec representing SQL Script to be executed.
   */
  def buildExecutionPlan(
      compound: CompoundBody,
      args: Map[String, Expression],
      context: SqlScriptingExecutionContext): CompoundBodyExec = {
    transformTreeIntoExecutable(compound, args, context)
      .asInstanceOf[CompoundBodyExec]
  }

  /**
   * Transform [[CompoundBody]] into [[CompoundBodyExec]].
 *
   * @param compoundBody
   *   CompoundBody to be transformed into CompoundBodyExec.
   * @param args
   *   A map of parameter names to SQL literal expressions.
   * @param context
   *   SqlScriptingExecutionContext keeps the execution state of current script.
   * @return
   *   Executable version of the CompoundBody .
   */
  private def transformBodyIntoExec(
      compoundBody: CompoundBody,
      args: Map[String, Expression],
      context: SqlScriptingExecutionContext): CompoundBodyExec = {
    // Map of conditions to their respective handlers.
    val conditionToExceptionHandlerMap: HashMap[String, ExceptionHandlerExec] = HashMap.empty
    // Map of SqlStates to their respective handlers.
    val sqlStateToExceptionHandlerMap: HashMap[String, ExceptionHandlerExec] = HashMap.empty
    // NOT FOUND handler.
    var notFoundHandler: Option[ExceptionHandlerExec] = None
    // Get SQLEXCEPTION handler.
    var sqlExceptionHandler: Option[ExceptionHandlerExec] = None

    compoundBody.handlers.foreach(handler => {
      val handlerBodyExec =
        transformBodyIntoExec(
          handler.body,
          args,
          context)

      // Execution node of handler.
      val handlerScopeLabel = if (handler.handlerType == ExceptionHandlerType.EXIT) {
        Some(compoundBody.label.get)
      } else {
        None
      }

      val handlerExec = new ExceptionHandlerExec(
        handlerBodyExec,
        handler.handlerType,
        handlerScopeLabel)

      // For each condition handler is defined for, add corresponding key value pair
      // to the conditionHandlerMap.
      handler.exceptionHandlerTriggers.conditions.foreach(condition => {
        // Condition can either be the key in conditions map or SqlState.
        if (conditionToExceptionHandlerMap.contains(condition)) {
          throw SqlScriptingErrors.duplicateHandlerForSameCondition(CurrentOrigin.get, condition)
        } else {
          conditionToExceptionHandlerMap.put(condition, handlerExec)
        }
      })

      // For each sqlState handler is defined for, add corresponding key value pair
      // to the sqlStateHandlerMap.
      handler.exceptionHandlerTriggers.sqlStates.foreach(sqlState => {
        if (sqlStateToExceptionHandlerMap.contains(sqlState)) {
          throw SqlScriptingErrors.duplicateHandlerForSameSqlState(CurrentOrigin.get, sqlState)
        } else {
          sqlStateToExceptionHandlerMap.put(sqlState, handlerExec)
        }
      })

      // Get NOT FOUND handler.
      notFoundHandler = if (handler.exceptionHandlerTriggers.notFound) {
        Some(handlerExec)
      } else None

      // Get SQLEXCEPTION handler.
      sqlExceptionHandler = if (handler.exceptionHandlerTriggers.sqlException) {
        Some(handlerExec)
      } else None
    })

    // Create a trigger to exception handler map for the current CompoundBody.
    val triggerToExceptionHandlerMap = new TriggerToExceptionHandlerMap(
      conditionToExceptionHandlerMap = conditionToExceptionHandlerMap.toMap,
      sqlStateToExceptionHandlerMap = sqlStateToExceptionHandlerMap.toMap,
      sqlExceptionHandler = sqlExceptionHandler,
      notFoundHandler = notFoundHandler)

    val statements = compoundBody.collection
      .map(st => transformTreeIntoExecutable(st, args, context)) match {
      case Nil => Seq(new NoOpStatementExec)
      case s => s
    }

    new CompoundBodyExec(
      statements,
      compoundBody.label,
      compoundBody.isScope,
      context,
      triggerToExceptionHandlerMap)
  }

  /**
   * Transform the parsed tree to the executable node.
   *
   * @param node
   *   Root node of the parsed tree.
   * @param args
   *   A map of parameter names to SQL literal expressions.
   * @param context
   *   SqlScriptingExecutionContext keeps the execution state of current script.
   * @return
   *   Executable statement.
   */
  private def transformTreeIntoExecutable(
      node: CompoundPlanStatement,
      args: Map[String, Expression],
      context: SqlScriptingExecutionContext): CompoundStatementExec =
    node match {
      case body: CompoundBody =>
        transformBodyIntoExec(body, args, context)

      case IfElseStatement(conditions, conditionalBodies, elseBody) =>
        val conditionsExec = conditions.map(condition =>
          new SingleStatementExec(
            condition.parsedPlan,
            condition.origin,
            args,
            isInternal = false,
            context))
        val conditionalBodiesExec = conditionalBodies.map(body =>
          transformTreeIntoExecutable(body, args, context).asInstanceOf[CompoundBodyExec])
        val unconditionalBodiesExec = elseBody.map(body =>
          transformTreeIntoExecutable(body, args, context).asInstanceOf[CompoundBodyExec])
        new IfElseStatementExec(
          conditionsExec, conditionalBodiesExec, unconditionalBodiesExec, session)

      case SearchedCaseStatement(conditions, conditionalBodies, elseBody) =>
        val conditionsExec = conditions.map(condition =>
          new SingleStatementExec(
            condition.parsedPlan,
            condition.origin,
            args,
            isInternal = false,
            context))
        val conditionalBodiesExec = conditionalBodies.map(body =>
          transformTreeIntoExecutable(body, args, context).asInstanceOf[CompoundBodyExec])
        val unconditionalBodiesExec = elseBody.map(body =>
          transformTreeIntoExecutable(body, args, context).asInstanceOf[CompoundBodyExec])
        new SearchedCaseStatementExec(
          conditionsExec, conditionalBodiesExec, unconditionalBodiesExec, session)

      case SimpleCaseStatement(caseExpr, conditionExpressions, conditionalBodies, elseBody) =>
        val caseValueStmt = SingleStatement(
          Project(Seq(Alias(caseExpr, "caseVariable")()), OneRowRelation()))
        val caseVarExec = new SingleStatementExec(
          caseValueStmt.parsedPlan,
          caseExpr.origin,
          args,
          isInternal = true,
          context)
        val conditionalBodiesExec = conditionalBodies.map(body =>
          transformTreeIntoExecutable(body, args, context).asInstanceOf[CompoundBodyExec])
        val elseBodyExec = elseBody.map(body =>
          transformTreeIntoExecutable(body, args, context).asInstanceOf[CompoundBodyExec])
        new SimpleCaseStatementExec(
          caseVarExec, conditionExpressions, conditionalBodiesExec, elseBodyExec, session, context)

      case WhileStatement(condition, body, label) =>
        val conditionExec =
          new SingleStatementExec(
            condition.parsedPlan,
            condition.origin,
            args,
            isInternal = false,
            context)
        val bodyExec =
          transformTreeIntoExecutable(body, args, context).asInstanceOf[CompoundBodyExec]
        new WhileStatementExec(conditionExec, bodyExec, label, session)

      case RepeatStatement(condition, body, label) =>
        val conditionExec =
          new SingleStatementExec(
            condition.parsedPlan,
            condition.origin,
            args,
            isInternal = false,
            context)
        val bodyExec =
          transformTreeIntoExecutable(body, args, context).asInstanceOf[CompoundBodyExec]
        new RepeatStatementExec(conditionExec, bodyExec, label, session)

      case LoopStatement(body, label) =>
        val bodyExec = transformTreeIntoExecutable(body, args, context)
          .asInstanceOf[CompoundBodyExec]
        new LoopStatementExec(bodyExec, label)

      case ForStatement(query, variableNameOpt, body, label) =>
        val queryExec =
          new SingleStatementExec(
            query.parsedPlan,
            query.origin,
            args,
            isInternal = false,
            context)
        val bodyExec =
          transformTreeIntoExecutable(body, args, context).asInstanceOf[CompoundBodyExec]
        new ForStatementExec(
          queryExec, variableNameOpt, bodyExec.statements, label, session, context)

      case leaveStatement: LeaveStatement =>
        new LeaveStatementExec(leaveStatement.label)

      case iterateStatement: IterateStatement =>
        new IterateStatementExec(iterateStatement.label)

      case sparkStatement: SingleStatement =>
        new SingleStatementExec(
          sparkStatement.parsedPlan,
          sparkStatement.origin,
          args,
          isInternal = false,
          context)

      case _ => throw SparkException.internalError(s"Unsupported statement: $node")
    }
}
