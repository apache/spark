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

import scala.collection.mutable

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.parser.{CompoundBody, CompoundPlanStatement, HandlerType, IfElseStatement, SingleStatement, WhileStatement}
import org.apache.spark.sql.catalyst.plans.logical.{CreateVariable, DropVariable, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.errors.SqlScriptingErrors


/**
 * SQL scripting interpreter - builds SQL script execution plan.
 */
case class SqlScriptingInterpreter(session: SparkSession) {

  /**
   * Build execution plan and return statements that need to be executed,
   *   wrapped in the execution node.
   *
   * @param compound
   *   CompoundBody for which to build the plan.
   * @return
   *   Iterator through collection of statements to be executed.
   */
  private def buildExecutionPlan(compound: CompoundBody): Iterator[CompoundStatementExec] = {
    transformTreeIntoExecutable(compound).asInstanceOf[CompoundBodyExec].getTreeIterator
  }

  /**
   * Fetch the name of the Create Variable plan.
   * @param plan
   *   Plan to fetch the name from.
   * @return
   *   Name of the variable.
   */
  private def getDeclareVarNameFromPlan(plan: LogicalPlan): Option[UnresolvedIdentifier] =
    plan match {
      case CreateVariable(name: UnresolvedIdentifier, _, _) => Some(name)
      case _ => None
    }

  /**
   * Transform [[CompoundBody]] into [[CompoundBodyExec]].
   * @param compoundBody
   *   CompoundBody to be transformed into CompoundBodyExec.
   * @param isExitHandler
   *   Flag to indicate if the body is an exit handler body to add leave statement at the end.
   * @param exitHandlerLabel
   *   If body is an exit handler body, this is the label of surrounding CompoundBody
   *   that should be exited.
   * @return
   *   Executable version of the CompoundBody .
   */
  private def transformBodyIntoExec(
      compoundBody: CompoundBody,
      isExitHandler: Boolean = false,
      exitHandlerLabel: String = ""): CompoundBodyExec = {
    val variables = compoundBody.collection.flatMap {
      case st: SingleStatement => getDeclareVarNameFromPlan(st.parsedPlan)
      case _ => None
    }
    val dropVariables = variables
      .map(varName => DropVariable(varName, ifExists = true))
      .map(new SingleStatementExec(_, Origin(), isInternal = true))
      .reverse

    // Create a map of conditions (SqlStates) to their respective handlers.
    val conditionHandlerMap = mutable.HashMap[String, ErrorHandlerExec]()
    compoundBody.handlers.foreach(handler => {
      val handlerBodyExec =
        transformBodyIntoExec(handler.body,
          handler.handlerType == HandlerType.EXIT,
          compoundBody.label.get)

      // Execution node of handler.
      val handlerExec = new ErrorHandlerExec(handlerBodyExec)

      // For each condition handler is defined for, add corresponding key value pair
      // to the conditionHandlerMap.
      handler.conditions.foreach(condition => {
        // Condition can either be the key in conditions map or SqlState.
        val conditionValue = compoundBody.conditions.getOrElse(condition, condition)
        if (conditionHandlerMap.contains(conditionValue)) {
          throw SqlScriptingErrors.duplicateHandlerForSameSqlState(
            CurrentOrigin.get, conditionValue)
        } else {
          conditionHandlerMap.put(conditionValue, handlerExec)
        }
      })
    })

    if (isExitHandler) {
      // Create leave statement to exit the surrounding CompoundBody after handler execution.
      val leave = new LeaveStatementExec(exitHandlerLabel)
      val statements = compoundBody.collection.map(st => transformTreeIntoExecutable(st)) ++
        dropVariables :+ leave

      return new CompoundBodyExec(statements, session, compoundBody.label, conditionHandlerMap)
    }

    new CompoundBodyExec(
      compoundBody.collection.map(st => transformTreeIntoExecutable(st)) ++ dropVariables,
      session,
      compoundBody.label,
      conditionHandlerMap)
  }

  /**
   * Transform the parsed tree to the executable node.
   *
   * @param node
   *   Root node of the parsed tree.
   * @return
   *   Executable statement.
   */
  private def transformTreeIntoExecutable(node: CompoundPlanStatement): CompoundStatementExec =
    node match {
      case body: CompoundBody =>
        // TODO [SPARK-48530]: Current logic doesn't support scoped variables and shadowing.
        transformBodyIntoExec(body)
      case IfElseStatement(conditions, conditionalBodies, elseBody) =>
        val conditionsExec = conditions.map(condition =>
          new SingleStatementExec(condition.parsedPlan, condition.origin, isInternal = false))
        val conditionalBodiesExec = conditionalBodies.map(body =>
          transformTreeIntoExecutable(body).asInstanceOf[CompoundBodyExec])
        val unconditionalBodiesExec = elseBody.map(body =>
          transformTreeIntoExecutable(body).asInstanceOf[CompoundBodyExec])
        new IfElseStatementExec(
          conditionsExec, conditionalBodiesExec, unconditionalBodiesExec, session)
      case WhileStatement(condition, body, _) =>
        val conditionExec =
          new SingleStatementExec(condition.parsedPlan, condition.origin, isInternal = false)
        val bodyExec =
          transformTreeIntoExecutable(body).asInstanceOf[CompoundBodyExec]
        new WhileStatementExec(conditionExec, bodyExec, session)
      case sparkStatement: SingleStatement =>
        new SingleStatementExec(
          sparkStatement.parsedPlan,
          sparkStatement.origin,
          shouldCollectResult = true)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported operation in the execution plan.")
    }

  def execute(compoundBody: CompoundBody): Iterator[Array[Row]] = {
    val executionPlan = buildExecutionPlan(compoundBody)
    executionPlan.flatMap {
      case statement: SingleStatementExec if statement.raisedError =>
        val sqlState = statement.errorState.getOrElse(throw statement.rethrow.get)

        // SQLWARNING and NOT FOUND are not considered as errors.
        if (!sqlState.startsWith("01") && !sqlState.startsWith("02")) {
          // Throw the error for SQLEXCEPTION.
          throw statement.rethrow.get
        }

        // Return empty result set for SQLWARNING and NOT FOUND.
        None
      case statement: SingleStatementExec if statement.shouldCollectResult => statement.result
      case _ => None
    }
  }
}
