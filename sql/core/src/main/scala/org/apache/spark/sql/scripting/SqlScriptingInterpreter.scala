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
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.parser.{CompoundBody, CompoundPlanStatement, HandlerType, IfElseStatement, SingleStatement}
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
   * @param session
   *   Spark session that SQL script is executed within.
   * @return
   *   Iterator through collection of statements to be executed.
   */
  def buildExecutionPlan(compound: CompoundBody): Iterator[CompoundStatementExec] = {
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

  private def transformBodyIntoExec(
      compoundBody: CompoundBody,
      isExitHandler: Boolean = false,
      label: String = ""): CompoundBodyExec = {
    val variables = compoundBody.collection.flatMap {
      case st: SingleStatement => getDeclareVarNameFromPlan(st.parsedPlan)
      case _ => None
    }
    val dropVariables = variables
      .map(varName => DropVariable(varName, ifExists = true))
      .map(new SingleStatementExec(_, Origin(), isInternal = true))
      .reverse

    val conditionHandlerMap = mutable.HashMap[String, ErrorHandlerExec]()
    val handlers = ListBuffer[ErrorHandlerExec]()
    compoundBody.handlers.foreach(handler => {
      val handlerBodyExec =
        transformBodyIntoExec(handler.body,
          handler.handlerType == HandlerType.EXIT,
          compoundBody.label.get)
      val handlerExec = new ErrorHandlerExec(handlerBodyExec)

      handler.conditions.foreach(condition => {
        val conditionValue = compoundBody.conditions.getOrElse(condition, condition)
        conditionHandlerMap.get(conditionValue) match {
          case Some(_) =>
            throw SqlScriptingErrors.duplicateHandlerForSameSqlState(
              CurrentOrigin.get, conditionValue)
          case None => conditionHandlerMap.put(conditionValue, handlerExec)
        }
      })

      handlers += handlerExec
    })

    if (isExitHandler) {
      val leave = new LeaveStatementExec(label)
      val statements = compoundBody.collection.map(st => transformTreeIntoExecutable(st)) ++
        dropVariables :+ leave

      return new CompoundBodyExec(
        compoundBody.label,
        statements,
        conditionHandlerMap,
        session)
    }

    new CompoundBodyExec(
      compoundBody.label,
      compoundBody.collection.map(st => transformTreeIntoExecutable(st)) ++ dropVariables,
      conditionHandlerMap,
      session)
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
