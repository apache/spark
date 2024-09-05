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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.parser.{CompoundBody, CompoundPlanStatement, IfElseStatement, IterateStatement, LeaveStatement, SingleStatement, WhileStatement}
import org.apache.spark.sql.catalyst.plans.logical.{CreateVariable, DropVariable, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.Origin

/**
 * SQL scripting interpreter - builds SQL script execution plan.
 */
case class SqlScriptingInterpreter() {

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
  def buildExecutionPlan(
      compound: CompoundBody,
      session: SparkSession): Iterator[CompoundStatementExec] = {
    transformTreeIntoExecutable(compound, session).asInstanceOf[CompoundBodyExec].getTreeIterator
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
   * Transform the parsed tree to the executable node.
   *
   * @param node
   *   Root node of the parsed tree.
   * @param session
   *   Spark session that SQL script is executed within.
   * @return
   *   Executable statement.
   */
  private def transformTreeIntoExecutable(
      node: CompoundPlanStatement, session: SparkSession): CompoundStatementExec =
    node match {
      case CompoundBody(collection, label) =>
        // TODO [SPARK-48530]: Current logic doesn't support scoped variables and shadowing.
        val variables = collection.flatMap {
          case st: SingleStatement => getDeclareVarNameFromPlan(st.parsedPlan)
          case _ => None
        }
        val dropVariables = variables
          .map(varName => DropVariable(varName, ifExists = true))
          .map(new SingleStatementExec(_, Origin(), isInternal = true))
          .reverse
        new CompoundBodyExec(
          collection.map(st => transformTreeIntoExecutable(st, session)) ++ dropVariables,
          label)
      case IfElseStatement(conditions, conditionalBodies, elseBody) =>
        val conditionsExec = conditions.map(condition =>
          new SingleStatementExec(condition.parsedPlan, condition.origin, isInternal = false))
        val conditionalBodiesExec = conditionalBodies.map(body =>
          transformTreeIntoExecutable(body, session).asInstanceOf[CompoundBodyExec])
        val unconditionalBodiesExec = elseBody.map(body =>
          transformTreeIntoExecutable(body, session).asInstanceOf[CompoundBodyExec])
        new IfElseStatementExec(
          conditionsExec, conditionalBodiesExec, unconditionalBodiesExec, session)
      case WhileStatement(condition, body, label) =>
        val conditionExec =
          new SingleStatementExec(condition.parsedPlan, condition.origin, isInternal = false)
        val bodyExec =
          transformTreeIntoExecutable(body, session).asInstanceOf[CompoundBodyExec]
        new WhileStatementExec(conditionExec, bodyExec, label, session)
      case leaveStatement: LeaveStatement =>
        new LeaveStatementExec(leaveStatement.label)
      case iterateStatement: IterateStatement =>
        new IterateStatementExec(iterateStatement.label)
      case sparkStatement: SingleStatement =>
        new SingleStatementExec(
          sparkStatement.parsedPlan,
          sparkStatement.origin,
          isInternal = false)
    }
}
