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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{CaseStatement, CompoundBody, CompoundPlanStatement, CreateVariable, DropVariable, ForStatement, IfElseStatement, IterateStatement, LeaveStatement, LogicalPlan, LoopStatement, RepeatStatement, SingleStatement, WhileStatement}
import org.apache.spark.sql.catalyst.trees.Origin

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
   * @return
   *   Iterator through collection of statements to be executed.
   */
  def buildExecutionPlan(
      compound: CompoundBody,
      args: Map[String, Expression]): Iterator[CompoundStatementExec] = {
    transformTreeIntoExecutable(compound, args)
      .asInstanceOf[CompoundBodyExec].getTreeIterator
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
   * @param args
   *   A map of parameter names to SQL literal expressions.
   * @return
   *   Executable statement.
   */
  private def transformTreeIntoExecutable(
      node: CompoundPlanStatement,
      args: Map[String, Expression]): CompoundStatementExec =
    node match {
      case CompoundBody(collection, label) =>
        // TODO [SPARK-48530]: Current logic doesn't support scoped variables and shadowing.
        val variables = collection.flatMap {
          case st: SingleStatement => getDeclareVarNameFromPlan(st.parsedPlan)
          case _ => None
        }
        val dropVariables = variables
          .map(varName => DropVariable(varName, ifExists = true))
          .map(new SingleStatementExec(_, Origin(), args, isInternal = true))
          .reverse

        val statements =
          collection.map(st => transformTreeIntoExecutable(st, args)) ++ dropVariables match {
            case Nil => Seq(new NoOpStatementExec)
            case s => s
          }

        new CompoundBodyExec(statements, label)

      case IfElseStatement(conditions, conditionalBodies, elseBody) =>
        val conditionsExec = conditions.map(condition =>
          new SingleStatementExec(
            condition.parsedPlan,
            condition.origin,
            args,
            isInternal = false))
        val conditionalBodiesExec = conditionalBodies.map(body =>
          transformTreeIntoExecutable(body, args).asInstanceOf[CompoundBodyExec])
        val unconditionalBodiesExec = elseBody.map(body =>
          transformTreeIntoExecutable(body, args).asInstanceOf[CompoundBodyExec])
        new IfElseStatementExec(
          conditionsExec, conditionalBodiesExec, unconditionalBodiesExec, session)

      case CaseStatement(conditions, conditionalBodies, elseBody) =>
        val conditionsExec = conditions.map(condition =>
          new SingleStatementExec(
            condition.parsedPlan,
            condition.origin,
            args,
            isInternal = false))
        val conditionalBodiesExec = conditionalBodies.map(body =>
          transformTreeIntoExecutable(body, args).asInstanceOf[CompoundBodyExec])
        val unconditionalBodiesExec = elseBody.map(body =>
          transformTreeIntoExecutable(body, args).asInstanceOf[CompoundBodyExec])
        new CaseStatementExec(
          conditionsExec, conditionalBodiesExec, unconditionalBodiesExec, session)

      case WhileStatement(condition, body, label) =>
        val conditionExec =
          new SingleStatementExec(
            condition.parsedPlan,
            condition.origin,
            args,
            isInternal = false)
        val bodyExec =
          transformTreeIntoExecutable(body, args).asInstanceOf[CompoundBodyExec]
        new WhileStatementExec(conditionExec, bodyExec, label, session)

      case RepeatStatement(condition, body, label) =>
        val conditionExec =
          new SingleStatementExec(
            condition.parsedPlan,
            condition.origin,
            args,
            isInternal = false)
        val bodyExec =
          transformTreeIntoExecutable(body, args).asInstanceOf[CompoundBodyExec]
        new RepeatStatementExec(conditionExec, bodyExec, label, session)

      case LoopStatement(body, label) =>
        val bodyExec = transformTreeIntoExecutable(body, args)
          .asInstanceOf[CompoundBodyExec]
        new LoopStatementExec(bodyExec, label)

      case ForStatement(query, variableNameOpt, body, label) =>
        val queryExec =
          new SingleStatementExec(
            query.parsedPlan,
            query.origin,
            args,
            isInternal = false)
        val bodyExec =
          transformTreeIntoExecutable(body, args).asInstanceOf[CompoundBodyExec]
        new ForStatementExec(queryExec, variableNameOpt, bodyExec, label, session)

      case leaveStatement: LeaveStatement =>
        new LeaveStatementExec(leaveStatement.label)

      case iterateStatement: IterateStatement =>
        new IterateStatementExec(iterateStatement.label)

      case sparkStatement: SingleStatement =>
        new SingleStatementExec(
          sparkStatement.parsedPlan,
          sparkStatement.origin,
          args,
          isInternal = false)
    }
}
