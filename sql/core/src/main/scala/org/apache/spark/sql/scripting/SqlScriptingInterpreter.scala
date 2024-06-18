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

import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.parser.{CompoundBody, CompoundPlanStatement, SingleStatement}
import org.apache.spark.sql.catalyst.plans.logical.{CreateVariable, DropVariable, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.Origin

trait ProceduralLanguageInterpreter {
  def buildExecutionPlan(compound: CompoundBody) : Iterator[CompoundStatementExec]
}

case class SqlScriptingInterpreter() extends ProceduralLanguageInterpreter {
  override def buildExecutionPlan(compound: CompoundBody): Iterator[CompoundStatementExec] = {
    transformTreeIntoExecutable(compound).asInstanceOf[CompoundBodyExec]
  }

  private def getDeclareVarNameFromPlan(plan: LogicalPlan): Option[UnresolvedIdentifier] =
    plan match {
      case CreateVariable(name: UnresolvedIdentifier, _, _) => Some(name)
      case _ => None
    }

  private def transformTreeIntoExecutable(node: CompoundPlanStatement): CompoundStatementExec =
    node match {
      case body: CompoundBody =>
        val variables = body.collection.flatMap {
          case st: SingleStatement => getDeclareVarNameFromPlan(st.parsedPlan)
          case _ => None
        }
        val dropVariables = variables
          .map(varName => DropVariable(varName, ifExists = true))
          .map(new SingleStatementExec(_, Origin(), isInternal = true))
          .reverse
        new CompoundBodyExec(
          body.collection.map(st => transformTreeIntoExecutable(st)) ++ dropVariables)
      case sparkStatement: SingleStatement =>
        new SingleStatementExec(
          sparkStatement.parsedPlan,
          sparkStatement.origin,
          isInternal = false)
    }
}
