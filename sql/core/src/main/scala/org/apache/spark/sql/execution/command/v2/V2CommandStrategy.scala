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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.expressions.VariableReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.classic.Strategy
import org.apache.spark.sql.execution.SparkPlan

object V2CommandStrategy extends Strategy {

  // TODO: move v2 commands to here which are not data source v2 related.
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateVariable(ident: ResolvedIdentifier, defaultExpr, replace) =>
      CreateVariableExec(ident, defaultExpr, replace) :: Nil

    case DropVariable(ident: ResolvedIdentifier, ifExists) =>
      DropVariableExec(ident.identifier.name, ifExists) :: Nil

    case SetVariable(variables, query) =>
      SetVariableExec(variables.map(_.asInstanceOf[VariableReference]), planLater(query)) :: Nil

    case _ => Nil
  }
}
