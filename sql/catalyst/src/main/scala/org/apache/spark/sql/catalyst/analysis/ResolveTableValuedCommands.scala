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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Rule that resolves table-valued command references.
 */
object ResolveTableValuedCommands extends Rule[LogicalPlan] {

  private def isSupportedCommand(plan: LogicalPlan): Boolean = plan match {
    case ShowNamespaces(_, _, _) => true
    case ShowTables(_, _, _) => true
    case ShowTableProperties(_, _, _) => true
    case ShowPartitions(_, _, _) => true
    case ShowColumns(_, _, _) => true
    case ShowViews(_, _, _) => true
    case ShowFunctions(_, _, _, _, _) => true
    case _ => false
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedTableValuedCommand =>
      val parsedPlan = CatalystSqlParser.parsePlan(u.command)
      if (isSupportedCommand(parsedPlan)) {
        parsedPlan
      } else {
        throw QueryCompilationErrors.unsupportedCommandForTableValuedError(u.command)
      }
    case u => u
  }

}
