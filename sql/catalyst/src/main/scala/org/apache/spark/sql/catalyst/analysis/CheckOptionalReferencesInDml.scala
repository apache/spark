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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{
  DeleteFromTable,
  LogicalPlan,
  MergeIntoTable,
  OverwriteByExpression,
  UpdateTable
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Rejects optional references in the expressions of a statement that modifies table data and
 * that was built without the SQL parser, which rejects them in SQL text (see
 * `OptionalReferenceValidation`). A missing column must not silently change which rows a
 * statement deletes or updates.
 *
 * {{{
 *   df.writeTo("t").overwrite(expr("deleted_at? IS NULL"))
 * }}}
 */
object CheckOptionalReferencesInDml extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.containsPattern(COMMAND)) {
      plan.foreach { operator =>
        statementName(operator).foreach { statement =>
          operator.expressions.foreach(checkExpression(_, statement))
        }
      }
    }
    plan
  }

  private def statementName(operator: LogicalPlan): Option[String] = operator match {
    case _: DeleteFromTable => Some("DELETE")
    case _: UpdateTable => Some("UPDATE")
    case _: MergeIntoTable => Some("MERGE")
    // The plan of INSERT ... REPLACE WHERE.
    case _: OverwriteByExpression => Some("INSERT")
    case _ => None
  }

  private def checkExpression(expression: Expression, statement: String): Unit = {
    OptionalReferences.unresolvedOptionalReference(expression).foreach { reference =>
      throw QueryCompilationErrors.optionalReferenceInDmlNotSupportedError(
        statement,
        reference.origin
      )
    }
  }
}
