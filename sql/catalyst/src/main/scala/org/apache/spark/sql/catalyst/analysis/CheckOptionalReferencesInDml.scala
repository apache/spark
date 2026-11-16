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

import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{
  DeleteFromTable,
  LogicalPlan,
  MergeIntoTable,
  OverwriteByExpression,
  UpdateTable,
  View
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
 * The source of a MERGE is checked too, like the source query of the SQL statement.
 * `Dataset.mergeInto` and the Spark Connect MERGE command put the plan of the source DataFrame
 * there, in which only the last operation is sure to be unanalyzed: a Dataset operation builds on
 * the analyzed plan of the one before, and Spark Connect analyzes some relations, such as
 * `withColumns`, when it plans them. In an analyzed operation, an optional reference that matched
 * nothing is the NULL that it resolved to, which is rejected as well.
 *
 * {{{
 *   df.writeTo("t").overwrite(expr("deleted_at? IS NULL"))
 *   spark.table("s").selectExpr("pk", "archived? AS archived").as("s").mergeInto("t", ...)
 * }}}
 */
object CheckOptionalReferencesInDml extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.containsPattern(COMMAND)) {
      plan.foreach { operator =>
        statementName(operator).foreach { statement =>
          operator.expressions.foreach(checkExpression(_, statement))
        }
        operator match {
          case merge: MergeIntoTable =>
            OptionalReferences
              .unresolvedOptionalReference(merge.sourceTable)
              .orElse(firstNullReference(merge.sourceTable))
              .foreach { reference =>
                throw QueryCompilationErrors.optionalReferenceInDmlNotSupportedError(
                  "MERGE",
                  reference.origin
                )
              }
          case _ =>
        }
      }
    }
    plan
  }

  private def statementName(operator: LogicalPlan): Option[String] = operator match {
    case _: DeleteFromTable => Some("DELETE")
    case _: UpdateTable => Some("UPDATE")
    case _: MergeIntoTable => Some("MERGE")
    // The plan of INSERT ... REPLACE WHERE and of `DataFrameWriterV2.overwrite(condition)`.
    case _: OverwriteByExpression => Some("INSERT ... REPLACE")
    case _ => None
  }

  /**
   * Returns the first NULL that an optional reference of `plan` resolved to in an analysis of an
   * operation of `plan`, see [[OptionalReferences.isNullReference]], or None. Views and SQL
   * functions resolve their own optional references, so the search does not enter a view, a SQL
   * table function, or the body of a SQL scalar function.
   */
  private def firstNullReference(plan: LogicalPlan): Option[Expression] = plan match {
    case _: View | _: SQLFunctionNode =>
      None
    case _ =>
      plan.expressions.iterator
        .flatMap(expression => firstNullReference(expression))
        .nextOption()
        .orElse(plan.children.iterator.flatMap(child => firstNullReference(child)).nextOption())
  }

  private def firstNullReference(expression: Expression): Option[Expression] = expression match {
    case _ if OptionalReferences.isNullReference(expression) =>
      Some(expression)
    case _: SQLScalarFunction =>
      None
    case subquery: SubqueryExpression =>
      firstNullReference(subquery.plan).orElse(
        subquery.children.iterator.flatMap(child => firstNullReference(child)).nextOption())
    case _ =>
      expression.children.iterator.flatMap(child => firstNullReference(child)).nextOption()
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
