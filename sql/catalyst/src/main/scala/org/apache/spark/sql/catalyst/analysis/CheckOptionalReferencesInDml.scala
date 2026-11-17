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
import org.apache.spark.sql.catalyst.trees.Origin
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
 * `withColumns`, when it plans them. An analyzed operation whose analysis resolved an optional
 * reference to NULL is rejected as well. Its analysis marked the plan that it returned, see
 * `OptionalReferences.RESOLVED_NULL_REFERENCE`, because the rest of the analysis can have
 * replaced the NULL itself, as for `attrs?['active']`.
 *
 * The same holds for a subquery of the statement that was built from a Dataset, such as
 * `Dataset.exists` in the condition of `DataFrameWriterV2.overwrite`, which holds the analyzed
 * plan of that Dataset.
 *
 * The rule runs whether or not optional references are enabled: a source DataFrame that was built
 * while they were enabled keeps the NULLs of its analyzed operations, also in a MERGE that runs
 * after they are disabled.
 *
 * {{{
 *   df.writeTo("t").overwrite(expr("deleted_at? IS NULL"))
 *   spark.table("s").selectExpr("pk", "attrs?['active'] AS active").as("s").mergeInto("t", ...)
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
            // TODO: An unbound named parameter marker `:p?` in the SQL text of a source DataFrame
            // is not rejected: by default, `SubstituteParamsParser` replaces it with the text NULL
            // before the text is parsed, which leaves nothing in the plan for this check to find.
            OptionalReferences
              .unresolvedOptionalReference(merge.sourceTable)
              .map(_.origin)
              .orElse(resolvedNullReference(merge.sourceTable))
              .foreach { origin =>
                throw QueryCompilationErrors.optionalReferenceInDmlNotSupportedError(
                  "MERGE",
                  origin
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
   * Returns the origin of the first optional reference of `plan` that an analysis of an operation
   * of `plan` resolved to NULL, or None: of a NULL that is still there, see
   * [[OptionalReferences.isNullReference]], or else of the first mark of such an analysis, see
   * [[OptionalReferences.RESOLVED_NULL_REFERENCE]]. A NULL that is still there is the more precise
   * position, as for `missing?:a`, where the mark keeps `missing?`, the first NULL that the
   * analysis created. Views and SQL functions resolve their own optional references, so the
   * search does not enter a view, a SQL table function, or the body of a SQL scalar function.
   */
  private def resolvedNullReference(plan: LogicalPlan): Option[Origin] = {
    val search = new ResolvedNullReferenceSearch
    search.inPlan(plan).orElse(search.firstMark)
  }

  /** Like [[resolvedNullReference]] for a plan, in `expression` and its subquery plans. */
  private def resolvedNullReference(expression: Expression): Option[Origin] = {
    val search = new ResolvedNullReferenceSearch
    search.inExpression(expression).orElse(search.firstMark)
  }

  /**
   * One search of [[resolvedNullReference]]. It visits every plan and expression at most once,
   * returns the first NULL, and remembers the first mark on the way.
   */
  private class ResolvedNullReferenceSearch {
    var firstMark: Option[Origin] = None

    def inPlan(plan: LogicalPlan): Option[Origin] = plan match {
      case _: View | _: SQLFunctionNode =>
        None
      case _ =>
        firstMark = firstMark.orElse(OptionalReferences.resolvedNullReferenceOrigin(plan))
        plan.expressions.iterator
          .flatMap(inExpression)
          .nextOption()
          .orElse(plan.children.iterator.flatMap(inPlan).nextOption())
    }

    def inExpression(expression: Expression): Option[Origin] = expression match {
      case _ if OptionalReferences.isNullReference(expression) =>
        Some(expression.origin)
      case _: SQLScalarFunction =>
        None
      case subquery: SubqueryExpression =>
        inPlan(subquery.plan).orElse(subquery.children.iterator.flatMap(inExpression).nextOption())
      case _ =>
        expression.children.iterator.flatMap(inExpression).nextOption()
    }
  }

  private def checkExpression(expression: Expression, statement: String): Unit = {
    OptionalReferences
      .unresolvedOptionalReference(expression)
      .map(_.origin)
      // A subquery that was built from a Dataset, e.g. with `Dataset.exists`, holds its analyzed
      // plan.
      .orElse(resolvedNullReference(expression))
      .foreach { origin =>
        throw QueryCompilationErrors.optionalReferenceInDmlNotSupportedError(statement, origin)
      }
  }
}
