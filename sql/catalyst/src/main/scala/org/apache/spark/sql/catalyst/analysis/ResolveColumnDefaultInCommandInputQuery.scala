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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_ATTRIBUTE
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.{containsExplicitDefaultColumn, getDefaultValueExprOrNullLit, isExplicitDefaultColumn}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField

/**
 * A virtual rule to resolve column "DEFAULT" in [[Project]] and [[UnresolvedInlineTable]] under
 * [[InsertIntoStatement]] and [[SetVariable]]. It's only used by the real rule `ResolveReferences`.
 *
 * This virtual rule is triggered if:
 * 1. The column "DEFAULT" can't be resolved normally by `ResolveReferences`. This is guaranteed as
 *    `ResolveReferences` resolves the query plan bottom up. This means that when we reach here to
 *    resolve the command, its child plans have already been resolved by `ResolveReferences`.
 * 2. The plan nodes between [[Project]] and command are all unary nodes that inherit the
 *    output columns from its child.
 * 3. The plan nodes between [[UnresolvedInlineTable]] and command are either
 *    [[Project]], or [[Aggregate]], or [[SubqueryAlias]].
 */
class ResolveColumnDefaultInCommandInputQuery(val catalogManager: CatalogManager)
  extends SQLConfHelper with ColumnResolutionHelper {

  // TODO (SPARK-43752): support v2 write commands as well.
  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case i: InsertIntoStatement if conf.enableDefaultColumns && i.table.resolved &&
        i.query.containsPattern(UNRESOLVED_ATTRIBUTE) =>
      val staticPartCols = i.partitionSpec.filter(_._2.isDefined).keySet.map(normalizeFieldName)
      // For INSERT with static partitions, such as `INSERT INTO t PARTITION(c=1) SELECT ...`, the
      // input query schema should match the table schema excluding columns with static
      // partition values.
      val expectedQuerySchema = i.table.schema.filter { field =>
        !staticPartCols.contains(normalizeFieldName(field.name))
      }
      // Normally, we should match the query schema with the table schema by position. If the n-th
      // column of the query is the DEFAULT column, we should get the default value expression
      // defined for the n-th column of the table. However, if the INSERT has a column list, such as
      // `INSERT INTO t(b, c, a)`, the matching should be by name. For example, the first column of
      // the query should match the column 'b' of the table.
      // To simplify the implementation, `resolveColumnDefault` always does by-position match. If
      // the INSERT has a column list, we reorder the table schema w.r.t. the column list and pass
      // the reordered schema as the expected schema to `resolveColumnDefault`.
      if (i.userSpecifiedCols.isEmpty) {
        i.withNewChildren(Seq(resolveColumnDefault(i.query, expectedQuerySchema)))
      } else {
        val colNamesToFields: Map[String, StructField] = expectedQuerySchema.map { field =>
          normalizeFieldName(field.name) -> field
        }.toMap
        val reorder = i.userSpecifiedCols.map { col =>
          colNamesToFields.get(normalizeFieldName(col))
        }
        if (reorder.forall(_.isDefined)) {
          i.withNewChildren(Seq(resolveColumnDefault(i.query, reorder.flatten)))
        } else {
          i
        }
      }

    case s: SetVariable if s.targetVariables.forall(_.isInstanceOf[VariableReference]) &&
        s.sourceQuery.containsPattern(UNRESOLVED_ATTRIBUTE) =>
      val expectedQuerySchema = s.targetVariables.map {
        case v: VariableReference =>
          StructField(v.identifier.name, v.dataType, v.nullable)
            .withCurrentDefaultValue(v.varDef.defaultValueSQL)
      }
      // We match the query schema with the SET variable schema by position. If the n-th
      // column of the query is the DEFAULT column, we should get the default value expression
      // defined for the n-th variable of the SET.
      s.withNewChildren(Seq(resolveColumnDefault(s.sourceQuery, expectedQuerySchema)))

    case _ => plan
  }

  /**
   * Resolves the column "DEFAULT" in [[Project]] and [[UnresolvedInlineTable]]. A column is a
   * "DEFAULT" column if all the following conditions are met:
   * 1. The expression inside project list or inline table expressions is a single
   *    [[UnresolvedAttribute]] with name "DEFAULT". This means `SELECT DEFAULT, ...` is valid but
   *    `SELECT DEFAULT + 1, ...` is not.
   * 2. The project list or inline table expressions have less elements than the expected schema.
   *    To find the default value definition, we need to find the matching column for expressions
   *    inside project list or inline table expressions. This matching is by position and it
   *    doesn't make sense if we have more expressions than the columns of expected schema.
   * 3. The plan nodes between [[Project]] and [[InsertIntoStatement]] are
   *    all unary nodes that inherit the output columns from its child.
   * 4. The plan nodes between [[UnresolvedInlineTable]] and [[InsertIntoStatement]] are either
   *    [[Project]], or [[Aggregate]], or [[SubqueryAlias]].
   */
  private def resolveColumnDefault(
      plan: LogicalPlan,
      expectedQuerySchema: Seq[StructField],
      acceptProject: Boolean = true,
      acceptInlineTable: Boolean = true): LogicalPlan = {
    plan match {
      case _: SubqueryAlias =>
        plan.mapChildren(
          resolveColumnDefault(_, expectedQuerySchema, acceptProject, acceptInlineTable))

      case _: GlobalLimit | _: LocalLimit | _: Offset | _: Sort if acceptProject =>
        plan.mapChildren(
          resolveColumnDefault(_, expectedQuerySchema, acceptInlineTable = false))

      case p: Project if acceptProject && p.child.resolved &&
          p.containsPattern(UNRESOLVED_ATTRIBUTE) &&
          p.projectList.length <= expectedQuerySchema.length =>
        val newProjectList = p.projectList.zipWithIndex.map {
          case (u: UnresolvedAttribute, i) if isExplicitDefaultColumn(u) =>
            Alias(getDefaultValueExprOrNullLit(expectedQuerySchema(i)), u.name)()
          case (other, _) if containsExplicitDefaultColumn(other) =>
            throw QueryCompilationErrors
              .defaultReferencesNotAllowedInComplexExpressionsInInsertValuesList()
          case (other, _) => other
        }
        val newChild = resolveColumnDefault(p.child, expectedQuerySchema, acceptProject = false)
        val newProj = p.copy(projectList = newProjectList, child = newChild)
        newProj.copyTagsFrom(p)
        newProj

      case _: Project | _: Aggregate if acceptInlineTable =>
        plan.mapChildren(resolveColumnDefault(_, expectedQuerySchema, acceptProject = false))

      case inlineTable: UnresolvedInlineTable if acceptInlineTable &&
          inlineTable.containsPattern(UNRESOLVED_ATTRIBUTE) &&
          inlineTable.rows.forall(exprs => exprs.length <= expectedQuerySchema.length) =>
        val newRows = inlineTable.rows.map { exprs =>
          exprs.zipWithIndex.map {
            case (u: UnresolvedAttribute, i) if isExplicitDefaultColumn(u) =>
              getDefaultValueExprOrNullLit(expectedQuerySchema(i))
            case (other, _) if containsExplicitDefaultColumn(other) =>
              throw QueryCompilationErrors
                .defaultReferencesNotAllowedInComplexExpressionsInInsertValuesList()
            case (other, _) => other
          }
        }
        val newInlineTable = inlineTable.copy(rows = newRows)
        newInlineTable.copyTagsFrom(inlineTable)
        newInlineTable

      case other => other
    }
  }

  /**
   * Normalizes a schema field name suitable for use in looking up into maps keyed by schema field
   * names.
   * @param str the field name to normalize
   * @return the normalized result
   */
  private def normalizeFieldName(str: String): String = {
    if (SQLConf.get.caseSensitiveAnalysis) {
      str
    } else {
      str.toLowerCase()
    }
  }
}
