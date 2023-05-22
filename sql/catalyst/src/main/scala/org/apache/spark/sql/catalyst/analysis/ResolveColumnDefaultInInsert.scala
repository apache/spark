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
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_ATTRIBUTE
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.{getDefaultValueExpr, isExplicitDefaultColumn}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField

/**
 * A virtual rule to resolve column "DEFAULT" in [[Project]] and [[UnresolvedInlineTable]] under
 * [[InsertIntoStatement]]. It's only used by the real rule `ResolveReferences`.
 *
 * This virtual rule is triggered if:
 * 1. The column "DEFAULT" can't be resolved normally by `ResolveReferences`. This is guaranteed as
 *    `ResolveReferences` resolves the query plan bottom up. This means that when we reach here to
 *    resolve [[InsertIntoStatement]], its child plans have already been resolved by
 *    `ResolveReferences`.
 * 2. The plan nodes between [[Project]]/[[UnresolvedInlineTable]] and [[InsertIntoStatement]] are
 *    all unary nodes that inherit the output columns from its child.
 */
case object ResolveColumnDefaultInInsert extends SQLConfHelper with ColumnResolutionHelper {
  // TODO: support v2 write commands as well.
  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case i: InsertIntoStatement if conf.enableDefaultColumns && i.table.resolved &&
        i.query.containsPattern(UNRESOLVED_ATTRIBUTE) =>
      val staticPartCols = i.partitionSpec.filter(_._2.isDefined).keys.map(normalizeFieldName).toSet
      val expectedQuerySchema = i.table.schema.filter { field =>
        !staticPartCols.contains(normalizeFieldName(field.name))
      }
      if (i.userSpecifiedCols.isEmpty) {
        i.withNewChildren(Seq(resolveColumnDefault(i.query, expectedQuerySchema)))
      } else {
        // Reorder the fields in `expectedQuerySchema` according to the user-specified column list
        // of the INSERT command.
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

    case _ => plan
  }

  private def resolveColumnDefault(
      plan: LogicalPlan,
      expectedQuerySchema: Seq[StructField]): LogicalPlan = {
    plan match {
      case _: GlobalLimit | _: LocalLimit | _: Offset | _: SubqueryAlias | _: Sort =>
        plan.mapChildren(resolveColumnDefault(_, expectedQuerySchema))

      case p: Project if p.child.resolved && p.containsPattern(UNRESOLVED_ATTRIBUTE) &&
          p.projectList.length <= expectedQuerySchema.length =>
        var changed = false
        val newProjectList = p.projectList.zipWithIndex.map {
          case (u: UnresolvedAttribute, i) if isExplicitDefaultColumn(u) =>
            changed = true
            val field = expectedQuerySchema(i)
            Alias(getDefaultValueExpr(field).getOrElse(Literal(null, field.dataType)), u.name)()
          case (other, _) => other
        }
        if (changed) {
          val newProj = p.copy(projectList = newProjectList)
          newProj.copyTagsFrom(p)
          newProj
        } else {
          p
        }

      case inlineTable: UnresolvedInlineTable
          if inlineTable.containsPattern(UNRESOLVED_ATTRIBUTE) &&
            inlineTable.rows.forall(exprs => exprs.length <= expectedQuerySchema.length) =>
        var changed = false
        val newRows = inlineTable.rows.map { exprs =>
          exprs.zipWithIndex.map {
            case (u: UnresolvedAttribute, i) if isExplicitDefaultColumn(u) =>
              changed = true
              val field = expectedQuerySchema(i)
              getDefaultValueExpr(field).getOrElse(Literal(null, field.dataType))
            case (other, _) => other
          }
        }
        if (changed) {
          val newInlineTable = inlineTable.copy(rows = newRows)
          newInlineTable.copyTagsFrom(inlineTable)
          newInlineTable
        } else {
          inlineTable
        }

      case other => other
    }
  }

  /**
   * Normalizes a schema field name suitable for use in looking up into maps keyed by schema field
   * names.
   * @param str the field name to normalize
   * @return the normalized result
   */
  def normalizeFieldName(str: String): String = {
    if (SQLConf.get.caseSensitiveAnalysis) {
      str
    } else {
      str.toLowerCase()
    }
  }
}
