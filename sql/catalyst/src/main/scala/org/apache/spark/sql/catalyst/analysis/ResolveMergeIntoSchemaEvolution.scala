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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, GetStructField}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation


/**
 * A rule that resolves schema evolution for MERGE INTO.
 *
 * This rule will call the DSV2 Catalog to update the schema of the target table.
 */
object ResolveMergeIntoSchemaEvolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // This rule should run only if all assignments are resolved, except those
    // that will be satisfied by schema evolution
    case m @ MergeIntoTable(_, _, _, _, _, _, _) if m.needSchemaEvolution =>
        val newTarget = m.targetTable.transform {
          case r : DataSourceV2Relation => performSchemaEvolution(r, m)
        }

        // Unresolve all references based on old target output
        val targetOutput = m.targetTable.output
        val unresolvedMergeCondition = unresolveCondition(m.mergeCondition, targetOutput)
        val unresolvedMatchedActions = unresolveActions(m.matchedActions, targetOutput)
        val unresolvedNotMatchedActions = unresolveActions(m.notMatchedActions, targetOutput)
        val unresolvedNotMatchedBySourceActions =
          unresolveActions(m.notMatchedBySourceActions, targetOutput)

        m.copy(
          targetTable = newTarget,
          mergeCondition = unresolvedMergeCondition,
          matchedActions = unresolvedMatchedActions,
          notMatchedActions = unresolvedNotMatchedActions,
          notMatchedBySourceActions = unresolvedNotMatchedBySourceActions)
  }

  private def unresolveActions(actions: Seq[MergeAction], output: Seq[Attribute]):
  Seq[MergeAction] = {
    actions.map {
      case UpdateAction(condition, assignments) =>
        UpdateAction(condition.map(unresolveCondition(_, output)),
          unresolveAssignmentKeys(assignments))
      case InsertAction(condition, assignments) =>
        InsertAction(condition.map(unresolveCondition(_, output)),
          unresolveAssignmentKeys(assignments))
      case DeleteAction(condition) =>
        DeleteAction(condition.map(unresolveCondition(_, output)))
      case other => other
    }
  }

  private def unresolveCondition(expr: Expression, output: Seq[Attribute]): Expression = {
    val outputSet = AttributeSet(output)
    expr.transform {
      case attr: AttributeReference if outputSet.contains(attr) =>
        val nameParts = if (attr.qualifier.nonEmpty) {
          attr.qualifier ++ Seq(attr.name)
        } else {
          Seq(attr.name)
        }
        UnresolvedAttribute(nameParts)
    }
  }

  private def unresolveAssignmentKeys(assignments: Seq[Assignment]): Seq[Assignment] = {
    assignments.map { assignment =>
      val unresolvedKey = assignment.key match {
        case _: UnresolvedAttribute => assignment.key
        case gsf: GetStructField =>
          // Recursively collect all nested GetStructField names and the base AttributeReference
          val nameParts = collectStructFieldNames(gsf)
          nameParts match {
            case Some(names) => UnresolvedAttribute(names)
            case None => assignment.key
          }
        case attr: AttributeReference =>
          UnresolvedAttribute(Seq(attr.name))
        case attr: Attribute =>
          UnresolvedAttribute(Seq(attr.name))
        case other => other
      }
      Assignment(unresolvedKey, assignment.value)
    }
  }

  private def collectStructFieldNames(expr: Expression): Option[Seq[String]] = {
    expr match {
      case GetStructField(child, _, Some(fieldName)) =>
        collectStructFieldNames(child) match {
          case Some(childNames) => Some(childNames :+ fieldName)
          case None => None
        }
      case attr: AttributeReference =>
        Some(Seq(attr.name))
      case _ =>
        None
    }
  }

  private def performSchemaEvolution(relation: DataSourceV2Relation, m: MergeIntoTable)
    : DataSourceV2Relation = {
    (relation.catalog, relation.identifier) match {
      case (Some(c: TableCatalog), Some(i)) =>
        val referencedSourceSchema = MergeIntoTable.sourceSchemaForSchemaEvolution(m)

        val changes = MergeIntoTable.schemaChanges(relation.schema, referencedSourceSchema)
        c.alterTable(i, changes: _*)
        val newTable = c.loadTable(i)
        val newSchema = CatalogV2Util.v2ColumnsToStructType(newTable.columns())
        // Check if there are any remaining changes not applied.
        val remainingChanges = MergeIntoTable.schemaChanges(newSchema, referencedSourceSchema)
        if (remainingChanges.nonEmpty) {
          throw QueryCompilationErrors.unsupportedTableChangesInAutoSchemaEvolutionError(
            remainingChanges, i.toQualifiedNameParts(c))
        }
        relation.copy(table = newTable, output = DataTypeUtils.toAttributes(newSchema))
      case _ => logWarning(s"Schema Evolution enabled but data source $relation " +
        s"does not support it, skipping.")
        relation
    }
  }
}
