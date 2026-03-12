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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.ExtractV2CatalogAndIdentifier

/**
 * A rule that resolves schema evolution for MERGE INTO.
 *
 * This rule will call the DSV2 Catalog to update the schema of the target table.
 */
object ResolveMergeIntoSchemaEvolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // This rule should run only if all assignments are resolved, except those
    // that will be satisfied by schema evolution
    case m: MergeIntoTable if m.pendingSchemaChanges.nonEmpty =>
      EliminateSubqueryAliases(m.targetTable) match {
        case ExtractV2CatalogAndIdentifier(catalog, ident) =>
          evolveSchema(catalog, ident, m.pendingSchemaChanges)
          val writePrivileges = MergeIntoTable.getWritePrivileges(m)
          val newTable = catalog.loadTable(ident, writePrivileges.toSet.asJava)
          val mergeWithNewTarget = replaceMergeTarget(m, newTable)

          val remainingChanges = mergeWithNewTarget.pendingSchemaChanges
          if (remainingChanges.nonEmpty) {
            throw QueryCompilationErrors.unsupportedAutoSchemaEvolutionChangesError(
              catalog, ident, remainingChanges)
          }

          mergeWithNewTarget
        case _ =>
          m
      }
  }

  private def evolveSchema(
      catalog: TableCatalog,
      ident: Identifier,
      changes: Seq[TableChange]): Unit = {
    try {
      catalog.alterTable(ident, changes: _*)
    } catch {
      case e: IllegalArgumentException if !e.isInstanceOf[SparkThrowable] =>
        throw QueryExecutionErrors.unsupportedTableChangeError(e)
      case NonFatal(e) =>
        throw QueryCompilationErrors.failedAutoSchemaEvolutionError(
          catalog, ident, e)
    }
  }

  private def replaceMergeTarget(
      merge: MergeIntoTable,
      newTable: Table): MergeIntoTable = {
    val oldOutput = merge.targetTable.output
    val newOutput = DataTypeUtils.toAttributes(newTable.columns)
    val newTargetTable = merge.targetTable.transform {
      case r: DataSourceV2Relation => r.copy(table = newTable, output = newOutput)
    }
    val mergeWithNewTargetTable = merge.copy(targetTable = newTargetTable)
    rewriteAttrs(mergeWithNewTargetTable, oldOutput, newOutput)
  }

  private def rewriteAttrs[T <: LogicalPlan](
      plan: T,
      oldOutput: Seq[Attribute],
      newOutput: Seq[Attribute]): T = {
    val attrMap = AttributeMap(oldOutput.zip(newOutput))
    plan.rewriteAttrs(attrMap).asInstanceOf[T]
  }
}
