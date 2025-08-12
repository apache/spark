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

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation


/**
 * A rule that resolves schema evolution for MERGE INTO.
 *
 * This rule will call the DSV2 Catalog to update the schema of the target table.
 */
object ResolveMergeIntoSchemaEvolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case m @ MergeIntoTable(aliasedTable, source, _, _, _, _, _)
      if m.needSchemaEvolution =>
        val newTarget = aliasedTable.transform {
          case r : DataSourceV2Relation => performSchemaEvolution(r, source)
        }
        m.copy(targetTable = newTarget)
  }

  private def performSchemaEvolution(relation: DataSourceV2Relation, source: LogicalPlan):
    DataSourceV2Relation = {
    (relation.catalog, relation.identifier) match {
      case (Some(c: TableCatalog), Some(i)) =>
        val changes = MergeIntoTable.schemaChanges(relation.schema, source.schema)
        c.alterTable(i, changes: _*)
        val newTable = c.loadTable(i)
        // Check if there are any remaining changes not applied.
        val remainingChanges = MergeIntoTable.schemaChanges(
          CatalogV2Util.v2ColumnsToStructType(newTable.columns()), source.schema)
        if (remainingChanges.nonEmpty) {
          throw QueryCompilationErrors.unsupportedTableChangesInAutoSchemaEvolutionError(
            remainingChanges, newTable.name)
        }
        relation.copy(table = newTable, output = DataTypeUtils.toAttributes(
          CatalogV2Util.v2ColumnsToStructType(newTable.columns())))
      case _ => logWarning(s"Schema Evolution enabled but data source $relation " +
        s"does not support it, skipping.")
        relation
    }
  }
}
