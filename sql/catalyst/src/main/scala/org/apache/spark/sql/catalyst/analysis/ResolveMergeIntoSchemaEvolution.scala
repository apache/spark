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
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation


/**
 * A rule that resolves schema evolution for MERGE INTO.
 *
 * This rule will call the Data Source connector to get a new schema for the target table
 * based on the source relation.
 */
object ResolveMergeIntoSchemaEvolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case m @ MergeIntoTable(aliasedTable, source, _, _, _, _, _)
      if m.withSchemaEvolution && !schemaEvolved(m) =>
        val newTarget = aliasedTable.transform {
          case s @ SubqueryAlias(_,
            r @ DataSourceV2Relation(t: SupportsRowLevelOperations, _, _, _, _)) =>
            val newSchema = t.mergeSchema(source.schema)
            val newRelation = r.copy(output = DataTypeUtils.toAttributes(newSchema))
            s.copy(child = newRelation)
          case r @ DataSourceV2Relation(t: SupportsRowLevelOperations, _, _, _, _) =>
            val newSchema = t.mergeSchema(source.schema)
            r.copy(output = DataTypeUtils.toAttributes(newSchema))
        }

        m.copy(targetTable = newTarget)
  }

  def schemaEvolved(m: MergeIntoTable): Boolean = {
    EliminateSubqueryAliases(m.targetTable) match {
      case r @ DataSourceV2Relation(t: SupportsRowLevelOperations, _, _, _, _) =>
        DataTypeUtils.fromAttributes(r.output) == t.mergeSchema(m.sourceTable.schema)
    }
  }
}

/**
 * This rule checks for references to V2 Datasource in MERGE INTO statements WITH SCHEMA EVOLUTION
 * and synchronizes the catalog if evolution was detected.
 */
object MergeSyncSchemaToMetastore extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit =
    plan.foreach {
      case MergeRows(_, _, _, _, _, _, _, child, Some(evolvedTargetSchema)) =>
        child foreach {
          case r: DataSourceV2Relation =>
            r.updateSchema(evolvedTargetSchema)
          case _ => // OK
        }
      case _ => // OK
    }
}
