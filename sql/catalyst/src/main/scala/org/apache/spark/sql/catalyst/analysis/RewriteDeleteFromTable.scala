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

import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, Expression, Not}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, LogicalPlan, ReplaceData}
import org.apache.spark.sql.connector.catalog.{SupportsDeleteV2, SupportsRowLevelOperations, TruncatableTable}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A rule that rewrites DELETE operations using plans that operate on individual or groups of rows.
 *
 * If a table implements [[SupportsDeleteV2]] and [[SupportsRowLevelOperations]], this rule will
 * still rewrite the DELETE operation but the optimizer will check whether this particular DELETE
 * statement can be handled by simply passing delete filters to the connector. If so, the optimizer
 * will discard the rewritten plan and will allow the data source to delete using filters.
 */
object RewriteDeleteFromTable extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case d @ DeleteFromTable(aliasedTable, cond) if d.resolved =>
      EliminateSubqueryAliases(aliasedTable) match {
        case DataSourceV2Relation(_: TruncatableTable, _, _, _, _) if cond == TrueLiteral =>
          // don't rewrite as the table supports truncation
          d

        case r @ DataSourceV2Relation(t: SupportsRowLevelOperations, _, _, _, _) =>
          val table = buildOperationTable(t, DELETE, CaseInsensitiveStringMap.empty())
          buildReplaceDataPlan(r, table, cond)

        case DataSourceV2Relation(_: SupportsDeleteV2, _, _, _, _) =>
          // don't rewrite as the table supports deletes only with filters
          d

        case _ =>
          d
      }
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      cond: Expression): ReplaceData = {

    // resolve all required metadata attrs that may be used for grouping data on write
    // for instance, JDBC data source may cluster data by shard/host before writing
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a read relation and include all required metadata columns
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    // construct a plan that contains unmatched rows in matched groups that must be carried over
    // such rows do not match the condition but have to be copied over as the source can replace
    // only groups of rows (e.g. if a source supports replacing files, unmatched rows in matched
    // files must be carried over)
    // it is safe to negate the condition here as the predicate pushdown for group-based row-level
    // operations is handled in a special way
    val remainingRowsFilter = Not(EqualNullSafe(cond, TrueLiteral))
    val remainingRowsPlan = Filter(remainingRowsFilter, readRelation)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    ReplaceData(writeRelation, cond, remainingRowsPlan, relation)
  }
}
