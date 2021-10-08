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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualNullSafe, Expression, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, LogicalPlan, Project, ReplaceData, WriteDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.catalog.{SupportsDelete, SupportsRowLevelOperations}
import org.apache.spark.sql.connector.write.{RowLevelOperationTable, SupportsDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.BooleanType

/**
 * Assigns a rewrite plan for v2 tables that support rewriting data to handle DELETE statements.
 *
 * If a table implements [[SupportsDelete]] and [[SupportsRowLevelOperations]], we assign a rewrite
 * plan but the optimizer will check whether this particular DELETE statement can be handled
 * by simply passing delete filters to the connector. If yes, the optimizer will then discard
 * the rewrite plan.
 */
object RewriteDeleteFromTable extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case d @ DeleteFromTable(aliasedTable, cond, None) if d.resolved =>
      EliminateSubqueryAliases(aliasedTable) match {
        case r @ DataSourceV2Relation(tbl: SupportsRowLevelOperations, _, _, _, _) =>
          val operation = buildRowLevelOperation(tbl, DELETE)
          val table = RowLevelOperationTable(tbl, operation)
          val rewritePlan = operation match {
            case _: SupportsDelta =>
              buildWriteDeltaPlan(r, table, cond)
            case _ =>
              buildReplaceDataPlan(r, table, cond)
          }
          // keep the original relation in DELETE so that we can attempt to delete with metadata
          DeleteFromTable(r, cond, Some(rewritePlan))

        case DataSourceV2Relation(_: SupportsDelete, _, _, _, _) =>
          // don't assign a rewrite plan as the table supports deletes only with filters
          d

        case DataSourceV2Relation(t, _, _, _, _) =>
          throw new AnalysisException(s"Table $t does not support DELETE statements")

        case _ =>
          d
      }
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      cond: Option[Expression]): ReplaceData = {

    // resolve all needed attrs (e.g. metadata attrs for grouping data on write)
    val rowAttrs = relation.output
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, table.operation)

    // construct a scan relation and include all required metadata columns
    val scanAttrs = dedupAttrs(rowAttrs ++ metadataAttrs)
    val scanRelation = relation.copy(table = table, output = scanAttrs)

    // construct a plan that contains unmatched rows in matched groups that must be carried over
    // such rows do not match the condition but have to be copied over as the source can replace
    // only groups of rows
    val deleteCond = cond.getOrElse(Literal.TrueLiteral)
    val remainingRowsFilter = Not(EqualNullSafe(deleteCond, Literal(true, BooleanType)))
    val remainingRowsPlan = Filter(remainingRowsFilter, scanRelation)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = table)
    ReplaceData(writeRelation, remainingRowsPlan, relation)
  }

  // build a rewrite plan for sources that support row deltas
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      cond: Option[Expression]): WriteDelta = {

    // resolve all needed attrs (e.g. row ID and any required metadata attrs)
    val rowAttrs = relation.output
    val rowIdAttrs = resolveRowIdAttrs(relation, table.operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, table.operation)

    // construct a scan relation and include all required metadata columns
    val scanAttrs = dedupAttrs(rowAttrs ++ rowIdAttrs ++ metadataAttrs)
    val scanRelation = relation.copy(table = table, output = scanAttrs)

    // construct a plan that only contains records to delete
    val deleteCond = cond.getOrElse(Literal.TrueLiteral)
    val deletedRowsPlan = Filter(deleteCond, scanRelation)
    val operationType = Alias(Literal(DELETE_OPERATION), OPERATION_COLUMN)()
    val requiredWriteAttrs = dedupAttrs(rowIdAttrs ++ metadataAttrs)
    val project = Project(operationType +: requiredWriteAttrs, deletedRowsPlan)

    // build a plan to write deletes to the table
    val writeRelation = relation.copy(table = table)
    val projections = buildWriteDeltaProjections(project, Nil, rowIdAttrs, metadataAttrs)
    WriteDelta(writeRelation, project, relation, projections)
  }
}
