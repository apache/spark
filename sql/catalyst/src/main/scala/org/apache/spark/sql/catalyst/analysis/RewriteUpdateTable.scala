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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, EqualNullSafe, Expression, If, Literal, MetadataAttribute, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Expand, Filter, LogicalPlan, Project, ReplaceData, Union, UpdateTable, WriteDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperationTable, SupportsDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A rule that rewrites UPDATE operations using plans that operate on individual or groups of rows.
 *
 * This rule assumes the commands have been fully resolved and all assignments have been aligned.
 */
object RewriteUpdateTable extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UpdateTable(aliasedTable, assignments, cond)
        if u.resolved && u.rewritable && u.aligned =>

      EliminateSubqueryAliases(aliasedTable) match {
        case r @ DataSourceV2Relation(tbl: SupportsRowLevelOperations, _, _, _, _) =>
          val table = buildOperationTable(tbl, UPDATE, CaseInsensitiveStringMap.empty())
          val updateCond = cond.getOrElse(TrueLiteral)
          table.operation match {
            case _: SupportsDelta =>
              buildWriteDeltaPlan(r, table, assignments, updateCond)
            case _ if SubqueryExpression.hasSubquery(updateCond) =>
              buildReplaceDataWithUnionPlan(r, table, assignments, updateCond)
            case _ =>
              buildReplaceDataPlan(r, table, assignments, updateCond)
          }

        case _ =>
          u
      }
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  // if the condition does NOT contain a subquery
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {

    // resolve all required metadata attrs that may be used for grouping data on write
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a read relation and include all required metadata columns
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    // build a plan with updated and copied over records
    val updatedAndRemainingRowsPlan = buildReplaceDataUpdateProjection(
      readRelation, assignments, cond)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    val query = addOperationColumn(WRITE_WITH_METADATA_OPERATION, updatedAndRemainingRowsPlan)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs)
    ReplaceData(writeRelation, cond, query, relation, projections, Some(cond))
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  // if the condition contains a subquery
  private def buildReplaceDataWithUnionPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {

    // resolve all required metadata attrs that may be used for grouping data on write
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a read relation and include all required metadata columns
    // the same read relation will be used to read records that must be updated and copied over
    // the analyzer will take care of duplicated attr IDs
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val updatedRowsPlan = buildReplaceDataUpdateProjection(matchedRowsPlan, assignments)

    // build a plan that contains unmatched rows in matched groups that must be copied over
    val remainingRowFilter = Not(EqualNullSafe(cond, Literal.TrueLiteral))
    val remainingRowsPlan = Filter(remainingRowFilter, readRelation)

    // the new state is a union of updated and copied over records
    val updatedAndRemainingRowsPlan = Union(updatedRowsPlan, remainingRowsPlan)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    val query = addOperationColumn(WRITE_WITH_METADATA_OPERATION, updatedAndRemainingRowsPlan)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs)
    ReplaceData(writeRelation, cond, query, relation, projections, Some(cond))
  }

  // this method assumes the assignments have been already aligned before
  private def buildReplaceDataUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = TrueLiteral): LogicalPlan = {

    // the plan output may include metadata columns at the end
    // that's why the number of assignments may not match the number of plan output columns
    val assignedValues = assignments.map(_.value)
    val updatedValues = plan.output.zipWithIndex.map { case (attr, index) =>
      if (index < assignments.size) {
        val assignedExpr = assignedValues(index)
        val updatedValue = If(cond, assignedExpr, attr)
        Alias(updatedValue, attr.name)()
      } else {
        assert(MetadataAttribute.isValid(attr.metadata))
        if (MetadataAttribute.isPreservedOnUpdate(attr)) {
          attr
        } else {
          val updatedValue = If(cond, Literal(null, attr.dataType), attr)
          Alias(updatedValue, attr.name)(explicitMetadata = Some(attr.metadata))
        }
      }
    }

    Project(updatedValues, plan)
  }

  // build a rewrite plan for sources that support row deltas
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): WriteDelta = {

    val operation = operationTable.operation.asInstanceOf[SupportsDelta]

    // resolve all needed attrs (e.g. row ID and any required metadata attrs)
    val rowAttrs = relation.output
    val rowIdAttrs = resolveRowIdAttrs(relation, operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)

    // construct a read relation and include all required metadata columns
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs, rowIdAttrs)

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val rowDeltaPlan = if (operation.representUpdateAsDeleteAndInsert) {
      buildDeletesAndInserts(matchedRowsPlan, assignments, rowIdAttrs)
    } else {
      buildWriteDeltaUpdateProjection(matchedRowsPlan, assignments, rowIdAttrs)
    }

    // build a plan to write the row delta to the table
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildWriteDeltaProjections(rowDeltaPlan, rowAttrs, rowIdAttrs, metadataAttrs)
    WriteDelta(writeRelation, cond, rowDeltaPlan, relation, projections)
  }

  // this method assumes the assignments have been already aligned before
  private def buildWriteDeltaUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute]): LogicalPlan = {

    // the plan output may include immutable metadata columns at the end
    // that's why the number of assignments may not match the number of plan output columns
    val assignedValues = assignments.map(_.value)
    val updatedValues = plan.output.zipWithIndex.map { case (attr, index) =>
      if (index < assignments.size) {
        val assignedExpr = assignedValues(index)
        Alias(assignedExpr, attr.name)()
      } else {
        assert(MetadataAttribute.isValid(attr.metadata))
        if (MetadataAttribute.isPreservedOnUpdate(attr)) {
          attr
        } else {
          Alias(Literal(null, attr.dataType), attr.name)(explicitMetadata = Some(attr.metadata))
        }
      }
    }

    // original row ID values must be preserved and passed back to the table to encode updates
    // if there are any assignments to row ID attributes, add extra columns for the original values
    val originalRowIdValues = buildOriginalRowIdValues(rowIdAttrs, assignments)

    val operationType = Alias(Literal(UPDATE_OPERATION), OPERATION_COLUMN)()

    Project(Seq(operationType) ++ updatedValues ++ originalRowIdValues, plan)
  }

  private def buildDeletesAndInserts(
      matchedRowsPlan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute]): Expand = {

    val (metadataAttrs, rowAttrs) = matchedRowsPlan.output.partition { attr =>
      MetadataAttribute.isValid(attr.metadata)
    }
    val deleteOutput = deltaDeleteOutput(rowAttrs, rowIdAttrs, metadataAttrs)
    val insertOutput = deltaReinsertOutput(assignments, metadataAttrs)
    val outputs = Seq(deleteOutput, insertOutput)
    val operationTypeAttr = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)()
    val attrs = operationTypeAttr +: matchedRowsPlan.output
    val expandOutput = generateExpandOutput(attrs, outputs)
    Expand(outputs, expandOutput, matchedRowsPlan)
  }
}
