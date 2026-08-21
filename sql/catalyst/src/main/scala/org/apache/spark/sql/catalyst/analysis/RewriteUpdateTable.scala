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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, EqualNullSafe, Expression, If, Literal, MetadataAttribute, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Expand, Filter, LogicalPlan, Project, ReplaceData, Union, UpdateTable, WriteDelta}
import org.apache.spark.sql.catalyst.trees.TreePattern.UPDATE_TABLE
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperation, RowLevelOperationTable, SupportsColumnUpdates, SupportsDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2Table}
import org.apache.spark.sql.types.IntegerType

/**
 * A rule that rewrites UPDATE operations using plans that operate on individual or groups of rows.
 *
 * This rule assumes the commands have been fully resolved and all assignments have been aligned.
 */
object RewriteUpdateTable extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(UPDATE_TABLE)) {
    case u @ UpdateTable(aliasedTable, assignments, cond)
        if u.resolved && u.rewritable && u.aligned =>

      EliminateSubqueryAliases(aliasedTable) match {
        case r @ ExtractV2Table(tbl: SupportsRowLevelOperations) =>
          checkNoGeneratedColumns(r, UPDATE)
          val updatedAttrs = collectUpdatedAttrs(assignments)
          val table = buildOperationTable(tbl, UPDATE, r.options, updatedAttrs)
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
    val supportsColumnUpdate = operationTable.operation.isInstanceOf[SupportsColumnUpdates]
    val connectorDataAttrs =
      resolveConnectorDataAttrs(relation, operationTable.operation, assignments)

    // construct a read relation and include all required metadata columns
    val readRelation = if (supportsColumnUpdate) {
      val narrowDataAttrs = computeNarrowReadAttrs(relation, connectorDataAttrs, assignments, cond)
      buildNarrowRelationWithAttrs(relation, operationTable, narrowDataAttrs, metadataAttrs)
    } else {
      buildRelationWithAttrs(relation, operationTable, metadataAttrs)
    }

    // build a plan with updated and copied over records
    val query = if (supportsColumnUpdate) {
      buildNarrowReplaceDataUpdateProjection(readRelation, assignments, cond)
    } else {
      buildReplaceDataUpdateProjection(readRelation, assignments, cond)
    }

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs,
      connectorDataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    ReplaceData(writeRelation, cond, query, relation, projections, groupFilterCond)
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
    val supportsColumnUpdate = operationTable.operation.isInstanceOf[SupportsColumnUpdates]
    val connectorDataAttrs =
      resolveConnectorDataAttrs(relation, operationTable.operation, assignments)

    // construct a read relation and include all required metadata columns
    // the same read relation will be used to read records that must be updated and copied over
    // the analyzer will take care of duplicated attr IDs
    val readRelation = if (supportsColumnUpdate) {
      val narrowDataAttrs = computeNarrowReadAttrs(relation, connectorDataAttrs, assignments, cond)
      buildNarrowRelationWithAttrs(relation, operationTable, narrowDataAttrs, metadataAttrs)
    } else {
      buildRelationWithAttrs(relation, operationTable, metadataAttrs)
    }

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val updatedRowsPlan = if (supportsColumnUpdate) {
      buildNarrowReplaceDataUpdateProjection(matchedRowsPlan, assignments)
    } else {
      buildReplaceDataUpdateProjection(matchedRowsPlan, assignments)
    }

    // build a plan that contains unmatched rows in matched groups that must be copied over
    val remainingRowFilter = Not(EqualNullSafe(cond, Literal.TrueLiteral))
    val remainingRowsPlan = addOperationColumn(COPY_OPERATION,
      Filter(remainingRowFilter, readRelation))

    // the new state is a union of updated and copied over records
    val query = Union(updatedRowsPlan, remainingRowsPlan)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs,
      connectorDataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    ReplaceData(writeRelation, cond, query, relation, projections, groupFilterCond)
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

    val writeOp = If(cond, Literal(UPDATE_OPERATION), Literal(COPY_OPERATION))
    val operationCol = Alias(writeOp, OPERATION_COLUMN)()
    Project(operationCol +: updatedValues, plan)
  }

  /**
   * Variant of `buildReplaceDataUpdateProjection` for the `SupportsColumnUpdates` narrow-scan
   * path.
   * For narrow attributes, looks up assignments by `ExprId` via `AttributeMap` and passes through
   * carry-along data columns (partition refs, cond refs, non-identity RHS refs) that landed in the
   * narrow scan but weren't user-assigned.
   */
  private def buildNarrowReplaceDataUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = TrueLiteral): LogicalPlan = {

    val assignmentMap = AttributeMap(assignments.collect {
      case Assignment(key: Attribute, value) => key -> value
    })

    val updatedValues = plan.output.map { attr =>
      if (MetadataAttribute.isValid(attr.metadata)) {
        if (MetadataAttribute.isPreservedOnUpdate(attr)) {
          attr
        } else {
          val updatedValue = If(cond, Literal(null, attr.dataType), attr)
          Alias(updatedValue, attr.name)(explicitMetadata = Some(attr.metadata))
        }
      } else {
        assignmentMap.get(attr) match {
          case Some(assignedExpr) =>
            Alias(If(cond, assignedExpr, attr), attr.name)()
          case None =>
            // Column present in the narrow read relation but not assigned (partition ref,
            // condition ref, or RHS ref carried in by `computeNarrowReadAttrs`) -- pass
            // through so COPY-tagged rows keep their current value.
            attr
        }
      }
    }

    val writeOp = If(cond, Literal(UPDATE_OPERATION), Literal(COPY_OPERATION))
    val operationCol = Alias(writeOp, OPERATION_COLUMN)()
    Project(operationCol +: updatedValues, plan)
  }

  // build a rewrite plan for sources that support row deltas
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): WriteDelta = {

    val operation = operationTable.operation.asInstanceOf[SupportsDelta]

    // resolve all needed attrs (e.g. row ID, required metadata attrs, and any connector-declared
    // data attrs for column-update writes)
    val rowAttrs = relation.output
    val supportsColumnUpdate = operation.isInstanceOf[SupportsColumnUpdates]
    val connectorDataAttrs = resolveConnectorDataAttrs(relation, operation, assignments)

    val rowIdAttrs = resolveRowIdAttrs(relation, operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)

    if (supportsColumnUpdate && operation.representUpdateAsDeleteAndInsert) {
      validateNoRowIdReassignment(operation, relation, connectorDataAttrs, assignments, rowIdAttrs)
      validateRowIdDeclared(operation, connectorDataAttrs, rowIdAttrs)
    }

    val narrowDataAttrs = if (supportsColumnUpdate) {
      computeNarrowReadAttrs(relation, connectorDataAttrs, assignments, cond)
    } else {
      relation.output
    }

    val readRelation = if (supportsColumnUpdate) {
      buildNarrowRelationWithAttrs(relation, operationTable, narrowDataAttrs, metadataAttrs,
        rowIdAttrs)
    } else {
      buildRelationWithAttrs(relation, operationTable, metadataAttrs, rowIdAttrs)
    }

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val rowDeltaPlan = if (supportsColumnUpdate) {
      if (operation.representUpdateAsDeleteAndInsert) {
        buildNarrowDeletesAndInserts(matchedRowsPlan, assignments, rowIdAttrs)
      } else {
        buildColumnUpdateProjection(
          matchedRowsPlan, assignments, rowIdAttrs, metadataAttrs, connectorDataAttrs)
      }
    } else {
      if (operation.representUpdateAsDeleteAndInsert) {
        buildDeletesAndInserts(matchedRowsPlan, assignments, rowIdAttrs)
      } else {
        buildWriteDeltaUpdateProjection(matchedRowsPlan, assignments, rowIdAttrs)
      }
    }

    // build a plan to write the row delta to the table
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildWriteDeltaProjections(
      rowDeltaPlan, rowAttrs, rowIdAttrs, metadataAttrs, connectorDataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    WriteDelta(writeRelation, cond, rowDeltaPlan, relation, projections, groupFilterCond)
  }

  /**
   * Builds the WriteDelta projection for the column-update path.
   */
  private def buildColumnUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute],
      connectorDataAttrs: Seq[AttributeReference]): LogicalPlan = {

    val assignedValues = assignments.collect {
      case Assignment(key: Attribute, value) if !isIdentityAssignment(key, value) =>
        Alias(value, key.name)()
    }

    // Connector-required columns whose value isn't being changed by the UPDATE: pass through the
    // current value so the connector receives a complete write row. Row-ID columns are excluded
    // here even when also connector-required (e.g. a primary key used for both row lookup and
    // write payload); they are emitted once, below, via rowIdValues.
    val assignedKeyIds = collectUpdatedAttrs(assignments).map(_.exprId).toSet
    val rowIdAttrSet = AttributeSet(rowIdAttrs)
    val connectorPassThroughValues = connectorDataAttrs.filterNot(a =>
      assignedKeyIds.contains(a.exprId) || rowIdAttrSet.contains(a))

    val metadataAttrSet = AttributeSet(metadataAttrs)
    val metadataValues = plan.output.filter(metadataAttrSet.contains).map { attr =>
      if (MetadataAttribute.isPreservedOnUpdate(attr)) {
        attr
      } else {
        Alias(Literal(null, attr.dataType), attr.name)(explicitMetadata = Some(attr.metadata))
      }
    }

    val rowIdValues = plan.output.filter(rowIdAttrSet.contains)

    val originalRowIdValues = buildOriginalRowIdValues(rowIdAttrs, assignments)
    val operationType = Alias(Literal(UPDATE_OPERATION), OPERATION_COLUMN)()

    Project(
      Seq(operationType) ++ assignedValues ++ connectorPassThroughValues ++
        metadataValues ++ rowIdValues ++ originalRowIdValues,
      plan)
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

  /**
   * Variant of `buildDeletesAndInserts` for the `SupportsColumnUpdates` narrow-scan path.
   * This variant realigns the assignments to one value per surviving rowAttr padding unassigned
   * rowAttrs with identity, so the reinsert output arity matches the delete output arity in
   * the resulting Expand.
   */
  private def buildNarrowDeletesAndInserts(
      matchedRowsPlan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute]): Expand = {

    val (metadataAttrs, rowAttrs) = matchedRowsPlan.output.partition { attr =>
      MetadataAttribute.isValid(attr.metadata)
    }
    val assignmentMap = AttributeMap(assignments.collect {
      case a @ Assignment(key: Attribute, _) => key -> a
    })
    val reinsertAssignments = rowAttrs.map { attr =>
      assignmentMap.get(attr) match {
        case Some(a) => a
        case None => Assignment(attr, attr)
      }
    }
    val deleteOutput = deltaDeleteOutput(rowAttrs, rowIdAttrs, metadataAttrs)
    val insertOutput = deltaReinsertOutput(reinsertAssignments, metadataAttrs)
    val outputs = Seq(deleteOutput, insertOutput)
    val operationTypeAttr = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)()
    val attrs = operationTypeAttr +: matchedRowsPlan.output
    val expandOutput = generateExpandOutput(attrs, outputs)
    Expand(outputs, expandOutput, matchedRowsPlan)
  }

  /**
   * Resolves the connector's `requiredDataAttributes()` if the operation opts into column
   * updates, validating that every assigned column is covered. Returns `Nil` otherwise.
   */
  private def resolveConnectorDataAttrs(
      relation: DataSourceV2Relation,
      operation: RowLevelOperation,
      assignments: Seq[Assignment]): Seq[AttributeReference] = operation match {
    case scu: SupportsColumnUpdates =>
      val attrs = resolveRequiredDataAttrs(relation, scu)
      validateUpdatedColumnsSubset(scu, assignments, attrs)
      attrs
    case _ => Nil
  }

  /**
   * Computes the narrow set of data columns that must be present in the scan for a column-update
   * write: connector-declared attrs (`requiredDataAttributes()`), unioned with any table columns
   * referenced by non-identity assignment RHS expressions and the operation condition.
   */
  private def computeNarrowReadAttrs(
      relation: DataSourceV2Relation,
      connectorDataAttrs: Seq[AttributeReference],
      assignments: Seq[Assignment],
      cond: Expression): Seq[AttributeReference] = {
    val relationSet = relation.outputSet
    val nonIdentityRhsRefs = assignments.iterator
      .filterNot(a => a.key.isInstanceOf[Attribute] &&
        isIdentityAssignment(a.key.asInstanceOf[Attribute], a.value))
      .flatMap(_.value.references.toSeq)
      .toSeq
    val extraRefs = (cond.references.toSeq ++ nonIdentityRhsRefs)
      .collect { case a: AttributeReference => a }
      .filter(relationSet.contains)
    dedupAttrs(connectorDataAttrs ++ extraRefs)
  }

  /**
   * Enforces that every column being assigned (non-identity) is present in the connector-declared
   * `requiredDataAttributes()`. Comparison is at root-column granularity
   */
  private def validateUpdatedColumnsSubset(
      operation: RowLevelOperation,
      assignments: Seq[Assignment],
      connectorDataAttrs: Seq[AttributeReference]): Unit = {
    val declaredIds = connectorDataAttrs.map(_.exprId).toSet
    val missing = assignments.collect {
      case Assignment(key: AttributeReference, value)
          if !isIdentityAssignment(key, value) && !declaredIds.contains(key.exprId) =>
        key.name
    }.distinct
    if (missing.nonEmpty) {
      throw QueryCompilationErrors.requiredDataAttributesMissingUpdatedColumnsError(
        operation.getClass.getName, missing)
    }
  }

  /**
   * For connectors that opt into narrow column updates AND represent UPDATE as delete + insert,
   * reject reassignment of any row-ID column, unless `requiredDataAttributes()` covers every
   * column in the relation. In that case the REINSERT payload already is the full row with the
   * new row-ID value, and the DELETE half still carries the original row-ID via
   * `newLazyRowIdProjection`, so reassignment is safe. Otherwise the REINSERT path has no
   * row-ID channel to reconstruct columns outside `requiredDataAttributes()`.
   */
  private def validateNoRowIdReassignment(
      operation: RowLevelOperation,
      relation: DataSourceV2Relation,
      connectorDataAttrs: Seq[AttributeReference],
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute]): Unit = {
    val declaredIds = connectorDataAttrs.map(_.exprId).toSet
    if (relation.output.forall(a => declaredIds.contains(a.exprId))) {
      return
    }
    val rowIdAttrSet = AttributeSet(rowIdAttrs)
    val reassigned = assignments.collect {
      case Assignment(key: AttributeReference, value)
          if rowIdAttrSet.contains(key) && !isIdentityAssignment(key, value) =>
        key.name
    }.distinct
    if (reassigned.nonEmpty) {
      throw QueryCompilationErrors.splitUpdateRowIdReassignmentError(
        operation.getClass.getName, reassigned)
    }
  }

  /**
   * For connectors that opt into narrow column updates AND represent UPDATE as delete + insert,
   * requires every row-ID column to be declared in `requiredDataAttributes()`, unless it is a
   * metadata column: metadata columns are preserved into the REINSERT row independently (see
   * `deltaReinsertOutput`/`MetadataAttribute.isPreservedOnReinsert`), so they don't need to be
   * declared as data attrs. Otherwise the REINSERT payload is narrow and only contains
   * `requiredDataAttributes()`, so an undeclared data-column row-ID column would leave the
   * reinserted row with no identity for the connector to place it by.
   */
  private def validateRowIdDeclared(
      operation: RowLevelOperation,
      connectorDataAttrs: Seq[AttributeReference],
      rowIdAttrs: Seq[Attribute]): Unit = {
    val declaredIds = connectorDataAttrs.map(_.exprId).toSet
    val undeclared = rowIdAttrs
      .filterNot(a => MetadataAttribute.isValid(a.metadata) || declaredIds.contains(a.exprId))
      .map(_.name).distinct
    if (undeclared.nonEmpty) {
      throw QueryCompilationErrors.splitUpdateRowIdNotDeclaredError(
        operation.getClass.getName, undeclared)
    }
  }
}
