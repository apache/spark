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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, EqualNullSafe, Exists, Expression, Literal, MetadataAttribute, NamedExpression, OuterReference}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, CTERelationDef, CTERelationRef, Filter, Join, JoinHint, LogicalPlan, Project, ReplaceData, ReplaceUsingTable, Union, WithCTE, WriteDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{COPY_OPERATION, INSERT_OPERATION, OPERATION_COLUMN}
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperationTable, SupportsDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.REPLACE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.IntegerType

/**
 * Rewrites `INSERT INTO ... REPLACE USING (cols) <query>` (a [[ReplaceUsingTable]]) onto the
 * row-level write machinery via a [[RowLevelOperation.Command.REPLACE]] operation.
 *
 * Scoped replace deletes every target row whose scope-column tuple appears in the source and
 * appends all source rows, including duplicates that share a scope tuple. The replacement state
 * is computed from independent target and source branches so that source rows are inserted
 * regardless of whether their scope tuple already exists in the target.
 *
 * The source is shared through a CTE so the delete/carryover join, the insert branch, and the
 * runtime group filter all refer to the same analyzed source plan. A non-deterministic source is
 * rejected because it could otherwise delete based on a different source result than it inserts.
 */
object RewriteReplaceUsing extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case r @ ReplaceUsingTable(aliasedTable, scopeColumns, source)
        if r.resolved && source.resolved =>

      // The source feeds the delete decision, insert payload, and runtime group filter.
      // Evaluating a non-deterministic source in those independent plan locations could produce
      // mutually inconsistent results.
      if (!source.deterministic) {
        throw QueryCompilationErrors.insertReplaceUsingNonDeterministicSource()
      }

      EliminateSubqueryAliases(aliasedTable) match {
        case rel: DataSourceV2Relation =>
          rel.table match {
            case tbl: SupportsRowLevelOperations =>
              val operationTable = buildOperationTable(tbl, REPLACE, rel.options)
              val scopeOrdinals = resolveScopeOrdinals(rel, scopeColumns)
              operationTable.operation match {
                case _: SupportsDelta =>
                  buildWriteDeltaPlan(rel, operationTable, scopeOrdinals, source)
                case _ =>
                  buildReplaceDataPlan(rel, operationTable, scopeOrdinals, source)
              }
            case _ =>
              throw QueryCompilationErrors.unsupportedInsertReplaceOnOrUsing(rel.table.name())
          }

        case other =>
          throw QueryCompilationErrors.unsupportedInsertReplaceOnOrUsing(
            other.simpleString(maxFields = 2))
      }
  }

  // Build a copy-on-write replacement from retained target rows and inserted source rows.
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      scopeOrdinals: Seq[Int],
      source: LogicalPlan): LogicalPlan = {

    val rowAttrs = relation.output
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    val sourceCte = CTERelationDef(source)
    val scopeRef = newSourceRef(sourceCte)
    val insertRef = newSourceRef(sourceCte)
    val filterRef = newSourceRef(sourceCte)

    // COW rewrites whole groups, so target rows whose scope tuple is absent from the source must
    // be re-emitted as COPY to preserve them (and their metadata).
    val antiJoinCond = scopeEquality(readRelation.output, scopeRef.output, scopeOrdinals)
    val carryoverJoin = Join(readRelation, scopeRef, LeftAnti, Some(antiJoinCond), JoinHint.NONE)
    val carryover = addOperationColumn(COPY_OPERATION, carryoverJoin)

    // All source rows are inserted. Carryover supplies target metadata; insert rows null it out.
    val insertData = rowAttrs.indices.map { i =>
      Alias(insertRef.output(i), rowAttrs(i).name)()
    }
    val insertMetadata = metadataAttrs.map(attr => Alias(Literal(null, attr.dataType), attr.name)())
    val insertOutput = operationAlias(INSERT_OPERATION) +: (insertData ++ insertMetadata)
    val inserts = Project(insertOutput, insertRef)

    val replacementQuery = Union(carryover :: inserts :: Nil)
    val projections = buildReplaceDataProjections(replacementQuery, rowAttrs, metadataAttrs)

    val groupFilterCond = if (groupFilterEnabled) {
      Some(scopeExists(relation, filterRef, scopeOrdinals))
    } else {
      None
    }

    // The replaced scope is source-defined, so there is no static target-only predicate to push
    // down during planning. File pruning flows through groupFilterCond, which
    // RowLevelOperationRuntimeGroupFiltering turns into a dynamic subquery.
    val writeRelation = relation.copy(table = operationTable)
    val replaceData =
      ReplaceData(writeRelation, TrueLiteral, replacementQuery, relation, projections,
        groupFilterCond)
    WithCTE(replaceData, sourceCte :: Nil)
  }

  // Build a merge-on-read delta from matched-row deletes and inserted source rows.
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      scopeOrdinals: Seq[Int],
      source: LogicalPlan): LogicalPlan = {

    val operation = operationTable.operation.asInstanceOf[SupportsDelta]
    val rowAttrs = relation.output
    val rowIdAttrs = resolveRowIdAttrs(relation, operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs, rowIdAttrs)

    // Keep row columns before metadata columns, matching the schema shape expected by
    // buildWriteDeltaProjections. Row ID attributes are metadata-classified and land in metaPart.
    val (metaPart, rowPart) = readRelation.output.partition { attr =>
      MetadataAttribute.isValid(attr.metadata)
    }
    val branchNames = OPERATION_COLUMN +: (rowPart ++ metaPart).map(_.name)

    val sourceCte = CTERelationDef(source)
    val scopeRef = newSourceRef(sourceCte)
    val insertRef = newSourceRef(sourceCte)
    val filterRef = newSourceRef(sourceCte)

    // Matched target rows are encoded as deletes: row ID and metadata are retained, row columns
    // are nulled out.
    val semiJoinCond = scopeEquality(readRelation.output, scopeRef.output, scopeOrdinals)
    val matchingRows = Join(readRelation, scopeRef, LeftSemi, Some(semiJoinCond), JoinHint.NONE)
    val deleteOutput = deltaDeleteOutput(rowPart, rowIdAttrs, metaPart)
    val deletes = Project(named(deleteOutput, branchNames), matchingRows)

    // Every source row is inserted; row IDs and metadata are null for new rows.
    val relationValueByExprId =
      relation.output.zipWithIndex.map {
        case (attr, i) => attr.exprId -> insertRef.output(i)
      }.toMap
    val insertAssignments = rowPart.map { attr =>
      val value = relationValueByExprId.getOrElse(attr.exprId, Literal(null, attr.dataType))
      Assignment(attr, value)
    }
    val insertOutput = deltaInsertOutput(insertAssignments, metaPart)
    val inserts = Project(named(insertOutput, branchNames), insertRef)

    val deltaQuery = Union(deletes :: inserts :: Nil)
    val projections = buildWriteDeltaProjections(deltaQuery, rowAttrs, rowIdAttrs, metadataAttrs)

    val groupFilterCond = if (groupFilterEnabled) {
      Some(scopeExists(relation, filterRef, scopeOrdinals))
    } else {
      None
    }

    val writeRelation = relation.copy(table = operationTable)
    val writeDelta =
      WriteDelta(writeRelation, TrueLiteral, deltaQuery, relation, projections, groupFilterCond)
    WithCTE(writeDelta, sourceCte :: Nil)
  }

  private def operationAlias(operation: Int): NamedExpression = {
    Alias(Literal(operation, IntegerType), OPERATION_COLUMN)()
  }

  private def named(exprs: Seq[Expression], names: Seq[String]): Seq[NamedExpression] = {
    exprs.zip(names).map {
      case (ne: NamedExpression, name) if ne.name == name => ne
      case (e, name) => Alias(e, name)()
    }
  }

  private def resolveScopeOrdinals(
      relation: DataSourceV2Relation,
      scopeColumns: Seq[String]): Seq[Int] = {

    val seen = scala.collection.mutable.HashSet.empty[Int]
    scopeColumns.map { name =>
      val resolved = relation.resolve(Seq(name), conf.resolver).getOrElse {
        throw QueryCompilationErrors.unresolvedColumnError(name, relation.output.map(_.name))
      }
      val ordinal = relation.output.indexWhere(_.exprId == resolved.toAttribute.exprId)
      if (ordinal < 0) {
        throw QueryCompilationErrors.unresolvedColumnError(name, relation.output.map(_.name))
      }
      if (!seen.add(ordinal)) {
        throw QueryCompilationErrors.insertReplaceUsingDuplicateScopeColumn(name)
      }
      ordinal
    }
  }

  private def newSourceRef(sourceDef: CTERelationDef): CTERelationRef = {
    val ref = CTERelationRef(
      sourceDef.id,
      _resolved = true,
      sourceDef.child.output,
      sourceDef.child.isStreaming)
    ref.newInstance().asInstanceOf[CTERelationRef]
  }

  private def scopeEquality(
      targetOutput: Seq[Attribute],
      sourceOutput: Seq[Attribute],
      scopeOrdinals: Seq[Int]): Expression = {
    scopeOrdinals
      .map(ordinal => EqualNullSafe(targetOutput(ordinal), sourceOutput(ordinal)): Expression)
      .reduce(And)
  }

  // A correlated EXISTS over the source on scope equality, used as the runtime group filter.
  private def scopeExists(
      relation: DataSourceV2Relation,
      sourceRef: CTERelationRef,
      scopeOrdinals: Seq[Int]): Expression = {
    val cond = scopeOrdinals
      .map { ordinal =>
        EqualNullSafe(OuterReference(relation.output(ordinal)), sourceRef.output(ordinal))
          : Expression
      }
      .reduce(And)
    val outerRefs = scopeOrdinals.map(ordinal => relation.output(ordinal))
    Exists(Filter(cond, sourceRef), outerRefs)
  }
}
