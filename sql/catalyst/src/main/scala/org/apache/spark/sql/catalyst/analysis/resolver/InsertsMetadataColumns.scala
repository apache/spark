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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.{HashMap, HashSet}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  ExposesMetadataColumns,
  Join,
  LeafNode,
  LogicalPlan,
  Project,
  SubqueryAlias,
  UnaryNode
}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf

/**
 * Mixed into [[Resolver]] to provide metadata column insertion into the plan tree. Called from
 * [[Resolver.insertReferencedMetadataColumnsForOperator]] after each operator resolves.
 *
 * Metadata columns (e.g. `_metadata`) originate from leaf operators like [[LogicalRelation]] or
 * [[DataSourceV2Relation]] and are stored in the [[NameScope]]'s hidden output, identified by the
 * `__metadata_col` attribute marker ([[Attribute.isMetadataCol]]). When an operator's expressions
 * reference a metadata column, this trait inserts the column into the plan tree below by
 * propagating it downward through intermediate operators until reaching the leaf node that owns it.
 *
 * The propagation descends through passthrough operators (e.g. [[Filter]], [[Sort]]) and [[Join]]
 * nodes, extends [[Project]] expression lists as needed, and calls
 * [[ExposesMetadataColumns.withMetadataColumns]] on leaf nodes. [[Aggregate]] is a propagation
 * barrier, matching the legacy analyzer where `Aggregate.metadataOutput` is `Nil`. For [[Join]],
 * metadata columns are sent to both children - only the child whose leaf owns the metadata
 * column's [[ExprId]] will actually insert it; the other child's subtree is a no-op.
 *
 * {{{
 * -- Project: _metadata resolved from hidden output, inserted into LogicalRelation output.
 * SELECT _metadata FROM nation
 *
 * -- Filter: _metadata.file_size resolved from hidden output in WHERE condition.
 * SELECT n_nationkey FROM nation WHERE _metadata.file_size > 0
 *
 * -- Aggregate: _metadata referenced in grouping expression.
 * SELECT count(*) AS cnt FROM nation GROUP BY _metadata.file_block_start
 *
 * -- Join: _metadata propagated to both children, only the owning leaf inserts it.
 * SELECT a._metadata FROM nation a JOIN region b ON a.n_regionkey = b.r_regionkey
 * }}}
 */
trait InsertsMetadataColumns extends SQLConfHelper {

  /**
   * Checks if the current operator's expression resolution referenced any metadata columns, and
   * if so, inserts them into the resolved plan tree by propagating down to the leaf nodes.
   *
   * Following the legacy [[AddMetadataColumns]] rule: after propagating metadata columns into the
   * children (which adds them to leaf outputs), if the operator's output schema changed (e.g. a
   * [[Filter]] whose child now exposes `_metadata`), we wrap the operator with a [[Project]] that
   * projects only the original output columns. This preserves the operator's output schema while
   * making the metadata columns available internally.
   *
   * `referencedMetadataColumns` is populated during expression resolution by cross-referencing
   * referenced attributes with [[NameScope.hiddenOutput]]. This avoids reading from
   * [[NameScope.hiddenOutput]] directly, which may have already been stripped by operator resolvers
   * that call [[NameScopeStack.overwriteOutputAndExtendHiddenOutput]] with
   * `stripMetadataFromHiddenOutput = true`. `currentOutput` is the operator's output that the
   * wrapping [[Project]] must preserve. `isTopLevel` is `true` when the operator is the plan's
   * top-level operator (carries [[ResolverTag.TOP_LEVEL_OPERATOR]] on its unresolved node); when
   * so, the wrapping [[Project]] preserves only the original output, without exposing the metadata
   * columns for a (nonexistent) parent to reference.
   */
  protected def insertReferencedMetadataColumnsForOperator(
      resolvedPlan: LogicalPlan,
      referencedMetadataColumns: HashMap[ExprId, Attribute],
      currentOutput: Seq[Attribute],
      isTopLevel: Boolean): LogicalPlan = {
    if (referencedMetadataColumns.isEmpty) {
      resolvedPlan
    } else {
      val metadataColumns = referencedMetadataColumns.values().asScala.toSeq

      val updatedChildren = resolvedPlan.children.map { child =>
        val (updatedChild, _) = insertReferencedMetadataColumnsIntoChild(
          operator = child,
          metadataColumns = metadataColumns
        )
        updatedChild
      }

      val updatedPlan = resolvedPlan.withNewChildren(updatedChildren)
      if (updatedPlan eq resolvedPlan) {
        resolvedPlan
      } else {
        resolvedPlan match {
          case _: Project | _: Aggregate =>
            updatedPlan
          case _ =>
            val metadataColumnsToPreserve =
              if (isTopLevel) {
                Seq.empty
              } else {
                metadataColumns.map(_.markAsQualifiedAccessOnly())
              }
            val wrappingProject = Project(currentOutput ++ metadataColumnsToPreserve, updatedPlan)
            if (metadataColumnsToPreserve.nonEmpty) {
              wrappingProject.setTagValue(
                InsertsMetadataColumns.PRESERVED_METADATA_COLUMN_IDS,
                metadataColumnsToPreserve.map(_.exprId).toSet
              )
            }
            wrappingProject
        }
      }
    }
  }

  /**
   * Recursively insert referenced metadata columns into the operator tree. Returns the modified
   * plan and the subset of requested columns that were actually accepted by
   * [[ExposesMetadataColumns]] leaf nodes. This allows parent operators to only add columns to
   * their expression lists that are actually available from the subtree below, which is essential
   * when metadata columns are sent to both sides of a [[Join]] -- only the side whose leaf owns
   * the column will report it as accepted.
   *
   * For [[Project]], recursion always proceeds with the full
   * `metadataColumns` to ensure the leaf's [[ExposesMetadataColumns.withMetadataColumns]] is
   * called even when the metadata column is already present in the operator's expression list
   * (e.g. `SELECT _metadata FROM t`). The filtering via [[filterColumnsNotInExpressionList]] is
   * applied after recursion to decide what to *add* to the expression list, not whether to recurse.
   */
  protected def insertReferencedMetadataColumnsIntoChild(
      operator: LogicalPlan,
      metadataColumns: Seq[Attribute]): (LogicalPlan, Seq[Attribute]) = {
    operator match {
      case project: Project =>
        failIfReferencingPreservedMetadataColumns(project, metadataColumns)

        insertMetadataColumnsIntoOutputList(
          operator = project,
          expressionList = project.projectList,
          metadataColumns = metadataColumns,
          buildOperator = (newProjectList, newChild) =>
            project.copy(projectList = newProjectList, child = newChild)
        )
      case aggregate: Aggregate =>
        (aggregate, Seq.empty)
      case exposesMetadata: ExposesMetadataColumns =>
        insertMetadataColumnsIntoLeaf(exposesMetadata, metadataColumns)
      case join: Join =>
        insertMetadataColumnsIntoJoin(join, metadataColumns)
      case unaryNode: UnaryNode =>
        insertMetadataColumnsIntoUnaryNode(unaryNode, metadataColumns)
      case _ =>
        (operator, Seq.empty)
    }
  }

  /**
   * Inserts accepted metadata columns into the expression list of a [[UnaryNode]] that defines
   * its output through one (e.g. [[Project]]). The metadata columns are first
   * propagated into the child subtree; the columns accepted by the leaves below are then added
   * to the expression list via `buildOperator`, unless already present.
   */
  private def insertMetadataColumnsIntoOutputList(
      operator: UnaryNode,
      expressionList: Seq[NamedExpression],
      metadataColumns: Seq[Attribute],
      buildOperator: (Seq[NamedExpression], LogicalPlan) => UnaryNode)
    : (LogicalPlan, Seq[Attribute]) = {
    val (expandedChild, acceptedColumns) = insertReferencedMetadataColumnsIntoChild(
      operator = operator.child,
      metadataColumns = metadataColumns
    )
    if (acceptedColumns.nonEmpty) {
      val columnsToAdd =
        filterColumnsNotInExpressionList(expressionList, acceptedColumns)
      val newExpressionList = if (columnsToAdd.nonEmpty) {
        val (qualifiedAccessOnlyColumns, regularColumns) =
          expressionList.partition(_.toAttribute.qualifiedAccessOnly)
        regularColumns ++ columnsToAdd.map(_.toAttribute.markAsAllowAnyAccess()) ++
          qualifiedAccessOnlyColumns
      } else {
        expressionList
      }
      val newOperator = buildOperator(newExpressionList, expandedChild)
      newOperator.copyTagsFrom(operator)
      (newOperator, acceptedColumns)
    } else {
      (operator, Seq.empty)
    }
  }

  /**
   * Fails if `metadataColumns` referenced by an operator reach a [[Project]] that wraps a lower
   * operator which references the same metadata columns (see
   * [[insertReferencedMetadataColumnsForOperator]]). For example:
   * {{{
   * SELECT _metadata.file_name FROM t WHERE _metadata.row_index < 100;
   * }}}
   * The fixed-point Analyzer adds such a [[Project]] only if the lower operator is resolved in an
   * earlier iteration than the operator above it (e.g. because its expressions need type
   * coercion), which single-pass cannot replicate.
   */
  private def failIfReferencingPreservedMetadataColumns(
      project: Project,
      metadataColumns: Seq[Attribute]): Unit = {
    project.getTagValue(InsertsMetadataColumns.PRESERVED_METADATA_COLUMN_IDS).foreach {
      preservedMetadataColumnIds =>
        if (metadataColumns.exists(column => preservedMetadataColumnIds.contains(column.exprId))) {
          throw new ExplicitlyUnsupportedResolverFeature(
            ResolutionGap.MetadataColumnsReferencedByNestedOperators
          )
        }
    }
  }

  private def insertMetadataColumnsIntoLeaf(
      exposesMetadata: ExposesMetadataColumns,
      metadataColumns: Seq[Attribute]): (LogicalPlan, Seq[Attribute]) = {
    val matchingColumns = metadataColumns.filter(column =>
      exposesMetadata.metadataOutput.exists(_.exprId == column.exprId)
    )
    if (matchingColumns.nonEmpty) {
      val expandedLeaf = exposesMetadata.withMetadataColumns()
      (expandedLeaf, matchingColumns)
    } else {
      (exposesMetadata, Seq.empty)
    }
  }

  private def insertMetadataColumnsIntoJoin(
      join: Join,
      metadataColumns: Seq[Attribute]): (LogicalPlan, Seq[Attribute]) = {
    val (updatedLeft, leftAccepted) = insertReferencedMetadataColumnsIntoChild(
      operator = join.left,
      metadataColumns = metadataColumns
    )
    val (updatedRight, rightAccepted) = insertReferencedMetadataColumnsIntoChild(
      operator = join.right,
      metadataColumns = metadataColumns
    )
    (join.withNewChildren(Seq(updatedLeft, updatedRight)), leftAccepted ++ rightAccepted)
  }

  private def insertMetadataColumnsIntoUnaryNode(
      unaryNode: UnaryNode,
      metadataColumns: Seq[Attribute]): (LogicalPlan, Seq[Attribute]) = {
    val (updatedChild, acceptedColumns) = insertReferencedMetadataColumnsIntoChild(
      operator = unaryNode.child,
      metadataColumns = metadataColumns
    )
    (unaryNode.withNewChildren(Seq(updatedChild)), acceptedColumns)
  }

  /**
   * Computes metadata columns for [[SubqueryAlias]]'s hidden output, following the behavior of
   * [[SubqueryAlias.metadataOutput]]. Metadata columns are propagated from the child if the child
   * is a [[LeafNode]] or [[SubqueryAlias]], or if
   * [[SQLConf.SUBQUERY_ALIAS_ALWAYS_PROPAGATE_METADATA_COLUMNS]] is enabled. Propagated columns
   * have their qualifier updated and only non-qualified-access-only columns are included.
   */
  protected def computeMetadataHiddenOutputForSubqueryAlias(
      resolvedChild: LogicalPlan,
      qualifier: Seq[String],
      hiddenOutput: Seq[Attribute]): Seq[Attribute] = {
    val canPropagate = conf.getConf(SQLConf.SUBQUERY_ALIAS_ALWAYS_PROPAGATE_METADATA_COLUMNS) ||
      (resolvedChild match {
        case _: LeafNode | _: SubqueryAlias => true
        case _ => false
      })
    if (canPropagate) {
      hiddenOutput
        .filter(attribute => attribute.isMetadataCol && !attribute.qualifiedAccessOnly)
        .map(_.withQualifier(qualifier))
    } else {
      Seq.empty
    }
  }

  private def filterColumnsNotInExpressionList(
      expressionList: Seq[NamedExpression],
      columns: Seq[Attribute]): Seq[Attribute] = {
    val expressionIds = new HashSet[ExprId](expressionList.size)
    expressionList.foreach(expression => expressionIds.add(expression.exprId))
    columns.filter(column => !expressionIds.contains(column.exprId))
  }
}

object InsertsMetadataColumns {

  /**
   * Tags a [[Project]] added by `insertReferencedMetadataColumnsForOperator` with the IDs of the
   * metadata columns it preserves for the operators above.
   */
  val PRESERVED_METADATA_COLUMN_IDS = TreeNodeTag[Set[ExprId]]("preserved_metadata_column_ids")
}
