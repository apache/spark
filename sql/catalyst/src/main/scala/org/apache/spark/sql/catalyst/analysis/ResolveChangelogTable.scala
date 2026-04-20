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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogInfo}
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.types.IntegerType

/**
 * Post-processes a resolved [[DataSourceV2Relation]] backed by a [[ChangelogTable]] to inject
 * carry-over removal and/or update detection plans.
 *
 * Fires after [[ResolveRelations]] has wrapped the connector's [[Changelog]] in a
 * [[DataSourceV2Relation]].
 *
 * Transformation order: carry-over removal BEFORE update detection.
 */
object ResolveChangelogTable extends Rule[LogicalPlan] {

  private val CHANGELOG_TRANSFORMED_TAG =
    TreeNodeTag[Boolean]("changelog_transformed")

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (isAlreadyTransformed(plan)) return plan
    var updatedPlan = plan
    updatedPlan = plan.resolveOperatorsUp {
      case rel @ DataSourceV2Relation(table: ChangelogTable, _, _, _, _, _) =>
        val changelog = table.changelog
        val options = table.changelogInfo
        var updatedRel: LogicalPlan = rel
        // Transformations in order: 1. carry-over removal, 2. update detection, 3. net changes.
        if (options.deduplicationMode() != ChangelogInfo.DeduplicationMode.NONE &&
            changelog.containsCarryoverRows()) {
          updatedRel = injectCarryoverRemoval(rel, changelog)
        }
        if (options.computeUpdates() && changelog.representsUpdateAsDeleteAndInsert()) {
          updatedRel = injectUpdateDetection(updatedRel, changelog)
        }
        if (options.deduplicationMode() == ChangelogInfo.DeduplicationMode.NET_CHANGES &&
            changelog.containsIntermediateChanges()) {
          updatedRel = injectNetChangeComputation(updatedRel, changelog)
        }
        updatedRel
    }
    if (updatedPlan ne plan) {
      updatedPlan.setTagValue(CHANGELOG_TRANSFORMED_TAG, true)
    }
    updatedPlan
  }

  // ---------------------------------------------------------------------------
  // Guard
  // ---------------------------------------------------------------------------

  /**
   * Returns true if this plan has already been transformed by this rule.
   * Uses a plan-level tag to prevent re-processing on subsequent rule executor iterations.
   */
  private def isAlreadyTransformed(plan: LogicalPlan): Boolean = {
    plan.getTagValue(CHANGELOG_TRANSFORMED_TAG).getOrElse(false)
  }

  // ---------------------------------------------------------------------------
  // Update Detection (window-based)
  // ---------------------------------------------------------------------------

  /**
   * Injects a window-based update detection plan above the given plan.
   *
   * Adds a Window node partitioned by (rowId, rowVersion) that counts deletes and inserts
   * per partition, then rewrites _change_type:
   *   - insert becomes update_postimage (when both delete and insert exist in same partition)
   *   - delete becomes update_preimage  (when both delete and insert exist in same partition)
   *   - otherwise keeps original _change_type
   *
   * Plan shape:
   *   Project (drop _ins_cnt, _del_cnt)
   *    |-- Project (rewrite _change_type via CASE WHEN)
   *         |-- Window (partition by rowId+rowVersion, compute _del_cnt/_ins_cnt)
   *              |-- [input plan]
   *
   * @param plan the input plan (DataSourceV2Relation or already-transformed plan)
   * @param cl the Changelog providing rowId() and rowVersion()
   * @return plan with update detection injected
   */
  private def injectUpdateDetection(
      plan: LogicalPlan,
      cl: Changelog): LogicalPlan = {
    val changeTypeAttr = plan.output.find(_.name == "_change_type").get
    val rowIdExprs = V2ExpressionUtils.resolveRefs[NamedExpression](cl.rowId().toSeq, plan)
    val rowVersionExpr = V2ExpressionUtils.resolveRef[NamedExpression](cl.rowVersion(), plan)
    // Layer 1: Window with _del_cnt, _ins_cnt
    val insertIf = If(
      EqualTo(changeTypeAttr, Literal("insert")), Literal(1), Literal(null, IntegerType))
    val deleteIf = If(
      EqualTo(changeTypeAttr, Literal("delete")), Literal(1), Literal(null, IntegerType))

    val partitionByCols = rowIdExprs ++ Seq(rowVersionExpr)
    val windowSpec = WindowSpecDefinition(partitionByCols, Nil, UnspecifiedFrame)

    val insCntAlias = Alias(
      WindowExpression(Count(Seq(insertIf)).toAggregateExpression(), windowSpec), "_ins_cnt")()
    val delCntAlias = Alias(
      WindowExpression(Count(Seq(deleteIf)).toAggregateExpression(), windowSpec), "_del_cnt")()

    val windowNode = Window(
      plan.output ++ Seq(delCntAlias, insCntAlias), partitionByCols, Nil, plan)

    // Layer 2: Project rewriting _change_type (preserve exprId!)
    val insCntRef = insCntAlias.toAttribute
    val delCntRef = delCntAlias.toAttribute

    val isUpdate = And(
      GreaterThanOrEqual(delCntRef, Literal(1L)),
      GreaterThanOrEqual(insCntRef, Literal(1L))
    )

    val updateType = If(
      EqualTo(changeTypeAttr, Literal("insert")),
      Literal("update_postimage"),
      Literal("update_preimage")
    )

    val caseExpr = CaseWhen(Seq((isUpdate, updateType)), changeTypeAttr)
    val newChangeTypeAlias = Alias(caseExpr, "_change_type")()
    val projectList = windowNode.output.map { attr =>
      if (attr.name == "_change_type") newChangeTypeAlias else attr
    }
    val layer2 = Project(projectList, windowNode)

    // Layer 3: Project dropping _del_cnt, _ins_cnt
    val layer3 = Project(
      layer2.output.filter(a => a.name != "_ins_cnt" && a.name != "_del_cnt"), layer2)
    layer3
  }

  // ---------------------------------------------------------------------------
  // Carry-Over Removal (sort-based)
  // ---------------------------------------------------------------------------

  /**
   * Injects carry-over removal above the given plan.
   *
   * Adds Repartition + Sort + CarryOverRemoval that compares consecutive delete+insert
   * pairs field-by-field and drops identical pairs.
   *
   * Plan shape:
   *   CarryOverRemoval (custom logical node; CarryOverRemovalExec physical node)
   *    |-- Sort [rowId ASC, rowVersion ASC, _change_type ASC]
   *         |-- RepartitionByExpression (rowId, rowVersion)
   *              |-- [input plan]
   *
   * @param plan the input plan
   * @param cl the Changelog providing rowId() and rowVersion()
   * @return plan with carry-over removal injected
   */
  private def injectCarryoverRemoval(
      plan: LogicalPlan,
      cl: Changelog): LogicalPlan = {
    // 1. Resolve rowId/rowVersion attributes
    val changeTypeAttr = plan.output.find(_.name == "_change_type").get
    val rowIdExprs =
      V2ExpressionUtils.resolveRefs[NamedExpression](cl.rowId().toSeq, plan)
    val rowVersionExpr =
      V2ExpressionUtils.resolveRef[NamedExpression](cl.rowVersion(), plan)
    // 2. RepartitionByExpression
    val partitionExpr = rowIdExprs ++ Seq(rowVersionExpr)
    val repartitioned = RepartitionByExpression(partitionExpr, plan, None)
    // 3. Sort (rowId, rowVersion, _change_type ASC; "delete" sorts before "insert")
    val sortOrder = (rowIdExprs ++ Seq(rowVersionExpr, changeTypeAttr))
      .map(e => SortOrder(e, Ascending))
    val sorted = Sort(sortOrder, global = false, repartitioned)
    // 4. Build CarryOverRemoval with column NAMES, not ordinals.
    //
    // TODO(review discussion): We pass column names and resolve them to ordinals at execution
    // time in CarryOverRemovalExec.doExecute(). Ordinals computed at analysis time were tried
    // first but broke: ColumnPruning / projection rewrites between analysis and physical
    // planning can reorder or re-number columns, so analysis-time ordinals pointed at the
    // wrong fields by the time the exec node ran. Resolving names against child.output at
    // execute time dodges that. Flagging this as a discussion point. @Johan: is
    // name-resolution the right idiom here, or is there a cleaner way to pin ordinals
    // through the optimizer?
    //
    // TODO(Sandro): Support nested rowId paths (e.g. Seq("payload", "id")). Spark's
    // NamedReference API admits them, and prior commits on this branch (see git log before
    // this commit) implemented struct-field extraction in CarryOverRemovalExec plus a
    // "keep the parent struct in data comparison" special case in the dataColumnNames
    // computation below. Restore from that history when the first real connector needs it.
    // The top-level-only restriction is enforced in ChangelogTable.validateSchema, so here
    // we can take `fieldNames()(0)` directly.
    val rowIdColumnNames = cl.rowId().map(_.fieldNames()(0)).toSeq
    val rowVersionColumnName = cl.rowVersion().fieldNames()(0)  // e.g. "_commit_version"

    val metadataNames = Set("_change_type", "_commit_version", "_commit_timestamp")
    val excluded = metadataNames ++ rowIdColumnNames.toSet
    val dataColumnNames = plan.output.map(_.name).filterNot(excluded.contains)

    // TODO(review discussion): pinning every relation attribute via requiredAttributes is a
    // blunt instrument: it disables ColumnPruning for the entire subtree rooted at
    // CarryOverRemoval. @Johan, is there a more surgical way to keep just the data columns
    // alive through the optimizer (e.g. a dedicated trait, or marking the logical node as
    // requires-all-output)? For now this is defensive.
    val requiredAttrs = plan.output

    CarryOverRemoval(sorted, rowIdColumnNames, rowVersionColumnName, dataColumnNames, requiredAttrs)
  }

  // ---------------------------------------------------------------------------
  // Net Change Computation TODO
  // ---------------------------------------------------------------------------

  /**
   * Injects net change computation above the given plan.
   * Collapses multiple changes per row identity into the net effect.
   */
  private def injectNetChangeComputation(
      plan: LogicalPlan,
      cl: Changelog): LogicalPlan = {
    throw new UnsupportedOperationException(
      "Net change computation is not yet supported")
  }
}
