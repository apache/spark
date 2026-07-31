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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.DataFrame

/**
 * Exposes an API to execute one SCD Type 2 AutoCDC microbatch reconciliation on a
 * foreachBatch streaming query.
 */
case class Scd2ForeachBatchHandler(
    batchProcessor: Scd2BatchProcessor,
    auxiliaryTableIdentifier: TableIdentifier,
    targetTableIdentifier: TableIdentifier) {

  /**
   * Process a single CDC microbatch and merge it into the auxiliary and target tables.
   *
   * Idempotent under same-`batchId` replay.
   */
  def execute(batchDf: DataFrame, batchId: Long): Unit = {
    val reconciled = reconcileMicrobatch(batchDf, batchId)

    batchProcessor.mergeRowsIntoAuxiliaryTable(
      reconciledDfWithAuxRowsTagged = reconciled.reconciledAndRoutedDf,
      originalAffectedRowsFromAuxiliaryTable = reconciled.affectedRowsFromAuxiliaryTable,
      auxiliaryTableIdentifier = auxiliaryTableIdentifier,
      batchId = batchId
    )

    batchProcessor.mergeRowsIntoTargetTable(
      reconciledDfWithAuxRowsTagged = reconciled.reconciledAndRoutedDf,
      affectedRowsFromTargetTable = reconciled.affectedRowsFromTargetTable,
      targetTableIdentifier = targetTableIdentifier
    )
  }

  /**
   * Validate and reconcile a single CDC microbatch against the current auxiliary- and target-table
   * state, producing the tagged post-reconciliation rows plus the affected-row sets the two merges
   * consume. Performs no writes: this is the entire pipeline up to (but excluding) the aux/target
   * merges, factored out of [[execute]] so that both [[execute]] and tests exercise the exact same
   * transform chain (they cannot silently desynchronize).
   */
  private[autocdc] def reconcileMicrobatch(
      batchDf: DataFrame,
      batchId: Long): Scd2ReconciliationResult = {
    ScdBatchValidator(
      destinationIdentifier = targetTableIdentifier,
      changeArgs = batchProcessor.changeArgs,
      batchDf = batchDf,
      batchId = batchId
    ).validateMicrobatch()

    val preprocessedBatchDf = batchProcessor.preprocessMicrobatch(batchDf)

    val perKeyMinimumSequenceInMicrobatchDf = batchProcessor.computeMinimumSequencePerKey(
      preprocessedBatchDf
    )

    val auxTableDf = batchDf.sparkSession.read.table(auxiliaryTableIdentifier.quotedString)
    val affectedRowsFromAuxiliaryTable = batchProcessor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = auxTableDf,
      perKeyMinimumSequenceInMicrobatchDf = perKeyMinimumSequenceInMicrobatchDf,
      batchId = batchId
    )

    val targetTableDf = batchDf.sparkSession.read.table(targetTableIdentifier.quotedString)
    val affectedRowsFromTargetTable = batchProcessor.findAffectedRowsFromTargetTable(
      targetTableDf = targetTableDf,
      perKeyMinimumSequenceInMicrobatchDf = perKeyMinimumSequenceInMicrobatchDf
    )

    // The three inputs share the canonical SCD2 row schema by name, but not necessarily by column
    // set: after cross-run schema evolution the target (and the aux table, which mirrors it) can
    // carry user columns that the current microbatch no longer emits. `allowMissingColumns` pads
    // such columns with null on the side that lacks them (recursing into structs and arrays; map
    // types are not supported) instead of failing the union. (findAffectedRowsFromAuxiliaryTable
    // drops the aux-only deletedByBatchId column.)
    val microbatchAndAffectedRows = preprocessedBatchDf
      .unionByName(affectedRowsFromAuxiliaryTable, allowMissingColumns = true)
      .unionByName(affectedRowsFromTargetTable, allowMissingColumns = true)

    val decomposedDf = microbatchAndAffectedRows
      .transform(batchProcessor.decomposeOutOfOrderRows)
      // Assert well-formedness before dropping redundant rows: both transforms document their
      // input as the output of decomposeOutOfOrderRows, and dropRedundantRowsPostDecomposition
      // assumes decomposition tails are the only rows with a null recordStartAt. Checking first
      // both honors that contract and prevents an ill-formed row from being silently dropped as
      // redundant (when it ties with a neighbour) before the assertion can catch it.
      .transform(d => batchProcessor.assertWellFormedRowsPostDecomposition(d, batchId))
      .transform(batchProcessor.dropRedundantRowsPostDecomposition)

    val reconciledAndRoutedDf = decomposedDf
      .transform(batchProcessor.reconcileStartAndEndAt)
      .transform(batchProcessor.dropLeftoverDeletesPostReconciliation)
      .transform(batchProcessor.promoteDecompositionTailsToTombstones)
      .transform(batchProcessor.identifyAndTagAuxRows)

    Scd2ReconciliationResult(
      reconciledAndRoutedDf = reconciledAndRoutedDf,
      affectedRowsFromAuxiliaryTable = affectedRowsFromAuxiliaryTable,
      affectedRowsFromTargetTable = affectedRowsFromTargetTable
    )
  }
}

/**
 * The products of [[Scd2ForeachBatchHandler.reconcileMicrobatch]] that the auxiliary- and
 * target-table merges consume.
 *
 * @param reconciledAndRoutedDf the post-reconciliation rows tagged with aux-vs-target routing.
 * @param affectedRowsFromAuxiliaryTable the aux rows pulled in for this microbatch (canonical SCD2
 *        row schema, aux-only deletedByBatchId dropped).
 * @param affectedRowsFromTargetTable the target rows pulled in for this microbatch.
 */
private[autocdc] case class Scd2ReconciliationResult(
    reconciledAndRoutedDf: DataFrame,
    affectedRowsFromAuxiliaryTable: DataFrame,
    affectedRowsFromTargetTable: DataFrame)
