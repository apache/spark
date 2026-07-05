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

    // All three share the canonical schema; findAffectedRowsFromAuxiliaryTable drops the aux-only
    // deletedByBatchId column.
    val microbatchAndAffectedRows = preprocessedBatchDf
      .unionByName(affectedRowsFromAuxiliaryTable)
      .unionByName(affectedRowsFromTargetTable)

    val decomposedDf = microbatchAndAffectedRows
      .transform(batchProcessor.decomposeOutOfOrderRows)
      .transform(batchProcessor.dropRedundantRowsPostDecomposition)
      .transform(d => batchProcessor.assertWellFormedRowsPostDecomposition(d, batchId))

    val reconciledAndRoutedDf = decomposedDf
      .transform(batchProcessor.reconcileStartAndEndAt)
      .transform(batchProcessor.dropLeftoverDeletesPostReconciliation)
      .transform(batchProcessor.promoteDecompositionTailsToTombstones)
      .transform(batchProcessor.identifyAndTagAuxRows)

    batchProcessor.mergeRowsIntoAuxiliaryTable(
      reconciledDfWithAuxRowsTagged = reconciledAndRoutedDf,
      originalAffectedRowsFromAuxiliaryTable = affectedRowsFromAuxiliaryTable,
      auxiliaryTableIdentifier = auxiliaryTableIdentifier,
      batchId = batchId
    )

    batchProcessor.mergeRowsIntoTargetTable(
      reconciledDfWithAuxRowsTagged = reconciledAndRoutedDf,
      affectedRowsFromTargetTable = affectedRowsFromTargetTable,
      targetTableIdentifier = targetTableIdentifier
    )
  }
}
