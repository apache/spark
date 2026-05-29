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

import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Per-microbatch processor for SCD Type 2 AutoCDC flows, complying to the specified
 * [[changeArgs]] configuration.
 *
 * @param changeArgs The CDC flow configuration.
 * @param resolvedSequencingType The post-analysis [[DataType]] of the sequencing column, derived
 *                               from the flow's resolved DataFrame at flow setup time.
 */
case class Scd2BatchProcessor(
    changeArgs: ChangeArgs,
    resolvedSequencingType: DataType) {

  /**
   * Reconcile a CDC microbatch into the canonical form the auxiliary- and target-table merges
   * consume.
   *
   * Step ordering is load-bearing: the row-extension steps reference user data columns that
   * target-column selection is allowed to drop, so selection runs last. Unlike SCD1, no per-key
   * deduplication step is needed - SCD2 preserves every event as part of the row's history.
   *
   * Requires the microbatch to have been validated upstream so that the sequencing column is
   * non-null and orderable.
   */
  private[autocdc] def preprocessMicrobatch(validatedBatchDf: DataFrame): DataFrame = {
    validatedBatchDf
      .transform(extendMicrobatchRowsWithStartAt)
      .transform(extendMicrobatchRowsWithEndAt)
      .transform(extendMicrobatchRowsWithCdcMetadata)
      .transform(projectTargetColumnsOntoMicrobatch)
  }

  /**
   * Stamp each microbatch row with its currently known start-at (i.e active-from) using its
   * sequencing.
   */
  private def extendMicrobatchRowsWithStartAt(microbatchDf: DataFrame): DataFrame = {
    microbatchDf.withColumn(
      colName = Scd2BatchProcessor.startAtColName,
      col = changeArgs.sequencing
    )
  }

  /**
   * Stamp each microbatch delete event row with its end time sequence, as they are instantaneous
   * events.
   *
   * Non-deletes leave a null end, as do not yet know if the row reprsents an active upsert, or a
   * closed upsert. This will become clear in later reconciliation against the aux/target tables.
   */
  private def extendMicrobatchRowsWithEndAt(microbatchDf: DataFrame): DataFrame = {
    microbatchDf.withColumn(
      colName = Scd2BatchProcessor.endAtColName,
      col = (
        changeArgs.deleteCondition match {
          case Some(deleteCondition) =>
            F.when(deleteCondition, changeArgs.sequencing).otherwise(null)
          case None =>
            F.lit(null)
        }
      ).cast(resolvedSequencingType)
    )
  }

  /**
   * Project the operational CDC metadata column carrying the literal event sequence. Downstream
   * merges rely on it to preserve original event lineage regardless of how rows start/end-at are
   * coalesced.
   */
  private def extendMicrobatchRowsWithCdcMetadata(microbatchDf: DataFrame): DataFrame = {
    microbatchDf.withColumn(
      colName = AutoCdcReservedNames.cdcMetadataColName,
      col = Scd2BatchProcessor.constructCdcMetadataStruct(
        recordStartAt = changeArgs.sequencing,
        sequencingType = resolvedSequencingType
      )
    )
  }

  /**
   * Apply the user's target column selection while preserving the SCD2 framework columns; the
   * latter are required by downstream merges and persisted to both the auxiliary and target
   * tables, so users cannot deselect them.
   *
   * Requires the framework columns to already be present on the input.
   */
  private def projectTargetColumnsOntoMicrobatch(
      microbatch: DataFrame
  ): DataFrame = {
    val dataSchema = StructType(
      microbatch.schema.fields.filterNot(f =>
        Scd2BatchProcessor.reservedFrameworkColNames.contains(f.name)
      )
    )
    val userSelectedDataSchema =
      ColumnSelection.applyToSchema(
        schemaName = "microbatch",
        schema = dataSchema,
        columnSelection = changeArgs.columnSelection,
        caseSensitive =
          microbatch.sparkSession.sessionState.conf.caseSensitiveAnalysis
      )
    val finalColumnsToSelect: Seq[Column] =
      userSelectedDataSchema.fieldNames.toSeq.map(colName => {
        // Spark drops backticks in the schema, quote all identifiers for safety before executing
        // select. Identifiers could have special characters such as '.'.
        F.col(QuotingUtils.quoteIdentifier(colName))
      }) ++ Seq(
        F.col(Scd2BatchProcessor.startAtColName),
        F.col(Scd2BatchProcessor.endAtColName),
        F.col(AutoCdcReservedNames.cdcMetadataColName)
      )
    microbatch.select(finalColumnsToSelect: _*)
  }

}

object Scd2BatchProcessor {
    // Metadata field that represents the exact time (sequence) of the CDC event that produced this
    // row. Null only for synthetic decomposition tails.
    private[autocdc] val recordStartAtFieldName: String = "__RECORD_START_AT"

    // In the target table:
    //    The user-visible column representing when this row is considered active from, i.e.
    //    this upsert run head's [[recordStartAtFieldName]].
    // In the aux table:
    //    If this row represents a tombstone, then the same value as [[recordStartAtFieldName]].
    //    Else this row represents a coalesced no-op row that is part of an upsert run.
    //    Inherit the [[recordStartAtFieldName]] of the head of this upsert's run.
    //
    // The invariant in both tables is: startAtColName <= recordStartAtFieldName. If an event was
    // generated at time X, it is active by time X, or earlier if it is not a run head.
    private[autocdc] val startAtColName: String = "__START_AT"

    // In the target table:
    //    The user-visible column representing when this row became inactive. Null IFF the row
    //    is active: neither superseded by a state-changing upsert nor affected by a delete.
    // In the aux table:
    //    If this row is a tombstone, then by convention the sequence of the delete event that
    //    produced it. Delete events are considered instantaneous in time.
    //    Else this row is a coalesced no-op row that is part of an upsert run, and by
    //    convention the value will always be null.
    private[autocdc] val endAtColName: String = "__END_AT"

    private val reservedFrameworkColNames: Set[String] = Set(
        startAtColName,
        endAtColName,
        AutoCdcReservedNames.cdcMetadataColName
    )

    /**
     * Construct the CDC metadata struct column for SCD1, following the exact schema and field
     * ordering defined by [[cdcMetadataColSchema]].
     */
    def constructCdcMetadataStruct(
        recordStartAt: Column,
        sequencingType: DataType
    ): Column = {
        F.struct(
            recordStartAt.cast(sequencingType).as(recordStartAtFieldName)
        )
    }
}
