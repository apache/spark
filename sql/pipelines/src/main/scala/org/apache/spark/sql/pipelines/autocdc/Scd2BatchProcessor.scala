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
      col = changeArgs.sequencing.cast(resolvedSequencingType)
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

/**
 * Concept: run of upsert events.
 *
 * A run is a maximal sequence of consecutive upsert events (in sorted order by sequencing)
 * for the same key whose tracked-history-column values are all identical. The transition
 * from a previous run's tail to a new run's head represents a real state change; every
 * subsequent event in the run is a no-op continuation that logically coalesces with the head.
 *
 * Runs matter because SCD2 only emits a new visible historical row when a
 * tracked-history column actually changes. By convention we choose that only the tail of a
 * run produces a visible row in the target table; the rest become hidden rows in the aux
 * table. Selecting the tail means the latest no-op upsert is reflected in the target table.
 *
 * Example, with trackHistoryCols = [name], events for some key:
 *   (S=5,  name=Alice)   -> starts run head at S=5. Row lives in aux table.
 *   (S=10, name=Alice)   -> no-op, adds to run at S=5. Row lives in aux table.
 *   (S=15, name=Alice)   -> no-op and tail of run at S=5. Row lives in target table with
 *                           START_AT=5.
 *   (S=20, name=Charlie) -> new run head/tail (run size=1) at S=20. Row lives in target
 *                           table.
 *
 * Now if a new late-arriving event (S=12, name=Bob) arrives for the same key, we have:
 *   (S=5,  name=Alice)   -> starts run head at S=5. Row lives in aux table.
 *   (S=10, name=Alice)   -> no-op but now tail of run at S=5. Row now lives in target
 *                           table with START_AT=5.
 *   (S=12, name=Bob)     -> new run head/tail (run size=1) at S=12. Row lives in target
 *                           table.
 *   (S=15, name=Alice)   -> previously-visible tail converts to a new run head at S=15. Row
 *                           remains in target table, but now with START_AT=15.
 *   (S=20, name=Charlie) -> new run head at S=20. Row lives in target table.
 *
 * Note that if we did not track the no-op events in the aux table for the run at S=5 before the
 * event (S=12, name=Bob) arrived, then we would not have correctly reconciled that the event
 * (S=10, name=Alice) is now the visible tail of the Alice run before Bob.
 *
 * -------------
 * Concept: target table.
 *
 * The user-consumable output table of the CDC transformation. Every row in the target table
 * represents the visible tail of a run (maybe size 1), carrying the run head's START_AT and the
 * latest row values for that run. The target table in its entirety represents the SCD2
 * representation of the CDC flow's source table.
 *
 * -------------
 * Concept: aux table.
 *
 * The side state table used to track out of order events from the CDC source. Two classes
 * of events are represented as rows in this table:
 *    1. Early-arriving deletes, with no matching upsert; this is considered a tombstone,
 *       and may match with a late-arriving upsert in a future microbatch.
 *    2. No-op upserts (i.e. tails of runs); hidden no-op rows that may reconcile as
 *       state-changing run heads in a future microbatch.
 *
 * The aux table is considered an internal table that users should neither tamper nor consider
 * public contract.
 *
 * -------------
 * Concept: same-sequence tie-break between an upsert and a delete.
 *
 * When an upsert event and a delete event share the same `__RECORD_START_AT`, the delete wins:
 * the visible upsert is dropped (as a zero-width interval) and only the tombstone is written
 * to the aux table. The reverse pair (delete arriving first, then an upsert at the same
 * sequence) is symmetric: the tombstone closes the upsert at the same instant, again leaving
 * a zero-width visible interval that is dropped, and only the tombstone survives.
 *
 * This tie-break is an internal contract only - we do not publicly guarantee deterministic
 * resolution when two events for the same key share a sequence value. Users who care about
 * ordering should ensure their sequencing column is unique per (key, event).
 */
object Scd2BatchProcessor {
  /**
   * Metadata field that represents the exact time (sequence) of the CDC event that produced
   * this row. Null only for synthetic decomposition tails.
   */
  private[autocdc] val recordStartAtFieldName: String = "__RECORD_START_AT"

  /**
   * What this column represents depends on which AutoCDC artifact table it is read from.
   *
   * In the target table:
   *    The user-visible column representing when this row is considered active from, i.e.
   *    this upsert run's head [[recordStartAtFieldName]].
   * In the aux table:
   *    If this row represents a tombstone, then the same value as [[recordStartAtFieldName]].
   *    Else this row represents a coalesced no-op row that is part of an upsert run.
   *    Inherit the [[recordStartAtFieldName]] of the head of this upsert's run.
   *
   * The invariant in both tables is: startAtColName <= recordStartAtFieldName. If an event was
   * generated at time X, it is active by time X, or earlier if it is not a run head.
   */
  private[autocdc] val startAtColName: String = "__START_AT"

  /**
   * What this column represents depends on which AutoCDC artifact table it is read from.
   *
   * In the target table:
   *    The user-visible column representing when this row became inactive. Null IFF the row
   *    is active: neither superseded by a state-changing upsert nor affected by a delete.
   * In the aux table:
   *    If this row is a tombstone, then by convention the sequence of the delete event that
   *    produced it. Delete events are considered instantaneous in time.
   *    Else this row is a coalesced no-op row that is part of an upsert run, and by
   *    convention the value will always be null.
   */
  private[autocdc] val endAtColName: String = "__END_AT"

  /**
   * Column names reserved by AutoCDC, that will be projected onto the microbatch and target
   * tables. If the user's source dataframe contains any of these columns, SCD2 reconciliation
   * will fail.
   */
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
