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

import org.apache.spark.SparkException
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

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
   * Backtick-quoted key column names. Use when the name flows through an expression parser
   * (e.g., [[F.col]]), which interprets dotted names as struct-field accesses.
   */
  private lazy val keysQuoted: Seq[String] = changeArgs.keys.map(_.quoted)

  /**
   * Raw key column names. Use when the name is matched literally against a schema field
   * (e.g., DataFrame `.join(other, usingColumns)`), where backticks are NOT stripped.
   */
  private lazy val keysRaw: Seq[String] = changeArgs.keys.map(_.name)

  /**
   * Reconcile a CDC microbatch into the canonical form the auxiliary- and target-table merges
   * consume.
   *
   * Step ordering is load-bearing: the row-extension steps reference user data columns that
   * target-column selection is allowed to drop, so selection runs last. Unlike SCD1, no per-key
   * deduplication step is performed here - SCD2 preserves every event as part of the row's
   * history, including byte-identical full-event duplicates.
   *
   * Duplicate event elimination (e.g., collapsing two identical events at the same sequence),
   * whether across microbatches or within the same microbatch, is the responsibility of
   * downstream reconciliation - not preprocessing.
   *
   * @param microbatchDf
   *   the incoming CDC microbatch.
   * @return
   *   a dataframe that retains every input row 1:1 - no rows added, dropped, reordered, or
   *   merged - with the following schema, in column order:
   *     1. The user columns of `microbatchDf` that survive [[ChangeArgs.columnSelection]], in
   *        the order they appeared in the input.
   *     2. [[startAtColName]], populated with the sequence value of the row.
   *     3. [[endAtColName]], populated with the sequence value of the row IFF it's a delete
   *        event, null otherwise.
   *     4. [[cdcMetadataColName]], conforming to [[targetCdcMetadataColSchema]].
   */
  private[autocdc] def preprocessMicrobatch(microbatchDf: DataFrame): DataFrame = {
    microbatchDf
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
      col = Scd2BatchProcessor.constructTargetCdcMetadataCol(
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

  /**
   * For each key in the preprocessed microbatch, compute the earliest [[recordStartAtFieldName]]
   * across the key's events.
   *
   * @param preprocessedBatchDf
   *   a validated and preprocessed microbatch as produced by [[preprocessMicrobatch]] - in
   *   particular, non-null key columns and a non-null [[recordStartAtFieldName]] on every row.
   * @return
   *   a dataframe containing one row per distinct key. Schema, in column order:
   *     1. The key columns ([[ChangeArgs.keys]]), in their declared order.
   *     2. [[minSequenceColName]], carrying the min [[recordStartAtFieldName]]
   *        across all records within the microbatch for that key.
   */
  private[autocdc] def computeMinimumSequencePerKey(preprocessedBatchDf: DataFrame): DataFrame = {
    val recordStartAt =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    preprocessedBatchDf
      .groupBy(keysQuoted.map(F.col): _*)
      .agg(F.min(recordStartAt).alias(Scd2BatchProcessor.minSequenceColName))
  }

  /**
   * Find the auxiliary-table rows whose state matters for reconciling the microbatch.
   *
   * @param rawAuxiliaryTableDf
   *   the auxiliary table in its native schema, whose CDC metadata column carries an extra
   *   [[deletedByBatchIdFieldName]] on top of the target/microbatch schema.
   * @param perKeyMinimumSequenceInMicrobatch
   *   one row per distinct key as produced by [[computeMinimumSequencePerKey]], representing
   *   the minimum sequence for that key in the microbatch.
   * @param batchId
   *   the underlying Spark streaming query's batchId, which serves as the idempotency key.
   * @return
   *   a dataframe containing all the affected aux rows, but with the CDC metadata column narrowed
   *   to the target/microbatch schema (aux-only subfields stripped) so the result is
   *   union-compatible with preprocessed microbatch rows and target-table rows downstream.
   */
  private[autocdc] def findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf: DataFrame,
      perKeyMinimumSequenceInMicrobatch: DataFrame,
      batchId: Long
  ): DataFrame = {
    val auxTableRecordStartAtField = Scd2BatchProcessor.recordStartAtOf(
      F.col(AutoCdcReservedNames.cdcMetadataColName)
    )
    val auxTableDeletedByBatchIdField = Scd2BatchProcessor.deletedByBatchIdOf(
      F.col(AutoCdcReservedNames.cdcMetadataColName)
    )

    val reducedAuxiliaryTableDf = rawAuxiliaryTableDf
      .filter(
        // Ignore any auxiliary table rows logically deleted by any microbatch other than this one
        // itself. Recall this execution could be a retry attempt on the same microbatch, and
        // batchId is our idempotency key.
        auxTableDeletedByBatchIdField.isNull ||
          auxTableDeletedByBatchIdField === F.lit(batchId)
      )
      // The aux table's CDC metadata column is a superset of the target's: it carries the
      // additional [[deletedByBatchIdFieldName]]. Since we eventually union aux rows with
      // target and microbatch rows (which use the target's narrower CDC metadata schema), strip
      // the aux-only subfields here so all three sources share an identical CDC metadata column
      // schema, and replace the existing CDC metadata column with it.
      .withColumn(
        AutoCdcReservedNames.cdcMetadataColName,
        Scd2BatchProcessor.constructTargetCdcMetadataCol(
          recordStartAt = auxTableRecordStartAtField,
          sequencingType = resolvedSequencingType
        )
      )

    val perKeyMinimumSequenceInMicrobatchCol = F.col(Scd2BatchProcessor.minSequenceColName)

    // Per key, identify the sequence value associated with the anchor row in the aux table.
    //
    // The anchor row is the aux row with the largest [[recordStartAtFieldName]] strictly less
    // than the min sequence in the incoming microbatch for that key. The reconciler needs this
    // "left context" in two cases:
    //   (1) Incoming no-op upsert: without the anchor, it would look like a new run head, when in
    //       reality it's a part of an existing no-op run/head.
    //   (2) Incoming state-changing upsert that bisects two aux no-ops: the anchor surfaces
    //       the before-half so both halves can be promoted to target. (The after-half is
    //       picked up by the >= minSeq branch.)
    //
    // Because no-op upserts are stored only in the aux table, the anchor concept only exists when
    // pulling in rows from the aux table, and is not relevant for the target table.
    //
    // Keys with no aux row strictly before the min sequence have no anchor; their affected set
    // reduces to "all aux rows at or after the min sequence."
    //
    // The shape of this DataFrame is: [key1, key2, ... keyN, anchorSequence]
    val perKeyAnchorSequence: DataFrame = reducedAuxiliaryTableDf
      // The number of rows in [[perKeyMinimumSequenceInMicrobatch]] is bounded by the
      // number of unique keys in the microbatch, which should typically be small. The
      // auxiliary table should generally also be small, containing only no-op upsert runs
      // and tombstones per key. Therefore this join should be cheap, and broadcast joinable.
      .join(perKeyMinimumSequenceInMicrobatch, keysRaw)
      .filter(auxTableRecordStartAtField < perKeyMinimumSequenceInMicrobatchCol)
      .groupBy(keysQuoted.map(F.col): _*)
      .agg(
        F.max(auxTableRecordStartAtField).as(Scd2BatchProcessor.anchorSequenceColName)
      )
    val anchorSequenceCol = F.col(Scd2BatchProcessor.anchorSequenceColName)
    val auxRowIsAnchorRow = auxTableRecordStartAtField === anchorSequenceCol

    // Now that we have the minimum sequence in the microbatch and the sequence of the anchor row,
    // we have enough information to compute the full set of auxiliary rows that affect or are
    // affected by the microbatch.
    val auxRowIsAfterMinSequenceInMicrobatch =
      auxTableRecordStartAtField >= perKeyMinimumSequenceInMicrobatchCol

    val auxRowAffectsMicrobatch = auxRowIsAfterMinSequenceInMicrobatch || auxRowIsAnchorRow

    val affectedRowsFromAuxiliaryTable = reducedAuxiliaryTableDf
      // Per row, join/project the minimum microbatch sequence and anchor sequence for that row's
      // key set. This join is relatively cheap, because the size of the dataframes being joined is
      // bound by the number of unique keys in the microbatch.
      .join(perKeyMinimumSequenceInMicrobatch, keysRaw)
      .join(
        perKeyAnchorSequence,
        keysRaw,
        joinType = "left"
      )
      // Using the joined information, determine if the row is affected by the microbatch.
      .filter(auxRowAffectsMicrobatch)
      .drop(perKeyMinimumSequenceInMicrobatchCol, anchorSequenceCol)

    affectedRowsFromAuxiliaryTable
  }

  /**
   * Find the target-table rows whose state matters for reconciling the microbatch.
   *
   * @param targetTableDf
   *   the target table in its native schema.
   * @param perKeyMinimumSequenceInMicrobatch
   *   one row per distinct key as produced by [[computeMinimumSequencePerKey]], representing
   *   the minimum sequence for that key in the microbatch.
   * @return
   *   a dataframe containing the affected target rows, exactly as-is from the target table.
   */
  private[autocdc] def findAffectedRowsFromTargetTable(
      targetTableDf: DataFrame,
      perKeyMinimumSequenceInMicrobatch: DataFrame
  ): DataFrame = {
    val targetEndAtCol = F.col(Scd2BatchProcessor.endAtColName)
    val perKeyMinimumSequenceInMicrobatchCol = F.col(Scd2BatchProcessor.minSequenceColName)

    // Per key, identify all the rows in the target table that may be affected by the
    // incoming microbatch.
    //
    // Unlike the auxiliary table, the target table holds visible rows only: no hidden open
    // no-op upsert rows, no tombstones. Visible rows for a given key form a non-overlapping
    // interval partition over the sequencing axis, and at most one row has a null [[endAtColName]]
    // (the currently active row per key).
    //
    // Hence we can simply grab all rows that were active at some point after the min sequencing
    // per key, which can be determined entirely by the row's [[endAtColName]].
    val isCurrentlyActiveRow = targetEndAtCol.isNull
    val rowEndsAfterMinimumSequence = targetEndAtCol >= perKeyMinimumSequenceInMicrobatchCol
    val rowMayBeAffected = isCurrentlyActiveRow || rowEndsAfterMinimumSequence

    val affectedRowsFromTargetTable = targetTableDf
      .join(perKeyMinimumSequenceInMicrobatch, keysRaw)
      .filter(rowMayBeAffected)
      .drop(perKeyMinimumSequenceInMicrobatchCol)

    affectedRowsFromTargetTable
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
 * Concept: decomposition tail.
 *
 * A transient and synthetic row produced by the batch processor during reconciliation (not
 * from the CDC source) when a previously-closed historical row [START_AT=X, END_AT=Y] is
 * bisected by a late-arriving event. The bisected row is split into a head
 * [START_AT=X, END_AT=null] - inheriting the original row's data and `__RECORD_START_AT` -
 * and a tail [START_AT=null, END_AT=Y, `__RECORD_START_AT`=null] that carries the original
 * row's right boundary. The tail typically becomes the closing END_AT of a bisecting upsert,
 * giving it a valid right boundary in the target-table history.
 *
 * Decomposition tails are uniquely identified by `__RECORD_START_AT` = null - the only row
 * category with that property - and are never persisted in their tail form: each is either
 * absorbed by the next event in the affected window (dropped as redundant) or promoted to a
 * tombstone in the aux table if it survives reconciliation unmatched.
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
   * CDC metadata column field that represents the exact time (sequence) of the CDC event that
   * produced this row. Null only for synthetic decomposition tails.
   */
  private[autocdc] val recordStartAtFieldName: String = "__RECORD_START_AT"

  /**
   * CDC metadata column field that represents the microbatch id a particular row was considered
   * logically deleted by. Any future microbatches should consider that row as deleted.
   *
   * Logically deleted rows exist as a concept in the auxiliary to provide idempotency, should a
   * microbatch fail between a MERGE executed against the auxiliary table and the MERGE executed
   * against the target table.
   *
   * This field only exists in the CDC metadata column for the auxiliary table, not in CDC
   * metadata column for the target table.
   */
  private val deletedByBatchIdFieldName: String = "__DELETED_BY_BATCH_ID"

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
   * Name of temporary column projected onto microbatch to compute the min sequencing value per
   * key within the microbatch.
   */
  private[autocdc] val minSequenceColName: String = s"${AutoCdcReservedNames.prefix}min_sequence"

  /**
   * Name of temporary column projected used to identify the sequence associated with the anchor
   * row found in the auxiliary table for the incoming microbatch. Since sequences must be unique
   * amongst all rows for a key (or risk undefined behavior), this sequence value uniquely
   * identifies an exact row in the aux.
   */
  private val anchorSequenceColName: String = s"${AutoCdcReservedNames.prefix}anchor_sequence"

  /** Project the [[recordStartAtFieldName]] out of an SCD2 CDC metadata column. */
  private def recordStartAtOf(cdcMetadataCol: Column): Column =
    cdcMetadataCol.getField(recordStartAtFieldName)

  /** Project the [[deletedByBatchIdFieldName]] out of an SCD2 CDC metadata column. */
  private def deletedByBatchIdOf(cdcMetadataCol: Column): Column =
    cdcMetadataCol.getField(deletedByBatchIdFieldName)

  /**
   * Schema of the CDC metadata struct column for SCD2 target table rows.
   */
  private[pipelines] def targetCdcMetadataColSchema(sequencingType: DataType): StructType =
    StructType(
      Seq(
        // The sequence value of the originating CDC event for this row. Nullable because
        // decomposition tails, which are transient and synthetically constructed during
        // reconciliation, have a null record start at.
        StructField(recordStartAtFieldName, sequencingType, nullable = true)
      )
    )

  /**
   * Construct the CDC metadata struct column for SCD2 target/microbatch rows, following the
   * exact schema and field ordering defined by [[targetCdcMetadataColSchema]].
   */
  private def constructTargetCdcMetadataCol(
      recordStartAt: Column,
      sequencingType: DataType
  ): Column = {
    val cdcMetadataFieldsInOrder = targetCdcMetadataColSchema(sequencingType).fields.map { field =>
      val value = field.name match {
        case `recordStartAtFieldName` => recordStartAt
        case other =>
          throw SparkException.internalError(
            s"Unable to construct SCD2 target CDC metadata column due to unknown " +
              s"`${other}` field."
          )
      }
      value.cast(field.dataType).as(field.name)
    }
    F.struct(cdcMetadataFieldsInOrder.toImmutableArraySeq: _*)
  }

  /**
   * Schema of the CDC metadata struct column for SCD2 aux-table rows. Strict superset of
   * [[targetCdcMetadataColSchema]]: extends it with the aux-only [[deletedByBatchIdFieldName]]
   * used for SCD2 idempotency.
   */
  private[pipelines] def auxCdcMetadataColSchema(sequencingType: DataType): StructType =
    StructType(
      targetCdcMetadataColSchema(sequencingType).fields.toImmutableArraySeq ++
        Seq(
          // The microbatch id by which this aux row was logically deleted, or null if the
          // row is still live.
          StructField(deletedByBatchIdFieldName, LongType, nullable = true)
        )
    )

  /**
   * Construct the CDC metadata struct column for SCD2 aux-table rows, following the exact
   * schema and field ordering defined by [[auxCdcMetadataColSchema]].
   */
  private[autocdc] def constructAuxCdcMetadataCol(
      recordStartAt: Column,
      deletedByBatchId: Column,
      sequencingType: DataType
  ): Column = {
    val cdcMetadataFieldsInOrder = auxCdcMetadataColSchema(sequencingType).fields.map { field =>
      val value = field.name match {
        case `recordStartAtFieldName` => recordStartAt
        case `deletedByBatchIdFieldName` => deletedByBatchId
        case other =>
          throw SparkException.internalError(
            s"Unable to construct SCD2 aux CDC metadata column due to unknown " +
              s"`${other}` field."
          )
      }
      value.cast(field.dataType).as(field.name)
    }
    F.struct(cdcMetadataFieldsInOrder.toImmutableArraySeq: _*)
  }
}
