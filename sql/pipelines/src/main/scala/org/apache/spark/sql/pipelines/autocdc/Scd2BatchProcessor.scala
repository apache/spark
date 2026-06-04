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
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

/**
 * Per-microbatch processor for SCD Type 2 AutoCDC flows, complying with the specified
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
   * WindowSpec that sorts CDC event rows in ascending order per key, by event origination
   * sequence time (i.e, record start at).
   */
  private[autocdc] val orderChronologicallyPerKeyWindow: WindowSpec = {
    val recordStartAtCol = Scd2BatchProcessor.recordStartAtOf(
      F.col(AutoCdcReservedNames.cdcMetadataColName)
    )
    val endAtCol = F.col(Scd2BatchProcessor.endAtColName)

    // All rows except decomposition tails have a non-null recordStartAt. Tails use their
    // logical endAt to order against other rows. That endAt comes from a real CDC event
    // from some processed microbatch, so it is comparable to other rows' recordStartAt.
    val sequencingIfDecompositionTail = endAtCol
    val effectiveRecordStartAt = F.coalesce(recordStartAtCol, sequencingIfDecompositionTail).asc

    val orderDecompositionTailsFirst =
      RowClassifier.isDecompositionTail(recordStartAtCol).desc

    val isClosedInterval = endAtCol.isNotNull
    val orderOpenIntervalsFirst = isClosedInterval.asc

    Window
      .partitionBy(keysQuoted.map(F.col): _*)
      .orderBy(
        // recordStartAt is the source timeline for CDC events.
        effectiveRecordStartAt,
        // If a decomposition tail shares an effective recordStartAt with a non-tail, then its
        // redundant - another delete/upsert event immediately overtakes the delete that this
        // synthetic tail encodes. Intentionally order such redundant tails first, so that a
        // decomposition tail can check its own redundancy by looking at its immediate next
        // neighbor with LEAD(1).
        orderDecompositionTailsFirst,
        // Open intervals sort first so they can match a same-recordStartAt tombstone
        // during LEAD(1) reconciliation. For decomposition tails this is a no-op.
        orderOpenIntervalsFirst
      )
  }

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
   *     4. [[cdcMetadataColName]], conforming to [[cdcMetadataColSchema]].
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
   * Non-deletes leave a null end, as we do not yet know if the row represents an active upsert,
   * or a closed upsert. This will become clear in later reconciliation against the aux/target
   * tables.
   */
  private def extendMicrobatchRowsWithEndAt(microbatchDf: DataFrame): DataFrame = {
    microbatchDf.withColumn(
      colName = Scd2BatchProcessor.endAtColName,
      col = (
        changeArgs.deleteCondition match {
          case Some(deleteCondition) =>
            F.when(deleteCondition, changeArgs.sequencing).otherwise(F.lit(null))
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
      col = Scd2BatchProcessor.constructCdcMetadataCol(
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
    val caseSensitive = microbatch.sparkSession.sessionState.conf.caseSensitiveAnalysis

    // Strip the framework columns through the same case-aware path as the user selection, for
    // consistency with Scd1BatchProcessor.projectTargetColumnsOntoMicrobatch.
    val dataSchema = ColumnSelection.applyToSchema(
      schemaName = "microbatch",
      schema = microbatch.schema,
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(
          Scd2BatchProcessor.reservedFrameworkColNames.toSeq.map(UnqualifiedColumnName(_))
        )
      ),
      caseSensitive = caseSensitive
    )
    val userSelectedDataSchema =
      ColumnSelection.applyToSchema(
        schemaName = "microbatch",
        schema = dataSchema,
        columnSelection = changeArgs.columnSelection,
        caseSensitive = caseSensitive
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
   *   the auxiliary table in its native schema, which is expected to contain
   *   [[deletedByBatchIdColName]] in addition to all of the columns in the target table.
   * @param perKeyMinimumSequenceInMicrobatchDf
   *   one row per distinct key as produced by [[computeMinimumSequencePerKey]], representing
   *   the minimum sequence for that key in the microbatch.
   * @param batchId
   *   the underlying Spark streaming query's batchId, which serves as the idempotency key.
   * @return
   *   a dataframe containing all the affected aux rows, with the aux-only
   *   [[deletedByBatchIdColName]] column dropped so the result is union-compatible with
   *   preprocessed microbatch rows and target-table rows downstream.
   */
  private[autocdc] def findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf: DataFrame,
      perKeyMinimumSequenceInMicrobatchDf: DataFrame,
      batchId: Long
  ): DataFrame = {
    val auxTableRecordStartAtField = Scd2BatchProcessor.recordStartAtOf(
      F.col(AutoCdcReservedNames.cdcMetadataColName)
    )
    val auxTableDeletedByBatchIdCol = F.col(Scd2BatchProcessor.deletedByBatchIdColName)

    val reducedAuxiliaryTableDf = rawAuxiliaryTableDf
      .filter(
        // Ignore any auxiliary table rows logically deleted by any microbatch other than this one
        // itself. Recall this execution could be a retry attempt on the same microbatch, and
        // batchId is our idempotency key.
        auxTableDeletedByBatchIdCol.isNull ||
          auxTableDeletedByBatchIdCol === F.lit(batchId)
      )
      // Drop the aux-only idempotency column so the output schema matches target-table rows
      // and preprocessed-microbatch rows (which share the same canonical SCD2 row schema).
      .drop(Scd2BatchProcessor.deletedByBatchIdColName)

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
    val perKeyAnchorSequenceDf = reducedAuxiliaryTableDf
      // The number of rows in [[perKeyMinimumSequenceInMicrobatchDf]] is bounded by the
      // number of unique keys in the microbatch, which should typically be small. The
      // auxiliary table should generally also be small, containing only no-op upsert runs
      // and tombstones per key. Therefore this join should be cheap, and broadcast joinable.
      .join(perKeyMinimumSequenceInMicrobatchDf, keysRaw)
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
    val auxRowIsAtOrAfterMinSequenceInMicrobatch =
      auxTableRecordStartAtField >= perKeyMinimumSequenceInMicrobatchCol

    val auxRowAffectsMicrobatch = auxRowIsAtOrAfterMinSequenceInMicrobatch || auxRowIsAnchorRow

    val affectedRowsFromAuxiliaryTable = reducedAuxiliaryTableDf
      // Per row, project the minimum microbatch sequence and anchor sequence for that row's key
      // set onto the row, so the affected-row predicate can be evaluated in a single filter.
      .join(perKeyMinimumSequenceInMicrobatchDf, keysRaw)
      .join(
        perKeyAnchorSequenceDf,
        keysRaw,
        joinType = "left"
      )
      .filter(auxRowAffectsMicrobatch)
      .drop(perKeyMinimumSequenceInMicrobatchCol, anchorSequenceCol)

    affectedRowsFromAuxiliaryTable
  }

  /**
   * Find the target-table rows whose state matters for reconciling the microbatch.
   *
   * @param targetTableDf
   *   the target table in its native schema.
   * @param perKeyMinimumSequenceInMicrobatchDf
   *   one row per distinct key as produced by [[computeMinimumSequencePerKey]], representing
   *   the minimum sequence for that key in the microbatch.
   * @return
   *   a dataframe containing the affected target rows, with all columns passed-through.
   */
  private[autocdc] def findAffectedRowsFromTargetTable(
      targetTableDf: DataFrame,
      perKeyMinimumSequenceInMicrobatchDf: DataFrame
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

    // `>=` (rather than strict `>`) additionally pulls in the row that closes exactly at the
    // smallest incoming sequence: the consecutive left neighbor of that incoming event. This
    // provides "left context" for the smallest event, analogous to the anchor row in
    // [[findAffectedRowsFromAuxiliaryTable]]. It may need to be demoted from a target run
    // boundary to an aux no-op continuation if the incoming event at minSeq turns out to
    // extend an earlier run.
    val rowEndsAfterMinimumSequence = targetEndAtCol >= perKeyMinimumSequenceInMicrobatchCol
    val rowMayBeAffected = isCurrentlyActiveRow || rowEndsAfterMinimumSequence

    val affectedRowsFromTargetTable = targetTableDf
      .join(perKeyMinimumSequenceInMicrobatchDf, keysRaw)
      .filter(rowMayBeAffected)
      .drop(perKeyMinimumSequenceInMicrobatchCol)

    affectedRowsFromTargetTable
  }

  /**
   * For every closed non-tombstone row in the input dataframe whose immediate window-order
   * successor (in the same per-key partition, per [[orderChronologicallyPerKeyWindow]])
   * has `recordStartAt` strictly less than its `endAt` (in other words the row is being
   * bisected by its neighbor), replace that row by a "head" + "tail" pair:
   *   - head: copies the parent row exactly, except [[endAtColName]] is set to null.
   *   - tail: copies the parent row exactly, except [[startAtColName]] is set to null and
   *     [[recordStartAtFieldName]] inside [[cdcMetadataColName]] is set to null. All user
   *     data columns are inherited from the parent as-is.
   *
   * Decomposition tails are uniquely identified by [[recordStartAtFieldName]] = null and
   * are transient: downstream reconciliation drops them when a coincident non-tail row
   * already represents the same closure, or promotes them to tombstones in the aux table
   * otherwise.
   *
   * All other input rows pass through unchanged ("no-op decompose"). This includes:
   *   1. Open rows ([[endAtColName]] = null): incoming upserts, no-op continuations, etc.
   *   2. Tombstones ([[startAtColName]] = [[endAtColName]]): protected from decomposition
   *      even though they qualify as "closed" in the broader sense, because their interval
   *      is degenerate.
   *   3. Closed non-tombstone rows whose successor's [[recordStartAtFieldName]] is `>=`
   *      this row's [[endAtColName]]: the closing event already coincides with or follows
   *      the run boundary, so there is nothing to bisect.
   *
   * Bisection detection is implemented via `LEAD(1)` over [[orderChronologicallyPerKeyWindow]]:
   * because the window orders by effective recordStartAt ascending, examining only the
   * immediate successor is sufficient to decide whether any other row in the partition has
   * a recordStartAt within `[recordStartAt, endAt)`.
   *
   * Decomposing closed rows that are being bisected gives us that chance to form new
   * closed intervals using the incoming microbatch events, later in reconciliation.
   *
   * @param rowsToDecomposePerKey
   *   a dataframe conforming to the canonical SCD2 row schema
   *   `[user_cols..., [[startAtColName]], [[endAtColName]], [[cdcMetadataColName]]]`, where
   *   [[cdcMetadataColName]] conforms to [[targetCdcMetadataColSchema]]. Decomposition tails
   *   (rows with [[recordStartAtFieldName]] = null) MUST NOT be present on input - they are
   *   produced exclusively by this function.
   * @return
   *   a dataframe with the same schema as the input. Every closed non-tombstone row that
   *   was bisected has been replaced by its head + tail pair; every other row is carried
   *   through as-is. Each output row can be classified as one of: {decomposition head,
   *   decomposition tail, tombstone, open upsert, closed-and-unbisected row}. It's possible
   *   that some of the returned decomposition tails are logically redundant, as deletion
   *   markers that are immediately overtaken by a succeeding row.
   */
  private[autocdc] def decomposeOutOfOrderRows(rowsToDecomposePerKey: DataFrame): DataFrame = {
    val recordStartAtField =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    val startAtCol = rowsToDecomposePerKey.col(Scd2BatchProcessor.startAtColName)
    val endAtCol = rowsToDecomposePerKey.col(Scd2BatchProcessor.endAtColName)

    // Track the next (in sorted order) row's recordStartAt in a temporary column.
    val rowsToDecomposeWithWindowCols = rowsToDecomposePerKey.withColumn(
      Scd2BatchProcessor.nextRecordStartAtColName,
      F.lead(recordStartAtField, 1).over(orderChronologicallyPerKeyWindow)
    )
    val nextRecordStartAt = rowsToDecomposeWithWindowCols.col(
      Scd2BatchProcessor.nextRecordStartAtColName
    )

    val isClosedNonTombstoneRow = RowClassifier.isClosedRow(
      recordStartAt = recordStartAtField,
      startAt = startAtCol,
      endAt = endAtCol,
      includeTombstones = false
    )
    val nextRowBisectsCurrentRow =
      nextRecordStartAt.isNotNull && nextRecordStartAt < endAtCol
    val rowShouldDecompose = isClosedNonTombstoneRow && nextRowBisectsCurrentRow

    val originalCols: Seq[String] = rowsToDecomposePerKey.columns.toSeq
    val originalSchema: StructType = rowsToDecomposePerKey.schema
    def withOriginalSchemaPreserved(fieldName: String, expr: Column): Column = {
      val f = originalSchema(fieldName)
      expr.cast(f.dataType).as(f.name, f.metadata)
    }

    // Constructs the head of a row post-decomposition.
    def constructDecomposedRowHead: Column = {
      val fields = originalCols.map {
        case colName if colName == Scd2BatchProcessor.endAtColName =>
          // End-at is opened (set to null); every other column is inherited as-is from the
          // original parent row.
          withOriginalSchemaPreserved(colName, F.lit(null))
        case colName =>
          withOriginalSchemaPreserved(colName, rowsToDecomposeWithWindowCols.col(colName))
      }
      F.struct(fields: _*)
    }

    // Constructs the tail of a row post-decomposition.
    def constructDecomposedRowTail: Column = {
      val fields = originalCols.map {
        case colName if colName == Scd2BatchProcessor.startAtColName =>
          // Start-at is opened (set to null), every other column is inherited as-is from the
          // original parent row.
          withOriginalSchemaPreserved(colName, F.lit(null))
        case colName if colName == AutoCdcReservedNames.cdcMetadataColName =>
          withOriginalSchemaPreserved(
            colName,
            Scd2BatchProcessor.constructTargetCdcMetadataCol(
              recordStartAt = F.lit(null).cast(resolvedSequencingType),
              sequencingType = resolvedSequencingType
            )
          )
        case colName =>
          withOriginalSchemaPreserved(colName, rowsToDecomposeWithWindowCols.col(colName))
      }
      F.struct(fields: _*)
    }

    // No-op decomposition carries over the row exactly as-is.
    def constructNoopDecomposedRow: Column = {
      val fields = originalCols.map(colName =>
        withOriginalSchemaPreserved(colName, rowsToDecomposeWithWindowCols.col(colName))
      )
      F.struct(fields: _*)
    }

    // If a row is bisected by the proceeding row, decompose it into a head + tail pair.
    // Otherwise pass through as a single-element array so the explode below is uniform.
    val perRowDecompositionResults = F
      .when(
        rowShouldDecompose,
        F.array(constructDecomposedRowHead, constructDecomposedRowTail)
      )
      .otherwise(F.array(constructNoopDecomposedRow))

    rowsToDecomposeWithWindowCols
      // The output schema matches the input schema exactly; no extra columns are projected.
      .withColumn(
        Scd2BatchProcessor.decompositionExplodedColName,
        F.explode(perRowDecompositionResults)
      )
      .select(F.col(s"${Scd2BatchProcessor.decompositionExplodedColName}.*"))
  }

  /**
   * Drops rows that are redundant within the per-key chronological window output by
   * [[decomposeOutOfOrderRows]]. Three classes of redundant rows are removed:
   *
   *   1. Same-event duplicates (defensive): two rows whose
   *      `(effectiveRecordStartAt, startAt, endAt)` null-safe-match represent the same
   *      event at the same sequence. The chronologically-leading copy is dropped. Should
   *      not fire in well-formed CDC streams where sequences are unique per key, but can
   *      occur when streaming from a distributed system source like Kafka and should still
   *      be graciously handled, since the resulting target table is still deterministic
   *      and well-defined if the user data columns are identical for both. If the user data
   *      columns are not identical, then behavior for which row is kept is publicly
   *      documented as undefined.
   *
   *   2. Upsert overtaken at the same instant (defensive): an open upsert whose next
   *      window-order neighbor shares its `effectiveRecordStartAt`. The upsert opens at
   *      an instant the next event already overtakes, leaving it with no effective
   *      duration. In practice this arises only from sequence collisions for the same
   *      key (a violation of the SCD2 sequencing-uniqueness contract); behavior for
   *      which upsert is kept is publicly documented as undefined.
   *
   *   3. Deletion overtaken at the same instant (routine): a decomposition tail (synthetic
   *      delete-at-`endAt` produced by [[decomposeOutOfOrderRows]]) whose `endAt` equals
   *      the next row's `effectiveRecordStartAt`. The synthetic delete is already encoded
   *      by the coincident event, so the tail is redundant. Fires whenever a microbatch
   *      event lands on an existing close boundary in the target.
   *
   * @param decomposedRowsPerKey
   *   the output of [[decomposeOutOfOrderRows]]: a dataframe conforming to the canonical
   *   SCD2 row schema `[user_cols..., [[startAtColName]], [[endAtColName]],
   *   [[cdcMetadataColName]]]`.
   * @return
   *   a dataframe with the same schema, with redundant rows filtered out.
   */
  private[autocdc] def dropRedundantRowsPostDecomposition(
      decomposedRowsPerKey: DataFrame): DataFrame = {
    val recordStartAtField =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    val startAtCol = F.col(Scd2BatchProcessor.startAtColName)
    val endAtCol = F.col(Scd2BatchProcessor.endAtColName)
    val effectiveRecordStartAt = F.coalesce(recordStartAtField, endAtCol)

    // Compute next-row window values into temporary columns before filtering on them.
    val withWindowCols = decomposedRowsPerKey
      .withColumn(
        Scd2BatchProcessor.nextEffectiveRecordStartAtColName,
        F.lead(effectiveRecordStartAt, 1).over(orderChronologicallyPerKeyWindow)
      )
      .withColumn(
        Scd2BatchProcessor.nextStartAtColName,
        F.lead(startAtCol, 1).over(orderChronologicallyPerKeyWindow)
      )
      .withColumn(
        Scd2BatchProcessor.nextEndAtColName,
        F.lead(endAtCol, 1).over(orderChronologicallyPerKeyWindow)
      )
    val nextEffectiveRecordStartAt =
      withWindowCols.col(Scd2BatchProcessor.nextEffectiveRecordStartAtColName)
    val nextStartAt = withWindowCols.col(Scd2BatchProcessor.nextStartAtColName)
    val nextEndAt = withWindowCols.col(Scd2BatchProcessor.nextEndAtColName)

    // Rule 1: full sequencing-dimension match (same event at same sequence).
    val isSequencingDimensionDuplicate =
      effectiveRecordStartAt <=> nextEffectiveRecordStartAt &&
        startAtCol <=> nextStartAt &&
        endAtCol <=> nextEndAt

    // Rule 2: open upsert whose effective sequence ties with the next row's.
    val isOpenUpsertOvertakenAtSameInstant = RowClassifier.isOpenUpsert(
      recordStartAt = recordStartAtField,
      startAt = startAtCol,
      endAt = endAtCol
    ) && (effectiveRecordStartAt <=> nextEffectiveRecordStartAt)

    // Rule 3: decomposition tail whose endAt coincides with the next row's effective
    // sequence. The explicit `nextEffectiveRecordStartAt.isNotNull` is redundant given
    // the strict `===` against a non-null tail endAt, but kept for clarity.
    val isDecompositionTailOvertakenAtSameInstant =
      RowClassifier.isDecompositionTail(recordStartAtField) &&
        nextEffectiveRecordStartAt.isNotNull &&
        endAtCol === nextEffectiveRecordStartAt

    withWindowCols
      .filter(!(
        isSequencingDimensionDuplicate ||
          isOpenUpsertOvertakenAtSameInstant ||
          isDecompositionTailOvertakenAtSameInstant
      ))
      // Drop the temporary window columns so the output schema matches the input.
      .drop(
        Scd2BatchProcessor.nextEffectiveRecordStartAtColName,
        Scd2BatchProcessor.nextStartAtColName,
        Scd2BatchProcessor.nextEndAtColName
      )
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
 * Lifetime invariant: per key, no two target-table persisted rows share the same
 * [[recordStartAtFieldName]] value.
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
 * Lifetime invariant: per key, no two aux-table persisted rows share the same
 * [[recordStartAtFieldName]] value.
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
   * Aux-table only column that holds the microbatch id by which a row was logically
   * deleted (null if the row is still live). Future microbatches must treat any row with a
   * non-null value here, other than the current batch's id, as deleted.
   *
   * Logically-deleted rows exist as a concept on the auxiliary table to provide
   * idempotency, should a microbatch fail between a MERGE executed against the auxiliary
   * table and the MERGE executed against the target table.
   */
  private[autocdc] val deletedByBatchIdColName: String =
    s"${AutoCdcReservedNames.prefix}deleted_by_batch_id"

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
   * Column names reserved by AutoCDC that will be projected onto the microbatch and
   * eventually persisted in the target table. If the user's source dataframe contains any of
   * these columns, SCD2 reconciliation will fail.
   *
   * TODO(SPARK-57251): validate at [[AutoCdcMergeFlow]] construction time that the source
   *   schema and column selection do not collide with these reserved names, so we fail fast
   *   with a user-actionable error instead of silently overwriting them at preprocess time.
   */
  private val reservedFrameworkColNames: Set[String] = Set(
    startAtColName,
    endAtColName,
    AutoCdcReservedNames.cdcMetadataColName
  )

  /**
   * Name of temporary column projected onto microbatch to compute the min sequencing value per
   * key within the microbatch.
   *
   * Transient in that the column has no observable side affect or persistence across microbatches.
   */
  private[autocdc] val minSequenceColName: String = s"${AutoCdcReservedNames.prefix}min_sequence"

  /**
   * Name of temporary column projected onto intermediary dataframe during decomposition to track
   * the next row's record start at when in sorted in chronological order as per
   * [[orderChronologicallyPerKeyWindow]].
   *
   * Transient in that the column has no observable side affect or persistence across microbatches.
   */
  private[autocdc] val nextRecordStartAtColName: String =
    s"${AutoCdcReservedNames.prefix}next_record_start_at"

  /**
   * Name of temporary column projected onto intermediary dataframe during decomposition that
   * stores the child rows that result from decomposing a parent row.
   *
   * Transient in that the column has no observable side affect or persistence across microbatches.
   */
  val decompositionExplodedColName = s"${{AutoCdcReservedNames.prefix}}decompose_output"

  /**
   * Names of temporary columns used by [[dropRedundantRowsPostDecomposition]] to reference CDC
   * metadata for successor rows in chronologically sorted order.
   *
   * Transient in that the column has no observable side affect or persistence across microbatches.
   */
  private[autocdc] val nextStartAtColName: String = s"${AutoCdcReservedNames.prefix}next_start_at"
  private[autocdc] val nextEndAtColName: String = s"${AutoCdcReservedNames.prefix}next_end_at"
  private[autocdc] val nextEffectiveRecordStartAtColName: String =
    s"${AutoCdcReservedNames.prefix}next_effective_record_start_at"
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

  /**
   * Schema of the CDC metadata struct column for SCD2 rows.
   */
  private[pipelines] def cdcMetadataColSchema(sequencingType: DataType): StructType =
    StructType(
      Seq(
        // The sequence value of the originating CDC event for this row. Nullable because
        // decomposition tails, which are transient and synthetically constructed during
        // reconciliation, have a null record start at.
        StructField(recordStartAtFieldName, sequencingType, nullable = true)
      )
    )

  /**
   * Construct the CDC metadata struct column for SCD2 rows, following the exact schema and
   * field ordering defined by [[cdcMetadataColSchema]].
   */
  private def constructCdcMetadataCol(
      recordStartAt: Column,
      sequencingType: DataType
  ): Column = {
    val cdcMetadataFieldsInOrder = cdcMetadataColSchema(sequencingType).fields.map { field =>
      val value = field.name match {
        case `recordStartAtFieldName` => recordStartAt
        case other =>
          throw SparkException.internalError(
            s"Unable to construct SCD2 CDC metadata column due to unknown `${other}` field."
          )
      }
      value.cast(field.dataType).as(field.name)
    }
    F.struct(cdcMetadataFieldsInOrder.toImmutableArraySeq: _*)
  }
}

object RowClassifier {

  /**
   * Synthetic right boundary created by splitting a closed row; identified by
   * `recordStartAt = null`. Never shows up in the target or aux tables.
   */
  def isDecompositionTail(recordStartAt: Column): Column = recordStartAt.isNull

  /**
   * Upsert row that is currently open in the visible timeline. Hidden no-op upserts are
   * also open until reconciliation decides whether they should stay hidden.
   *
   * Defined positively rather than as the composition of row-kind negations so that
   * any future row kind fails closed: a row whose shape doesn't match the canonical
   * open-upsert invariants gets classified as not-an-open-upsert, surfacing as missing
   * rows downstream rather than silently leaking through upsert-conditional logic.
   */
  def isOpenUpsert(
      recordStartAt: Column,
      startAt: Column,
      endAt: Column
  ): Column =
    recordStartAt.isNotNull &&
      startAt.isNotNull &&
      endAt.isNull

  /**
   * Any row with a closed interval. When `includeTombstones` is true, this
   * includes instantaneous delete boundaries; otherwise it only matches
   * visible historical upsert rows.
   */
  def isClosedRow(
      recordStartAt: Column,
      startAt: Column,
      endAt: Column,
      includeTombstones: Boolean
  ): Column =
    recordStartAt.isNotNull &&
      startAt.isNotNull &&
      endAt.isNotNull &&
      (if (includeTombstones) startAt <= endAt else startAt < endAt)
}
