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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{CreateMap, If, Literal, RaiseError}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.{DataFrame, ExpressionUtils}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StringType, StructField,
  StructType}
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
    val startAtCol = F.col(Scd2BatchProcessor.startAtColName)
    val endAtCol = F.col(Scd2BatchProcessor.endAtColName)
    val row = Scd2IntervalColumns(recordStartAtCol, startAtCol, endAtCol)

    // Order by effective recordStartAt. The decomposition-tail fallback - tails carry a null
    // recordStartAt and order by their endAt instead - is encapsulated in
    // Scd2IntervalColumns.effectiveRecordStartAt, so no tail special-casing is needed here.
    val effectiveRecordStartAt = row.effectiveRecordStartAt.asc

    val orderDecompositionTailsFirst = RowClassifier.isDecompositionTail(row).desc

    val orderUpsertRepresentingRowsFirst = RowClassifier.isUpsertRepresentingRow(row).desc

    Window
      .partitionBy(keysQuoted.map(F.col): _*)
      .orderBy(
        // Primary sort key: the source-CDC sequence time. Users are required to guarantee
        // events emitted by their source have a unique sequence per key; violating this is
        // publicly documented as undefined behavior.
        //
        // Decomposition tails are synthetic rows created during reconciliation; they have a
        // null recordStartAt and use their endAt as their _effective_ recordStartAt. Under
        // a compliant source they are the only rows that may legitimately tie with another
        // row on effective recordStartAt: a tail inherits its endAt from its parent closed
        // row, and that endAt is by construction the recordStartAt of the event that
        // originally closed the run, so the tail ties with that closing event whenever
        // both appear in the same partition.
        effectiveRecordStartAt,
        // Among rows tied on effective recordStartAt, decomposition tails sort first so a
        // tail can detect its own redundancy via LEAD(1): if the next row is a non-tail at
        // the same instant, the synthetic close the tail encodes is already represented by
        // that event and the tail is dropped downstream.
        //
        // Any tiebreaking beyond this rule only meaningfully fires when the user's source
        // has emitted two or more events at the same sequence, violating the uniqueness
        // contract above. Behavior in that case is publicly undefined and the remaining
        // tiebreaker clauses exist as a best-effort to keep retries and replays deterministic.
        orderDecompositionTailsFirst,
        // Upsert-representing rows sort before tombstones because rows detect if they are being
        // bisected by LEAD(1). This allows upserts to match against same-sequence deletes, an
        // arbitrary but deterministic convention. When this happens, the delete event will survive
        // and persist as a tombstone in the auxiliary table.
        orderUpsertRepresentingRowsFirst
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
   *   a dataframe containing one row per distinct key, with the key columns
   *   ([[ChangeArgs.keys]]) and a [[minSequenceColName]] column carrying the min
   *   [[recordStartAtFieldName]] across that key's records in the microbatch.
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
   *   the underlying Spark streaming query's batchId, used to scope aux-row visibility for
   *   replay-stability across retries of the same microbatch.
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
        // [[deletedByBatchIdColName]] carries the batchId whose MERGE logically deleted the
        // row, or null on live aux rows. Rows deleted by other batches are excluded - those
        // decisions are final. Rows deleted by THIS batch are re-included - this can only
        // happen when the current execution is a retry of a partially-failed prior attempt of
        // the same microbatch.
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
    // we have enough information to compute the full set of auxiliary rows that may affect or
    // be affected by the microbatch. Membership here is a conservative superset: every row that
    // could possibly participate in reconciliation is included, but downstream reconciliation
    // determines the actual outcome per row.
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
   * are temporary: downstream reconciliation drops them when a coincident non-tail row
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
   *   [[cdcMetadataColName]] conforms to [[cdcMetadataColSchema]]. Decomposition tails
   *   (rows with [[recordStartAtFieldName]] = null) MUST NOT be present on input - they are
   *   produced exclusively by this function.
   * @return
   *   a dataframe with the same schema as the input. Every closed non-tombstone row that
   *   was bisected has been replaced by its head + tail pair; every other row is carried
   *   through as-is. Each output row can be classified as one of: {decomposition head,
   *   decomposition tail, tombstone, open upsert, closed-and-unbisected row}. Tombstones here
   *   are aux-table-only rows representing early-arriving deletes (SCD2 has no tombstone in the
   *   target table - a delete there is just a closed history record); see the "tombstone" and
   *   "aux table" concepts in this class's scaladoc. It's possible that some of the returned
   *   decomposition tails are logically redundant, as deletion markers that are immediately
   *   overtaken by a succeeding row.
   */
  private[autocdc] def decomposeOutOfOrderRows(rowsToDecomposePerKey: DataFrame): DataFrame = {
    val recordStartAtField =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    val startAtCol = F.col(Scd2BatchProcessor.startAtColName)
    val endAtCol = F.col(Scd2BatchProcessor.endAtColName)
    val nextRecordStartAt = F.col(Scd2BatchProcessor.nextRecordStartAtColName)

    // Track the next (in sorted order) row's recordStartAt in a temporary column.
    val rowsToDecomposeWithWindowCols = rowsToDecomposePerKey.withColumn(
      Scd2BatchProcessor.nextRecordStartAtColName,
      F.lead(recordStartAtField, 1).over(orderChronologicallyPerKeyWindow)
    )

    val isClosedUpsertRow = RowClassifier.isClosedUpsert(
      Scd2IntervalColumns(recordStartAtField, startAtCol, endAtCol)
    )
    val nextRowBisectsCurrentRow =
      nextRecordStartAt.isNotNull && nextRecordStartAt < endAtCol
    val rowShouldDecompose = isClosedUpsertRow && nextRowBisectsCurrentRow

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
          withOriginalSchemaPreserved(colName, F.col(colName))
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
            Scd2BatchProcessor.constructCdcMetadataCol(
              recordStartAt = F.lit(null).cast(resolvedSequencingType),
              sequencingType = resolvedSequencingType
            )
          )
        case colName =>
          withOriginalSchemaPreserved(colName, F.col(colName))
      }
      F.struct(fields: _*)
    }

    // No-op decomposition carries over the row exactly as-is.
    def constructNoopDecomposedRow: Column = {
      val fields = originalCols.map(colName =>
        withOriginalSchemaPreserved(colName, F.col(colName))
      )
      F.struct(fields: _*)
    }

    // If a row is bisected by its window-order successor, decompose it into a head + tail
    // pair. Otherwise pass through as a single-element array so the explode below is uniform.
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
   * Asserts that every row in `decomposedRowsPerKey` conforms to one of the four canonical
   * post-decomposition shapes - tombstone, open upsert, closed upsert, or decomposition
   * tail - and is otherwise a structural identity transform.
   *
   * @param decomposedRowsPerKey
   *   the output of [[decomposeOutOfOrderRows]]: a dataframe conforming to the canonical
   *   SCD2 row schema `[user_cols..., [[startAtColName]], [[endAtColName]],
   *   [[cdcMetadataColName]]]`.
   * @return
   *   a dataframe with the exact same schema and rows as the input. Failing the
   *   well-formedness check is treated as an internal-invariant violation: at execution
   *   time, the first ill-formed row encountered aborts the query with a SparkRuntimeException
   */
  private[autocdc] def assertWellFormedRowsPostDecomposition(
      decomposedRowsPerKey: DataFrame,
      batchId: Long
  ): DataFrame = {
    val recordStartAtField =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    val startAtCol = F.col(Scd2BatchProcessor.startAtColName)
    val endAtCol = F.col(Scd2BatchProcessor.endAtColName)

    val row = Scd2IntervalColumns(recordStartAtField, startAtCol, endAtCol)
    val isWellFormedRow =
      RowClassifier.isDecompositionTail(row) ||
        RowClassifier.isTombstone(row) ||
        RowClassifier.isUpsertRepresentingRow(row)

    def stringOrNullLit(c: Column): Column = F.coalesce(c.cast(StringType), F.lit("null"))
    val malformedRowDiagnostic = F.concat(
      F.lit(
        s"During SCD2 reconciliation of microbatch [id=${batchId}], encountered a " +
        "post-decomposition row of unexpected shape:"
      ),
      F.lit(s" ${Scd2BatchProcessor.recordStartAtFieldName}="),
      stringOrNullLit(recordStartAtField),
      F.lit(s", ${Scd2BatchProcessor.startAtColName}="),
      stringOrNullLit(startAtCol),
      F.lit(s", ${Scd2BatchProcessor.endAtColName}="),
      stringOrNullLit(endAtCol),
      F.lit(".")
    )

    val internalErrorOnMalformed = ExpressionUtils.column(
      If(
        predicate = ExpressionUtils.expression(isWellFormedRow),
        trueValue = Literal(null, BooleanType),
        falseValue = RaiseError(
          Literal("INTERNAL_ERROR"),
          CreateMap(Seq(
            Literal("message"),
            ExpressionUtils.expression(malformedRowDiagnostic)
          )),
          BooleanType
        )
      )
    )
    decomposedRowsPerKey.filter(internalErrorOnMalformed.isNull)
  }

  /**
   * Drops rows that are redundant within the per-key chronological window output by
   * [[decomposeOutOfOrderRows]]. The redundancy criterion is a single rule:
   *
   *   A row is redundant whenever it sorts before some other row with the same effective
   *   recordStartAt under [[orderChronologicallyPerKeyWindow]]; equivalently, only the
   *   trailing row in any such tie survives.
   *
   * The window's tiebreakers (see [[orderChronologicallyPerKeyWindow]]) put the more
   * informative row last in every meaningful collision: a decomposition tail drops in
   * favor of the coincident real event that already encodes the same closure, and an
   * upsert drops in favor of a same-sequence tombstone (delete wins over upsert at the
   * same instant).
   *
   * @param decomposedRowsPerKey
   *   the output of [[decomposeOutOfOrderRows]]: a dataframe conforming to the canonical
   *   SCD2 row schema `[user_cols..., [[startAtColName]], [[endAtColName]],
   *   [[cdcMetadataColName]]]`.
   * @return
   *   a dataframe conforming to the same schema as the input. Per key, no two rows in the
   *   returned dataframe will share the same effective record-start-at.
   */
  private[autocdc] def dropRedundantRowsPostDecomposition(
      decomposedRowsPerKey: DataFrame): DataFrame = {
    val recordStartAtField =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    val startAtCol = F.col(Scd2BatchProcessor.startAtColName)
    val endAtCol = F.col(Scd2BatchProcessor.endAtColName)
    val effectiveRecordStartAt =
      Scd2IntervalColumns(recordStartAtField, startAtCol, endAtCol).effectiveRecordStartAt

    val withNextEffectiveRecordStartAt = decomposedRowsPerKey.withColumn(
      Scd2BatchProcessor.nextEffectiveRecordStartAtColName,
      F.lead(effectiveRecordStartAt, 1).over(orderChronologicallyPerKeyWindow)
    )
    val nextEffectiveRecordStartAt =
      withNextEffectiveRecordStartAt.col(Scd2BatchProcessor.nextEffectiveRecordStartAtColName)

    // A row is redundant whenever it ties with its immediate window-order successor on
    // effective recordStartAt. Window tiebreakers ensure the *leading* row in any such tie
    // is the one to drop.
    val isRedundantAtSameEffectiveSequence =
      effectiveRecordStartAt <=> nextEffectiveRecordStartAt

    withNextEffectiveRecordStartAt
      .filter(!isRedundantAtSameEffectiveSequence)
      .drop(Scd2BatchProcessor.nextEffectiveRecordStartAtColName)
  }

  /**
   * Recompute every row's [[startAtColName]] and [[endAtColName]] over the per-key chronological
   * window so the dataframe reflects the canonical SCD2 timeline that the downstream aux- and
   * target-table merges consume.
   *
   * Decomposition tails and tombstones round-trip unchanged. An open upsert may close at its
   * successor's effective sequence (becoming closed); the closing successor may be either a
   * later state-changing upsert or a delete, which is represented as an instantaneous tombstone
   * (see the "tombstone" concept in this class's scaladoc), so a delete closes the preceding
   * upsert at the tombstone's sequence like any other successor. A closed upsert may conversely
   * have its endAt cleared when absorbed into a run as a no-op continuation (becoming open).
   * [[recordStartAtFieldName]] is never modified.
   *
   * @param decomposedAndCleanedDf
   *   the output of [[dropRedundantRowsPostDecomposition]]: a dataframe conforming to the
   *   canonical SCD2 row schema `[user_cols..., [[startAtColName]], [[endAtColName]],
   *   [[cdcMetadataColName]]]` where every row is in one of the four canonical post-
   *   decomposition shapes (decomposition tail, tombstone, open upsert, closed upsert).
   *   If a row is a closed upsert in the input, it is assumed to not be bisected by any other
   *   row in the input.
   * @return
   *   a dataframe with the same schema and row count as the input, with each row's
   *   [[startAtColName]] / [[endAtColName]] replaced by their reconciled values.
   */
  private[autocdc] def reconcileStartAndEndAt(
      decomposedAndCleanedDf: DataFrame): DataFrame = {
    val trackedHistoryColumns = computeTrackedHistoryColumns(decomposedAndCleanedDf)

    val recordStartAtField =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    val startAtCol = F.col(Scd2BatchProcessor.startAtColName)
    val endAtCol = F.col(Scd2BatchProcessor.endAtColName)

    // Decomposition tails carry no recordStartAt of their own, so they take the closing
    // sequence (`endAt`) as their effective ordering position - the same convention used by
    // [[orderChronologicallyPerKeyWindow]] and [[dropRedundantRowsPostDecomposition]].
    val current = Scd2IntervalColumns(recordStartAtField, startAtCol, endAtCol)
    val previous = current.lagBy(1, orderChronologicallyPerKeyWindow)
    val next = current.leadBy(1, orderChronologicallyPerKeyWindow)

    // A row is the last in its per-key window when `LEAD(1)` has no successor; a constant
    // literal is sufficient since we only care whether one exists.
    val isLastRowInKeyWindow =
      F.lead(F.lit(true), 1).over(orderChronologicallyPerKeyWindow).isNull

    // The current row's tracked-history equality is computed against both its predecessor and
    // its successor so the same window scan can decide both run-head start (LAG-side) and no-op
    // continuation closure (LEAD-side) without an extra pass. The comparison is null-safe
    // (`<=>`), so two rows with matching null values in the same tracked column register as
    // equal. An empty tracked-history column set collapses to a constant `true`, which makes
    // every consecutive upsert pair a no-op continuation - the correct degenerate behavior when
    // the user tracks nothing.
    val areTrackedColumnsEqualInPreviousRow = trackedHistoryColumns
      .map { c =>
        val col = F.col(QuotingUtils.quoteIdentifier(c))
        col <=> F.lag(col, 1).over(orderChronologicallyPerKeyWindow)
      }
      .reduceOption(_ && _)
      .getOrElse(F.lit(true))

    val areTrackedColumnsEqualInNextRow = trackedHistoryColumns
      .map { c =>
        val col = F.col(QuotingUtils.quoteIdentifier(c))
        col <=> F.lead(col, 1).over(orderChronologicallyPerKeyWindow)
      }
      .reduceOption(_ && _)
      .getOrElse(F.lit(true))

    // Reconciliation of start/end at is dependent on the class of row being reconciled. Build
    // row classification predicates.
    val isDecompositionTail = RowClassifier.isDecompositionTail(current)
    val isUpsertRepresentingRow = RowClassifier.isUpsertRepresentingRow(current)

    // From the previous row's perspective, the current row is its successor.
    val previousIsNoOpUpsertWithCurrent =
      RowClassifier.isNoOpUpsertContinuation(
        row = previous,
        next = current,
        areTrackedColumnsEqual = areTrackedColumnsEqualInPreviousRow
      )

    // "Window-local run head" means the current row begins a new run within the affected
    // window. The first row in the window is automatically considered local-run-head since
    // there's no predecessor to coalesce with. A non-first row is a local run head iff its
    // predecessor is not a no-op continuation that absorbs it.
    val isWindowLocalUpsertRunHead =
      isUpsertRepresentingRow && !previousIsNoOpUpsertWithCurrent
    val isFirstRowInKeyWindow = previous.effectiveRecordStartAt.isNull
    val runHeadStartAt =
      F.when(
        isWindowLocalUpsertRunHead,
        // The first row in the window may be a window-local run head but not a global run
        // head (e.g., an aux anchor row pulled in for left context). In that case, `startAt`
        // may be strictly less than `recordStartAt`, encoding the true global run start, and
        // we propagate it forward to later in-window continuations of the same run.
        // For every later window-local upsert run head, `recordStartAt` is the run start.
        F.when(isFirstRowInKeyWindow, startAtCol).otherwise(recordStartAtField)
      )

    // Propagate the run head's `startAt` forward to every row in the run via a running
    // `last(...)` over `[unboundedPreceding, currentRow]`. `runHeadStartAt` is non-null
    // only on run heads, and `ignoreNulls = true` makes intermediate rows inherit the most
    // recent head's value.
    val runStartAt =
      F.last(runHeadStartAt, ignoreNulls = true).over(
        orderChronologicallyPerKeyWindow.rowsBetween(
          Window.unboundedPreceding,
          Window.currentRow
        )
      )

    val currentIsNoOpUpsertWithNext =
      RowClassifier.isNoOpUpsertContinuation(
        row = current,
        next = next,
        areTrackedColumnsEqual = areTrackedColumnsEqualInNextRow
      )

    val finalStartAt =
      F.when(isDecompositionTail, F.lit(null).cast(resolvedSequencingType))
        .when(isUpsertRepresentingRow, runStartAt)
        .otherwise(startAtCol)

    val finalEndAt =
      F.when(isDecompositionTail, endAtCol)
        .when(isLastRowInKeyWindow, endAtCol)
        // A no-op continuation collapses into its run head, so its own visible interval is
        // erased by clearing `endAt` to null. Note this cleared `endAt` is NOT by itself the
        // aux-routing signal: the run tail is the row that represents the run to the user, and
        // a tail can also carry `endAt = null` (an open run whose latest event is still active),
        // so `endAt = null` alone does not mean "route to aux". Deciding which reconciled rows
        // land in the target table (the run tail) versus the aux table (interior no-op rows)
        // happens in a later transform (SPARK-57378), keyed off the reconciled run shape rather
        // than `endAt` alone. A run therefore never fully disappears from the target.
        .when(currentIsNoOpUpsertWithNext, F.lit(null).cast(resolvedSequencingType))
        // The row already closes strictly before the next event (e.g., a tombstone or a
        // closed upsert that ended before the next event arrived), so there is nothing to
        // re-close.
        .when(
          RowClassifier.rowClosesStrictlyBeforeNextRow(endAtCol, next.effectiveRecordStartAt),
          endAtCol)
        .otherwise(next.effectiveRecordStartAt)

    // Stage the recomputed start/end values into temporary columns first, then project them
    // back over the originals via `select`. Both staged values reference the original
    // `startAt`/`endAt`, so we cannot replace the originals in a single `withColumn` step.
    val staged = decomposedAndCleanedDf
      .withColumn(Scd2BatchProcessor.finalStartAtColName, finalStartAt)
      .withColumn(Scd2BatchProcessor.finalEndAtColName, finalEndAt)

    // Determine columns for selection from staged dataframe using the original input dataframe's
    // schema. The final startAt/endAt should be remapped, all other columns should pass through
    // as-is.
    val outputColumns = decomposedAndCleanedDf.columns.map {
      case col if col == Scd2BatchProcessor.startAtColName =>
        val startAtMetadata = decomposedAndCleanedDf.schema(col).metadata
        F.col(Scd2BatchProcessor.finalStartAtColName)
          .as(Scd2BatchProcessor.startAtColName, startAtMetadata)
      case col if col == Scd2BatchProcessor.endAtColName =>
        val endAtMetadata = decomposedAndCleanedDf.schema(col).metadata
        F.col(Scd2BatchProcessor.finalEndAtColName)
          .as(Scd2BatchProcessor.endAtColName, endAtMetadata)
      case col =>
        F.col(QuotingUtils.quoteIdentifier(col))
    }
    staged.select(outputColumns.toImmutableArraySeq: _*)
  }

  /**
   * Drop delete-encoded rows (tombstones and decomposition tails) that became redundant after
   * reconciliation.
   *
   * The rule is the same for both row kinds: a delete-encoded row encodes a delete boundary in its
   * [[endAtColName]], and it is redundant once the immediately preceding reconciled upsert closes
   * (its [[endAtColName]]) exactly on that same boundary. When they coincide, the closed upsert
   * already carries the boundary, so the standalone delete-encoded row can be dropped rather than
   * routed to aux. Only the preceding upsert's `endAt` matters here; its `startAt` (identity) is
   * irrelevant to the comparison.
   *
   * Both examples reduce to that single check - the preceding upsert's reconciled `endAt` equals
   * the delete-encoded row's boundary:
   *   - Tombstone: an open upsert `[startAt=10, endAt=null)` followed by a tombstone at `15`
   *     reconciles into a closed upsert `[10, 15)`. Its `endAt=15` matches the tombstone's
   *     boundary `15`, so the tombstone is redundant.
   *   - Decomposition tail: an existing closed `[10, 20)` is bisected by an out-of-order event at
   *     `15`. Decomposition first splits the original row into an open head `[10, null)` plus a
   *     decomposition tail `[null, 20)` - a synthetic row with a null recordStartAt that carries
   *     the original right boundary `20` so it can be re-closed later. The bisecting event then
   *     reconciles into `[15, 20)`, whose `endAt=20` matches the tail's boundary `20`, so the tail
   *     is redundant. (A tail that does *not* coincide with a preceding upsert survives and is
   *     later turned into a tombstone by [[promoteDecompositionTailsToTombstones]].)
   */
  private[autocdc] def dropLeftoverDeletesPostReconciliation(
      reconciledDf: DataFrame): DataFrame = {
    val recordStartAt =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    val startAt = F.col(Scd2BatchProcessor.startAtColName)
    val endAt = F.col(Scd2BatchProcessor.endAtColName)

    // Both tombstones and decomposition tails encode a delete boundary in their `endAt`. Either
    // becomes redundant when the immediately preceding upsert was reconciled to close exactly on
    // that boundary, since the resulting closed upsert already carries it.
    val row = Scd2IntervalColumns(recordStartAt, startAt, endAt)
    val isTombstone = RowClassifier.isTombstone(row)
    val isDecompositionTail = RowClassifier.isDecompositionTail(row)
    val isDeleteEncodedRow = isTombstone || isDecompositionTail

    // The immediately preceding chronologically-ordered row for the same key.
    val previous = row.lagBy(1, orderChronologicallyPerKeyWindow)

    val withWindowCols = reconciledDf
      .withColumn(
        Scd2BatchProcessor.isRedundantDeleteEncodingColName,
        isDeleteEncodedRow &&
          // Defensive: the predecessor should always be an upsert, because upstream cleanup
          // prevents adjacent delete-encoded rows sharing the same boundary from reaching here.
          // Asserting it directly keeps this method's redundancy rule matching its documented
          // "immediately preceding upsert" invariant, rather than dropping a delete-encoded row
          // that merely happens to abut another delete-encoded row on the same boundary.
          RowClassifier.isUpsertRepresentingRow(previous) &&
          (previous.endAt <=> endAt)
      )

    withWindowCols
      .filter(!F.col(Scd2BatchProcessor.isRedundantDeleteEncodingColName))
      .drop(Scd2BatchProcessor.isRedundantDeleteEncodingColName)
  }

  /**
   * Convert surviving decomposition tails into tombstones.
   *
   * A decomposition tail that survives deletion cleanup is an unmatched delete boundary; setting
   * [[recordStartAtFieldName]], and [[startAtColName]] to the tail's end sequence lets downstream
   * aux handling preserve it as a tombstone.
   *
   * For example, if an existing closed upsert `[startAt=10, endAt=20)` is bisected by an event at
   * `15`, decomposition first produces an open head `[10, null)` plus a tail `[null, 20)`;
   * reconciliation may close the head as `[10, 15)`, leaving the tail's boundary at `20` to be
   * promoted into a tombstone.
   */
  private[autocdc] def promoteDecompositionTailsToTombstones(
      reconciledDf: DataFrame): DataFrame = {
    val recordStartAt =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    val startAt = F.col(Scd2BatchProcessor.startAtColName)
    val endAt = F.col(Scd2BatchProcessor.endAtColName)
    val isDecompositionTail =
      RowClassifier.isDecompositionTail(Scd2IntervalColumns(recordStartAt, startAt, endAt))

    val outputColumns = reconciledDf.columns.map {
      case c if c == AutoCdcReservedNames.cdcMetadataColName =>
        val metadata = reconciledDf.schema(c).metadata
        F.when(
          isDecompositionTail,
          Scd2BatchProcessor.constructCdcMetadataCol(
            recordStartAt = endAt,
            sequencingType = resolvedSequencingType
          )
        ).otherwise(F.col(c)).as(c, metadata)
      case c if c == Scd2BatchProcessor.startAtColName =>
        val metadata = reconciledDf.schema(c).metadata
        F.when(isDecompositionTail, endAt).otherwise(startAt).as(c, metadata)
      // The other cases match framework columns whose names are known and identifier-safe; only
      // this default case handles arbitrary user-column names, which may contain dots or other
      // special characters, so it is the only one that needs quoting.
      case c =>
        F.col(QuotingUtils.quoteIdentifier(c))
    }

    reconciledDf.select(outputColumns.toImmutableArraySeq: _*)
  }

  /**
   * Return the schema field names of columns selected for history-tracking on `df`:
   * the eligible user-data columns (those not in [[ChangeArgs.keys]] or the framework
   * reserved set) filtered through [[ChangeArgs.trackHistorySelection]].
   */
  private def computeTrackedHistoryColumns(df: DataFrame): Seq[String] = {
    val conf = df.sparkSession.sessionState.conf
    val resolver = conf.resolver

    val keyColNames = changeArgs.keys.map(_.name)
    val reservedColNames = Scd2BatchProcessor.reservedFrameworkColNames

    val eligibleSchema = StructType(df.schema.fields.filterNot { field =>
      reservedColNames.exists(resolver(_, field.name)) ||
        keyColNames.exists(resolver(_, field.name))
    })

    ColumnSelection
      .applyToSchema(
        schemaName = "trackHistorySelection",
        schema = eligibleSchema,
        columnSelection = changeArgs.trackHistorySelection,
        caseSensitive = conf.caseSensitiveAnalysis
      )
      .fieldNames
      .toImmutableArraySeq
  }

  /**
   * Tag each post-reconciliation row with [[Scd2BatchProcessor.shouldRouteToAuxTableColName]]:
   * `true` for rows that belong in the auxiliary table (tombstones and hidden no-op upserts),
   * `false` for rows that do not. The flag's contract is solely about aux-table membership; it
   * makes no claim about whether a `false` row belongs in the target table.
   *
   * @param reconciledDf the canonical post-reconciliation rows for the affected keys.
   * @return `reconciledDf` with an added boolean
   *         [[Scd2BatchProcessor.shouldRouteToAuxTableColName]] column; no rows are added or
   *         removed.
   */
  private[autocdc] def identifyAndTagAuxRows(reconciledDf: DataFrame): DataFrame = {
    val recordStartAt =
      Scd2BatchProcessor.recordStartAtOf(F.col(AutoCdcReservedNames.cdcMetadataColName))
    val startAt = F.col(Scd2BatchProcessor.startAtColName)
    val endAt = F.col(Scd2BatchProcessor.endAtColName)
    val current = Scd2IntervalColumns(recordStartAt, startAt, endAt)
    val next = current.leadBy(1, orderChronologicallyPerKeyWindow)

    val trackedHistoryColumns = computeTrackedHistoryColumns(reconciledDf)
    val areTrackedColumnsEqualInNextRow = trackedHistoryColumns
      .map { c =>
        val col = F.col(QuotingUtils.quoteIdentifier(c))
        col <=> F.lead(col, 1).over(orderChronologicallyPerKeyWindow)
      }
      .reduceOption(_ && _)
      .getOrElse(F.lit(true))

    val isTombstone = RowClassifier.isTombstone(current)
    val isHiddenNoOpUpsert = RowClassifier.isNoOpUpsertContinuation(
      row = current,
      next = next,
      areTrackedColumnsEqual = areTrackedColumnsEqualInNextRow
    )

    reconciledDf.withColumn(
      Scd2BatchProcessor.shouldRouteToAuxTableColName,
      isTombstone || isHiddenNoOpUpsert
    )
  }

  /**
   * Merge the reconciled rows that belong in the auxiliary table (tombstones and hidden no-op
   * upserts, as tagged by [[identifyAndTagAuxRows]]) onto the auxiliary table, and logically
   * delete any previously-affected aux row that did not survive reconciliation.
   *
   * Idempotency across `foreachBatch` retries: rows this batch logically deletes are not
   * physically removed but stamped with [[Scd2BatchProcessor.deletedByBatchIdColName]] `=
   * batchId`, so a retry of the same `batchId` still observes them via
   * [[findAffectedRowsFromAuxiliaryTable]] and re-derives the same reconciliation output. Aux
   * rows logically deleted by an older, already-committed batch
   * ([[Scd2BatchProcessor.deletedByBatchIdColName]] `!= batchId`) are physically
   * garbage-collected as part of this same merge.
   *
   * @param reconciledDfWithAuxRowsTagged reconciled rows tagged by [[identifyAndTagAuxRows]].
   * @param originalAffectedRowsFromAuxiliaryTable the affected aux rows pulled in for this
   *        microbatch, in canonical SCD2 row schema (i.e. as returned by
   *        [[findAffectedRowsFromAuxiliaryTable]], with the aux-only
   *        [[Scd2BatchProcessor.deletedByBatchIdColName]] already dropped).
   * @param auxiliaryTableIdentifier the identifier of the auxiliary table to merge into.
   * @param batchId the underlying Spark streaming query's batchId, used to stamp logical deletes
   *        and scope garbage collection.
   */
  private[autocdc] def mergeRowsIntoAuxiliaryTable(
      reconciledDfWithAuxRowsTagged: DataFrame,
      originalAffectedRowsFromAuxiliaryTable: DataFrame,
      auxiliaryTableIdentifier: TableIdentifier,
      batchId: Long): Unit = {
    val resolver = reconciledDfWithAuxRowsTagged.sparkSession.sessionState.conf.resolver
    val deletedByBatchIdCol = Scd2BatchProcessor.deletedByBatchIdColName

    val reconciledAuxRows = reconciledDfWithAuxRowsTagged
      .filter(F.col(Scd2BatchProcessor.shouldRouteToAuxTableColName))
      .drop(Scd2BatchProcessor.shouldRouteToAuxTableColName)

    // All of the reconciledAuxRows will be landing in the aux table as alive (non-deleted) rows,
    // so tag them with a null deleted-by batch id.
    val auxRowsToUpsert = reconciledAuxRows
      .withColumn(deletedByBatchIdCol, F.lit(null).cast(LongType))

    // Any aux row pulled in for reconciliation but absent from the post-reconciliation aux rows
    // was either dropped as redundant or promoted to the (now-visible) target table. Either way
    // it must leave the aux table; stamp it with this batch's id for deleted-by batch id. On this
    // batch's MERGE, the existing aux row will be considered logically deleted. In a future
    // microbatch's merge, it will be physically deleted.
    val auxRowsToDelete = antiJoinRowsByRecordStartAtPerKey(
        leftRows = originalAffectedRowsFromAuxiliaryTable,
        rightRows = reconciledAuxRows
      )
      .withColumn(deletedByBatchIdCol, F.lit(batchId))

    val mergeSource = auxRowsToUpsert
      .unionByName(auxRowsToDelete)
      .as("source")

    // At this point [[mergeSource]] must have the same columns/shape as the persisted aux table.
    val auxTableColumns = mergeSource.columns.toImmutableArraySeq

    // Build predicate for whether rows represent the same event (match). In SCD2 each key can have
    // multiple records, so a row's identity is defined by (keys, sequencing).
    val auxIdentQuoted = auxiliaryTableIdentifier.quotedString
    val meta = AutoCdcReservedNames.cdcMetadataColName

    val mergeSourceRecordStartAt =
      Scd2BatchProcessor.recordStartAtOf(F.col(s"source.`$meta`"))
    val auxTableRecordStartAt =
      Scd2BatchProcessor.recordStartAtOf(F.col(s"$auxIdentQuoted.`$meta`"))

    val doKeysMatch = changeArgs.keys
      .map(k => F.col(s"source.${k.quoted}") === F.col(s"$auxIdentQuoted.${k.quoted}"))
      .reduce(_ && _)
    val doRowsMatch = doKeysMatch && (mergeSourceRecordStartAt <=> auxTableRecordStartAt)

    // On updates, MERGE requires only non-key columns are updated (remapped). For inserts, all of
    // the row's columns must explicitly be mapped.
    val keyNames = changeArgs.keys.map(_.name)
    def upsertAssignments(columnName: String): (String, Column) = {
      val quoted = QuotingUtils.quoteIdentifier(columnName)
      s"$auxIdentQuoted.$quoted" -> F.col(s"source.$quoted")
    }
    val nonKeyUpdateAssignments = auxTableColumns
      .filterNot(c => keyNames.exists(resolver(_, c)))
      .map(upsertAssignments)
      .toMap
    val insertAssignments = auxTableColumns.map(upsertAssignments).toMap

    // Physically garbage-collect aux rows logically deleted by some other, already-committed
    // batch. Such rows are always excluded when pulling in the current microbatch's affected set,
    // so they can never match against a row being merged in.
    val auxTableDeletedByBatchId = F.col(s"$auxIdentQuoted.`$deletedByBatchIdCol`")
    val isGarbageCollectableAuxRow =
      auxTableDeletedByBatchId.isNotNull && auxTableDeletedByBatchId =!= F.lit(batchId)

    // Whether a row in the MERGE source represents a row being [logically] deleted, as opposed to
    // being upserted.
    val shouldLogicallyDeleteAuxRow = F.col(s"source.`$deletedByBatchIdCol`").isNotNull

    mergeSource
      .mergeInto(auxIdentQuoted, doRowsMatch)
      // Keys in source row match against existing row in aux table, and declare intent to delete
      // the corresponding row in the aux; mark the row as logically deleted in the aux table.
      .whenMatched(shouldLogicallyDeleteAuxRow)
      .update(
        Map(
          s"$auxIdentQuoted.`$deletedByBatchIdCol`" ->
            F.col(s"source.`$deletedByBatchIdCol`")
        )
      )
      // Keys in source row match against existing row in aux table and does not represent a row
      // being deleted; update the data/operational columns
      .whenMatched(!shouldLogicallyDeleteAuxRow)
      .update(nonKeyUpdateAssignments)
      // Keys in source do not match against an existing row in aux table and does not represent a
      // row being deleted; insert the new key's row.
      .whenNotMatched(!shouldLogicallyDeleteAuxRow)
      .insert(insertAssignments)
      // If this is a row not affected by the current microbatch but is eligible for garbage
      // collection now, proactively hard-delete it.
      // TODO: This GC triggers a full scan of the aux table; revisit whether to GC only
      // periodically rather than on every microbatch.
      .whenNotMatchedBySource(isGarbageCollectableAuxRow)
      .delete()
      .merge()
  }

  /**
   * Merge the reconciled rows that are visible in the target table (run tails that are
   * upsert-representing and not routed to the aux table) onto the target table, and delete any
   * previously-affected target row that did not survive reconciliation.
   *
   * A previously-affected target row is deleted when it is absent from the post-reconciliation
   * visible rows. This covers both (a) a row reconciled away entirely (e.g. closed by a
   * coincident tombstone) and (b) a row demoted to a hidden aux row (e.g. a previously-visible
   * no-op tail superseded within its run by a later no-op upsert). In either case the
   * corresponding target row must go.
   *
   * @param reconciledDfWithAuxRowsTagged reconciled rows tagged by [[identifyAndTagAuxRows]].
   * @param affectedRowsFromTargetTable the affected target rows pulled in for this microbatch.
   * @param targetTableIdentifier the identifier of the target table to merge into.
   */
  private[autocdc] def mergeRowsIntoTargetTable(
      reconciledDfWithAuxRowsTagged: DataFrame,
      affectedRowsFromTargetTable: DataFrame,
      targetTableIdentifier: TableIdentifier): Unit = {
    val targetIdentQuoted = targetTableIdentifier.quotedString
    val meta = AutoCdcReservedNames.cdcMetadataColName

    val resolver = reconciledDfWithAuxRowsTagged.sparkSession.sessionState.conf.resolver
    val keyNames = changeArgs.keys.map(_.name)

    // Find the reconciled rows that should specifically land in the target table, as per SCD2.
    val recordStartAt =
      Scd2BatchProcessor.recordStartAtOf(reconciledDfWithAuxRowsTagged.col(meta))
    val startAt = reconciledDfWithAuxRowsTagged.col(Scd2BatchProcessor.startAtColName)
    val endAt = reconciledDfWithAuxRowsTagged.col(Scd2BatchProcessor.endAtColName)
    val isVisibleTargetRow =
      // Aux table by definition only holds rows that should not be visible to users, but are
      // required for cross-microbatch stateful reconciliation (tombstones and currently no-op
      // upserts).
      !F.col(Scd2BatchProcessor.shouldRouteToAuxTableColName) &&
        // While rows headed to aux are mutually exclusive to rows that should land in the target,
        // it's possible there are rows that belong to neither aux nor target (ex. decomposition
        // tails). Only non-no-op upsert-representing rows should land in the target, and no-op
        // upsert-representing rows would have been marked as routed to aux table; hence all
        // remaining upserts should land in the target table.
        RowClassifier.isUpsertRepresentingRow(Scd2IntervalColumns(recordStartAt, startAt, endAt))

    // Compute the set of affected rows that should be upserted back into the target table, as well
    // as rows that have been dropped or demoted out of the target table.
    val reconciledTargetRows = reconciledDfWithAuxRowsTagged
      .filter(isVisibleTargetRow)
      .drop(Scd2BatchProcessor.shouldRouteToAuxTableColName)

    val rowsToDeleteFromTarget = antiJoinRowsByRecordStartAtPerKey(
      leftRows = affectedRowsFromTargetTable,
      rightRows = reconciledTargetRows
    )

    // At this point, [[reconciledTargetRows]] shares the same columns/shape as the target table.
    val targetTableColumns = reconciledTargetRows.columns.toImmutableArraySeq

    // Unlike the aux table the target table does not persist any delete marker (logical or not),
    // so we need to augment a column to all MERGE source rows to indicate whether the row
    // represents that a corresponding target table row should be deleted. Once we explicitly mark
    // each row as a deletion or not, we can union them without losing information.
    val shouldDeleteTargetRowCol = Scd2BatchProcessor.shouldDeleteTargetRowColName
    val mergeSource = reconciledTargetRows
      .withColumn(shouldDeleteTargetRowCol, F.lit(false))
      .unionByName(rowsToDeleteFromTarget.withColumn(shouldDeleteTargetRowCol, F.lit(true)))
      .select(
        targetTableColumns.map(c => F.col(QuotingUtils.quoteIdentifier(c))) :+
          F.col(shouldDeleteTargetRowCol): _*
      )
      .as("source")

    val mergeSourceRecordStartAt = Scd2BatchProcessor.recordStartAtOf(F.col(s"source.`$meta`"))
    val targetTableRecordStartAt =
      Scd2BatchProcessor.recordStartAtOf(F.col(s"$targetIdentQuoted.`$meta`"))
    val doKeysMatch = changeArgs.keys
      .map(k => F.col(s"source.${k.quoted}") === F.col(s"$targetIdentQuoted.${k.quoted}"))
      .reduce(_ && _)
    val doRowsMatch = doKeysMatch && (mergeSourceRecordStartAt <=> targetTableRecordStartAt)

    // On updates, MERGE requires only non-key columns are updated (remapped). For inserts, all of
    // the row's columns must explicitly be mapped.
    def upsertAssignments(columnName: String): (String, Column) = {
      val quoted = QuotingUtils.quoteIdentifier(columnName)
      s"$targetIdentQuoted.$quoted" -> F.col(s"source.$quoted")
    }
    val nonKeyUpdateAssignments = targetTableColumns
      .filterNot(c => keyNames.exists(resolver(_, c)))
      .map(upsertAssignments)
      .toMap
    val insertAssignments = targetTableColumns.map(upsertAssignments).toMap

    // Whether a row in the MERGE source represents a row being deleted, as opposed to being
    // upserted. Unlike the aux table, target deletes are physical (hard) deletes.
    val shouldDeleteTargetRow = F.col(s"source.`$shouldDeleteTargetRowCol`")

    mergeSource
      .mergeInto(targetIdentQuoted, doRowsMatch)
      // Keys in source row match against an existing row in the target table, and declare intent
      // to delete the corresponding row; physically remove it from the target table.
      .whenMatched(shouldDeleteTargetRow)
      .delete()
      // Keys in source row match against an existing row in the target table and does not
      // represent a row being deleted; update the data/operational columns.
      .whenMatched(!shouldDeleteTargetRow)
      .update(nonKeyUpdateAssignments)
      // Keys in source do not match against an existing row in the target table and does not
      // represent a row being deleted; insert the new key's row.
      .whenNotMatched(!shouldDeleteTargetRow)
      .insert(insertAssignments)
      .merge()
  }

  /**
   * Left-anti-join `leftRows` against `rightRows`, matching on the key columns and
   * [[Scd2BatchProcessor.recordStartAtFieldName]] (null-safe). Returns the `leftRows` that have
   * no counterpart in `rightRows` - i.e. the rows that were dropped between the two sets -
   * preserving `leftRows`' schema.
   *
   * Both inputs must carry the key columns and the [[AutoCdcReservedNames.cdcMetadataColName]]
   * column.
   */
  private[autocdc] def antiJoinRowsByRecordStartAtPerKey(
      leftRows: DataFrame,
      rightRows: DataFrame): DataFrame = {
    val leftAlias = "left"
    val rightAlias = "right"
    val leftRecordStartAt = Scd2BatchProcessor.recordStartAtOf(
      F.col(s"$leftAlias.`${AutoCdcReservedNames.cdcMetadataColName}`")
    )
    val rightRecordStartAt = Scd2BatchProcessor.recordStartAtOf(
      F.col(s"$rightAlias.`${AutoCdcReservedNames.cdcMetadataColName}`")
    )
    val doKeysMatch = changeArgs.keys
      .map(k => F.col(s"$leftAlias.${k.quoted}") === F.col(s"$rightAlias.${k.quoted}"))
      .reduce(_ && _)

    leftRows
      .as(leftAlias)
      .join(
        rightRows.as(rightAlias),
        doKeysMatch && (leftRecordStartAt <=> rightRecordStartAt),
        joinType = "left_anti"
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
 * Lifetime invariant: As per the SCD2 contract, the target table should never have overlapping
 * rows by [startAt, endAt) intervals. The target table also never persists a zero-width
 * upsert-representing row (a non-tombstone with `startAt == endAt`); such rows are dropped
 * during reconciliation.
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
 * A temporary and synthetic row produced by the batch processor during reconciliation (not
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
   * non-null value here, other than the current batch's id, as deleted, and may safely
   * physically reap them since prior microbatches commit before the next one starts.
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
   * Temporary in that the column has no observable side effect or persistence across microbatches.
   * Package level visiblity for unit-testing.
   */
  private[autocdc] val minSequenceColName: String = s"${AutoCdcReservedNames.prefix}min_sequence"

  /**
   * Name of temporary column projected onto intermediary dataframe during decomposition to track
   * the next row's record start at when in sorted in chronological order as per
   * [[orderChronologicallyPerKeyWindow]].
   *
   * Temporary in that the column has no observable side effect or persistence across microbatches.
   */
  private val nextRecordStartAtColName: String =
    s"${AutoCdcReservedNames.prefix}next_record_start_at"

  /**
   * Name of temporary column projected onto intermediary dataframe during decomposition that
   * stores the child rows that result from decomposing a parent row.
   *
   * Temporary in that the column has no observable side effect or persistence across microbatches.
   */
  private val decompositionExplodedColName: String =
    s"${AutoCdcReservedNames.prefix}decompose_output"

  /**
   * Name of temporary column used by [[dropRedundantRowsPostDecomposition]] to reference the
   * next row's effective recordStartAt in chronologically sorted order.
   *
   * Temporary in that the column has no observable side effect or persistence across microbatches.
   */
  private[autocdc] val nextEffectiveRecordStartAtColName: String =
    s"${AutoCdcReservedNames.prefix}next_effective_record_start_at"

  /**
   * Names of temporary columns used by [[reconcileStartAndEndAt]] to stage the recomputed
   * [[startAtColName]] / [[endAtColName]] values before projecting them back over the originals.
   *
   * Temporary in that the columns have no observable side effect or persistence across
   * microbatches.
   */
  private val finalStartAtColName: String =
    s"${AutoCdcReservedNames.prefix}final_start_at"
  private val finalEndAtColName: String =
    s"${AutoCdcReservedNames.prefix}final_end_at"

  /**
   * Name of the temporary column used by [[dropLeftoverDeletesPostReconciliation]] to flag
   * delete-encoded rows (tombstones and decomposition tails) already encoded by the immediately
   * preceding upsert row's [[endAtColName]].
   *
   * Temporary in that the column has no observable side effect or persistence across microbatches.
   */
  private val isRedundantDeleteEncodingColName: String =
    s"${AutoCdcReservedNames.prefix}is_redundant_delete_encoding"

  /**
   * Name of the temporary column used to identify the sequence associated with the anchor
   * row found in the auxiliary table for the incoming microbatch. Since sequences must be unique
   * amongst all rows for a key (or risk undefined behavior), this sequence value uniquely
   * identifies an exact row in the aux.
   */
  private val anchorSequenceColName: String = s"${AutoCdcReservedNames.prefix}anchor_sequence"

  /**
   * Name of the temporary column projected by [[Scd2BatchProcessor.identifyAndTagAuxRows]] to
   * mark rows destined for the auxiliary table (tombstones and hidden no-op upserts) rather than
   * the target table.
   *
   * Temporary in that the column has no observable side effect or persistence across microbatches.
   */
  private[autocdc] val shouldRouteToAuxTableColName: String =
    s"${AutoCdcReservedNames.prefix}should_route_to_aux_table"

  /**
   * Name of the temporary column used by [[Scd2BatchProcessor.mergeRowsIntoTargetTable]] to flag,
   * within the unioned merge source, which rows should be deleted from (rather than upserted
   * into) the target table.
   *
   * Temporary in that the column has no observable side effect or persistence across
   * microbatches.
   */
  private val shouldDeleteTargetRowColName: String =
    s"${AutoCdcReservedNames.prefix}should_delete_target_row"

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
        // decomposition tails, which are temporarily and synthetically constructed during
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

/**
 * The three columns that locate a row on the SCD2 timeline: its source record-start sequence
 * (`recordStartAt`, null only for decomposition tails) and the bounds of its visible interval
 * [`startAt`, `endAt`). [[RowClassifier]] classifies a row purely from this triple.
 */
private[autocdc] case class Scd2IntervalColumns(
    recordStartAt: Column,
    startAt: Column,
    endAt: Column) {

  /**
   * The row's effective ordering position. Decomposition tails carry no `recordStartAt` and
   * fall back to their closing sequence (`endAt`), the same convention used by
   * [[Scd2BatchProcessor.orderChronologicallyPerKeyWindow]].
   */
  def effectiveRecordStartAt: Column = F.coalesce(recordStartAt, endAt)

  /** The same interval columns read from the row `offset` positions earlier in `window`. */
  def lagBy(offset: Int, window: WindowSpec): Scd2IntervalColumns =
    Scd2IntervalColumns(
      F.lag(recordStartAt, offset).over(window),
      F.lag(startAt, offset).over(window),
      F.lag(endAt, offset).over(window))

  /** The same interval columns read from the row `offset` positions later in `window`. */
  def leadBy(offset: Int, window: WindowSpec): Scd2IntervalColumns =
    Scd2IntervalColumns(
      F.lead(recordStartAt, offset).over(window),
      F.lead(startAt, offset).over(window),
      F.lead(endAt, offset).over(window))
}

object RowClassifier {

  /**
   * Synthetic right boundary created by splitting a closed row, temporarily present during
   * microbatch reconciliation but never materializes in the target or aux tables.
   */
  private[autocdc] def isDecompositionTail(row: Scd2IntervalColumns): Column =
    row.recordStartAt.isNull && row.startAt.isNull && row.endAt.isNotNull

  /**
   * Upsert row that is currently open in the visible timeline. Hidden no-op upserts are
   * also open until reconciliation decides whether they should stay hidden.
   */
  private[autocdc] def isOpenUpsert(row: Scd2IntervalColumns): Column =
    row.recordStartAt.isNotNull &&
      row.startAt.isNotNull &&
      row.endAt.isNull &&
      // startAt < recordStartAt implies this row belongs to but is not the head of some
      // upsert-run, else this is the head of a run.
      row.startAt <= row.recordStartAt

  /**
   * Upsert row whose visible interval has already been closed by a strictly later event;
   * the historical counterpart to [[isOpenUpsert]].
   *
   * Notably, a zero-width [startAt, endAt) interval is not considered a valid closed upsert.
   */
  private[autocdc] def isClosedUpsert(row: Scd2IntervalColumns): Column =
    row.recordStartAt.isNotNull &&
      row.startAt.isNotNull &&
      row.endAt.isNotNull &&
      row.recordStartAt < row.endAt &&
      row.startAt < row.endAt &&
      // startAt <= recordStartAt covers both the run-head case (startAt == recordStartAt)
      // and the no-op-continuation case (startAt < recordStartAt).
      row.startAt <= row.recordStartAt

  /**
   * Any row that semantically encodes an upsert event.
   */
  private[autocdc] def isUpsertRepresentingRow(row: Scd2IntervalColumns): Column =
    isOpenUpsert(row) || isClosedUpsert(row)

  /**
   * Tombstone (delete-boundary) row, encoded as an instantaneous interval at
   * `recordStartAt`. Never materializes in the target table, only in the aux table.
   *
   * User-data column values on tombstones are not part of the SCD2 contract: they may
   * reflect the originating delete event, the values of the upsert whose closed-interval
   * row was bisected (when the tombstone was promoted from a decomposition tail), or be
   * null altogether. Reconciliation does not consume these values for any semantic
   * decision.
   */
  private[autocdc] def isTombstone(row: Scd2IntervalColumns): Column =
    row.recordStartAt.isNotNull &&
      row.startAt.isNotNull &&
      row.endAt.isNotNull &&
      row.startAt === row.recordStartAt &&
      row.endAt === row.recordStartAt

  /**
   * Whether a row closes (`endAt`) strictly before the next chronologically-ordered row for the
   * same key begins (`nextEffectiveRecordStartAt`), leaving a gap in the visible timeline.
   */
  private[autocdc] def rowClosesStrictlyBeforeNextRow(
      endAt: Column,
      nextEffectiveRecordStartAt: Column
  ): Column =
    endAt.isNotNull && endAt < nextEffectiveRecordStartAt

  /**
   * Whether `row` carries no new information beyond its immediate successor `next` and so
   * collapses into that successor's run instead of standing as its own visible interval. It is
   * the caller's responsibility to pass `row` and `next` as successive rows in chronological
   * order, and `areTrackedColumnsEqual` as true iff the two rows hold equal values for every
   * tracked-history column.
   *
   * Returns true iff `row` and `next` are both upsert-representing, `row`'s interval reaches
   * `next` without leaving a gap, and the two are tracked-history equal. It is false whenever
   * there is no successor (the last row in a key window) or either row is not upsert-representing.
   */
  private[autocdc] def isNoOpUpsertContinuation(
      row: Scd2IntervalColumns,
      next: Scd2IntervalColumns,
      areTrackedColumnsEqual: Column
  ): Column =
    isUpsertRepresentingRow(row) &&
      isUpsertRepresentingRow(next) &&
      !rowClosesStrictlyBeforeNextRow(row.endAt, next.effectiveRecordStartAt) &&
      areTrackedColumnsEqual
}
