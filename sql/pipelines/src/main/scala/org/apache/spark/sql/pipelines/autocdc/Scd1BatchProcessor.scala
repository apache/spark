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
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

/**
 * Per-microbatch processor for SCD Type 1 AutoCDC flows, complying to the specified [[changeArgs]]
 * configuration.
 *
 * @param changeArgs The CDC flow configuration.
 * @param resolvedSequencingType The post-analysis [[DataType]] of the sequencing column, derived
 *                               from the flow's resolved DataFrame at flow setup time.
 */
case class Scd1BatchProcessor(
    changeArgs: ChangeArgs,
    resolvedSequencingType: DataType) {

  /**
   * Reconcile a CDC microbatch into the canonical form that the auxiliary- and target-table
   * merges consume. Composes the per-step transforms in the only order that produces correct
   * SCD1 semantics:
   *
   *   1. [[deduplicateMicrobatch]]: collapse same-key events to the latest by sequence.
   *   2. [[extendMicrobatchRowsWithCdcMetadata]]: project the operational `_cdc_metadata` column
   *      (must run before column selection, which may drop inputs the metadata expressions
   *      reference).
   *   3. [[projectTargetColumnsOntoMicrobatch]]: apply the user-defined column selection while
   *      preserving the CDC metadata column.
   *   4. [[applyTombstonesToMicrobatch]]: filter out late-arriving events superseded by
   *      tombstones already recorded in the auxiliary table.
   *
   * The per-step methods are kept package-visible so that focused unit tests can pin each
   * transform's behavior independently. This method itself is package-visible so that
   * [[Scd1ForeachBatchHandler]] can call it after running [[ScdBatchValidator.validateMicrobatch]]
   * - validation is intentionally not folded in here, as it must run before any of these
   * transforms touch the data.
   *
   * @param batchDf          The validated incoming CDC microbatch.
   * @param auxiliaryTableDf A snapshot of the auxiliary table for tombstone reconciliation.
   *                         Must contain at minimum the key columns + `_cdc_metadata`.
   * @return The reconciled microbatch, ready to be merged onto both tables.
   */
  private[autocdc] def reconcileMicrobatch(
      batchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val deduplicated = deduplicateMicrobatch(validatedMicrobatch = batchDf)
    val withCdcMetadata = extendMicrobatchRowsWithCdcMetadata(validatedMicrobatch = deduplicated)
    val projected = projectTargetColumnsOntoMicrobatch(
      microbatchWithCdcMetadataDf = withCdcMetadata
    )
    applyTombstonesToMicrobatch(
      microbatchDf = projected,
      auxiliaryTableDf = auxiliaryTableDf
    )
  }

  /**
   * Deduplicate the incoming CDC microbatch by key, keeping the most recent event per key
   * as ordered by [[ChangeArgs.sequencing]].
   *
   * For SCD1 we only care about the most recent (by sequence value) event per key. When
   * multiple events share the same key and the same sequence value, the row selected is
   * non-deterministic and undefined.
   *
   * @param validatedMicrobatch A microbatch that has already been validated such that the
   *                            sequencing column should not contain null values, and its data type
   *                            should support ordering.
   *
   * The schema of the returned dataframe matches the schema of the microbatch exactly.
   */
  private[autocdc] def deduplicateMicrobatch(validatedMicrobatch: DataFrame): DataFrame = {
    // The `max_by` API can only return a single column, so pack/unpack the entire row into a
    // temporary column before and after the `max_by` operation.
    val winningRowCol = Scd1BatchProcessor.winningRowColName

    val allMicrobatchColumns =
      validatedMicrobatch.columns
        .map(colName => F.col(QuotingUtils.quoteIdentifier(colName)))
        .toImmutableArraySeq

    validatedMicrobatch
      .groupBy(changeArgs.keys.map(k => F.col(k.quoted)): _*)
      .agg(
        F.max_by(F.struct(allMicrobatchColumns: _*), changeArgs.sequencing)
          .as(winningRowCol)
      )
      .select(F.col(s"$winningRowCol.*"))
  }

  /**
   * Project the CDC metadata column onto the microbatch.
   *
   * This must run before any column selection is applied to the microbatch. The
   * [[ChangeArgs.deleteCondition]] and [[ChangeArgs.sequencing]] expressions are evaluated against
   * the current microbatch schema, and column selection may drop inputs required by those
   * expressions.
   *
   * Rows are classified as deletes only when [[ChangeArgs.deleteCondition]] evaluates to true. A
   * false or null delete condition classifies the row as an upsert.
   *
   * @param validatedMicrobatch A microbatch that has already been validated such that the
   *                            sequencing column should not contain null values, and its data type
   *                            should support ordering.
   *
   * The returned dataframe has all of the columns in the input microbatch + the CDC metadata
   * column.
   */
  private[autocdc] def extendMicrobatchRowsWithCdcMetadata(
      validatedMicrobatch: DataFrame): DataFrame = {
    val rowDeleteSequence: Column = changeArgs.deleteCondition match {
      case Some(deleteCondition) =>
        F.when(deleteCondition, changeArgs.sequencing).otherwise(F.lit(null))
      case None =>
        F.lit(null)
    }

    val rowUpsertSequence: Column =
      // A row that is not a delete must be an upsert, these are mutually exclusive and a complete
      // set of CDC event types.
      F.when(rowDeleteSequence.isNull, changeArgs.sequencing).otherwise(F.lit(null))

    validatedMicrobatch.withColumn(
      AutoCdcReservedNames.cdcMetadataColName,
      Scd1BatchProcessor.constructCdcMetadataCol(
        deleteSequence = rowDeleteSequence,
        upsertSequence = rowUpsertSequence,
        sequencingType = resolvedSequencingType
      )
    )
  }

  /**
   * Project the user-defined column selection onto the microbatch. By this point the input
   * microbatch should already have projected its CDC metadata, because it's possible that the
   * user-defined column selection drops columns that are otherwise necessary to compute the
   * CDC metadata.
   *
   * Returned dataframe's schema is: all of the user-selected columns in the input dataframe as per
   * [[ChangeArgs.columnSelection]] + the CDC metadata column.
   */
  private[autocdc] def projectTargetColumnsOntoMicrobatch(
      microbatchWithCdcMetadataDf: DataFrame): DataFrame = {
    val caseSensitiveColumnComparison =
      microbatchWithCdcMetadataDf.sparkSession.sessionState.conf.caseSensitiveAnalysis

    // The user schema is the microbatch schema after dropping the system CDC metadata column.
    // We project out the system column before applying user selection and project it back in
    // afterwards, so that users cannot control whether this [necessary] column shows up in the
    // target table.
    val userColumnsInMicrobatchSchema = ColumnSelection.applyToSchema(
      schemaName = "microbatch",
      schema = microbatchWithCdcMetadataDf.schema,
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName(AutoCdcReservedNames.cdcMetadataColName))
        )
      ),
      caseSensitive = caseSensitiveColumnComparison
    )

    val userSelectedColumnsInMicrobatchSchema =
      ColumnSelection.applyToSchema(
        schemaName = "microbatch",
        schema = userColumnsInMicrobatchSchema,
        columnSelection = changeArgs.columnSelection,
        caseSensitive = caseSensitiveColumnComparison
      )

    // In addition to the explicit user-selected columns, re-project the operational CDC metadata
    // column as the last column.
    val finalColumnsInMicrobatchToSelect =
      userSelectedColumnsInMicrobatchSchema.fieldNames.map(colName => {
        // Spark drops backticks in the schema, quote all identifiers for safety before executing
        // select. Identifiers could have special characters such as '.'.
        F.col(QuotingUtils.quoteIdentifier(colName))
      }) :+ F.col(
        AutoCdcReservedNames.cdcMetadataColName
      )

    microbatchWithCdcMetadataDf.select(
      finalColumnsInMicrobatchToSelect.toImmutableArraySeq: _*
    )
  }

  /**
   * Left anti-join the microbatch with the auxiliary table on tombstones that match against and
   * effectively delete late-arriving upserts (or stale deletes).
   *
   * @param microbatchDf The incoming microbatch dataframe with at minimum all of the key
   *                     columns + CDC metadata column.
   * @param auxiliaryTableDf Dataframe representing the auxiliary table, with at minimum the key
   *                         columns + CDC metadata column.
   *
   * The returned filtered dataframe has the same schema as the input microbatch, but with only
   * the rows that remain unaffected by any known tombstones.
   */
  private[autocdc] def applyTombstonesToMicrobatch(
      microbatchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val aliasedMicrobatchDf = microbatchDf.alias("microbatch")
    val aliasedAuxiliaryTableDf = auxiliaryTableDf.alias("auxiliaryTable")

    val cdcMetadata = AutoCdcReservedNames.cdcMetadataColName

    val microbatchCdcMetadata = F.col(s"microbatch.$cdcMetadata")
    val effectiveSeq = F.greatest(
      Scd1BatchProcessor.deleteSequenceOf(microbatchCdcMetadata),
      Scd1BatchProcessor.upsertSequenceOf(microbatchCdcMetadata)
    )
    val tombstoneDeleteSeq =
      Scd1BatchProcessor.deleteSequenceOf(F.col(s"auxiliaryTable.$cdcMetadata"))

    val keysMatch = changeArgs.keys
      .map { k =>
        F.col(s"microbatch.${k.quoted}") === F.col(s"auxiliaryTable.${k.quoted}")
      }
      .reduce(_ && _)

    // A microbatch row is considered late-arriving (and therefore deleted by the tombstone) when
    // the auxiliary table holds a tombstone for the same key with a strictly larger delete
    // sequence. Both late-arriving upserts and deletes are dropped.
    val microbatchRowDeletedByTombstone = effectiveSeq < tombstoneDeleteSeq

    aliasedMicrobatchDf.join(
      right = aliasedAuxiliaryTableDf,
      joinExprs = keysMatch && microbatchRowDeletedByTombstone,
      joinType = "left_anti"
    )
  }

  /**
   * Merge the reconciled (deduplicated per key) microbatch onto the auxiliary table,
   * advancing or deleting existing tombstones and inserting new tombstones for previously
   * untracked keys.
   *
   * After the merge, the auxiliary table has the same schema as before, but with the latest
   * tombstone data per key.
   *
   * @param reconciledMicrobatchDf   The deduplicated microbatch.
   * @param auxiliaryTableIdentifier The identifier of the auxiliary table.
   */
  private[autocdc] def mergeMicrobatchOntoAuxiliaryTable(
      reconciledMicrobatchDf: DataFrame,
      auxiliaryTableIdentifier: TableIdentifier
  ): Unit = {
    val auxIdentQuoted = auxiliaryTableIdentifier.quotedString
    val meta = AutoCdcReservedNames.cdcMetadataColName

    // Project the reconciled microbatch down to just keys + `_cdc_metadata`; data columns are
    // irrelevant for the auxiliary table and should not be persisted.
    val reducedMicrobatch = reconciledMicrobatchDf
      .select(changeArgs.keys.map(k => F.col(k.quoted)) :+ F.col(meta): _*)
      .as("reducedMicrobatch")

    val microbatchCdcMetadata: Column = F.col(s"reducedMicrobatch.`$meta`")
    val incomingDelete: Column = Scd1BatchProcessor.deleteSequenceOf(microbatchCdcMetadata)
    val incomingUpsert: Column = Scd1BatchProcessor.upsertSequenceOf(microbatchCdcMetadata)

    val auxCdcMetadata: Column = F.col(s"$auxIdentQuoted.`$meta`")
    val auxDelete: Column = Scd1BatchProcessor.deleteSequenceOf(auxCdcMetadata)

    val doKeysMatch = changeArgs.keys
      .map(k => F.col(s"reducedMicrobatch.${k.quoted}") === F.col(s"$auxIdentQuoted.${k.quoted}"))
      .reduce(_ && _)

    val incomingRowRepresentsDeleteEvent =
      incomingDelete.isNotNull && (incomingUpsert.isNull || incomingDelete > incomingUpsert)

    reducedMicrobatch
      .mergeInto(auxIdentQuoted, doKeysMatch)
      // Incoming delete is newer than the stored one: advance the high-water mark.
      .whenMatched(
        incomingRowRepresentsDeleteEvent && incomingDelete > auxDelete
      )
      .update(Map(s"$auxIdentQuoted.`$meta`" -> microbatchCdcMetadata))
      // Incoming upsert is newer than the stored delete: the key was re-inserted after the
      // delete, so the aux tombstone is stale - remove it to prevent unbounded growth.
      .whenMatched(
        !incomingRowRepresentsDeleteEvent && incomingUpsert >= auxDelete
      )
      .delete()
      // New delete for a key not yet tracked, add it to auxiliary table. Note that in the
      // reconciled microbatch, there is at most one event for key, which represents the latest
      // known event for the key. If the latest known event is a delete, it must be a tombstone.
      .whenNotMatched(incomingRowRepresentsDeleteEvent)
      .insertAll()
      .merge()
  }

  /**
   * Merge the reconciled (deduplicated, tombstone applied, and column selection + metadata
   * column projected) microbatch onto the target table, as per SCD1 semantics.
   *
   * Microbatch invariants:
   *   - Exactly one of {upsert, delete} version is non-null, the other is null.
   *   - There is at most one event per key, representing the latest known event for the key
   *     across the microbatch and auxiliary table.
   *
   * Target table invariants:
   *   - Target table only contains live rows; delete sequence is always null, upsert sequence
   *     is always non-null.
   *
   * @param reconciledMicrobatchDf The reconciled microbatch dataframe.
   * @param targetTableIdentifier  The identifier of the target table.
   */
  private[autocdc] def mergeMicrobatchOntoTarget(
      reconciledMicrobatchDf: DataFrame,
      targetTableIdentifier: TableIdentifier
  ): Unit = {
    val meta = AutoCdcReservedNames.cdcMetadataColName

    val destinationTableStr = targetTableIdentifier.quotedString
    // (Re-)alias the reconciled microbatch DF for easy reference for the remainder of the merge.
    val microbatchDf = reconciledMicrobatchDf.as("microbatch")

    val microbatchCdcMetadataCol = F.col(s"microbatch.`$meta`")
    val destinationCdcMetadataCol =
      F.col(s"$destinationTableStr.`$meta`")

    val microbatchDeleteVersionField =
      Scd1BatchProcessor.deleteSequenceOf(microbatchCdcMetadataCol)
    val microbatchUpsertVersionField =
      Scd1BatchProcessor.upsertSequenceOf(microbatchCdcMetadataCol)
    val destinationUpsertVersionField =
      Scd1BatchProcessor.upsertSequenceOf(destinationCdcMetadataCol)

    val keysMatch = changeArgs.keys
      .map(k =>
        F.col(s"microbatch.${k.quoted}") === F.col(s"$destinationTableStr.${k.quoted}")
      )
      .reduce(_ && _)

    // Upsert beats existing row if incoming upsert sequence is geq to the upsert sequence on
    // the target.
    val incomingWinsUpsert = microbatchUpsertVersionField.isNotNull &&
      microbatchUpsertVersionField >= destinationUpsertVersionField

    // Delete beats existing row if delete sequencing is strictly greater than the upsert
    // sequence on the target. This is an arbitrary but deliberate choice to maintain that
    // upserts get priority over deletes on duplicate sequencing.
    val incomingWinsDelete = microbatchDeleteVersionField.isNotNull &&
      microbatchDeleteVersionField > destinationUpsertVersionField

    val resolver = microbatchDf.sparkSession.sessionState.conf.resolver
    val keyNames = changeArgs.keys.map(_.name)

    def constructTargetColumnAssignmentsFromMicrobatch(columnName: String): (String, Column) = {
      // Map a column in the target table to its direct equivalent in the microbatch. Note that
      // because of target-table schema evolution during SDP dataset materialization, the
      // microbatch's columns are always a subset of (or equal to) the target's columns.
      val quotedCol = QuotingUtils.quoteIdentifier(columnName)
      s"$destinationTableStr.$quotedCol" -> F.col(s"microbatch.$quotedCol")
    }

    // Most merge implementations require that join columns are not mutated, even when the
    // mutation would be a no-op. The remaining microbatch columns (including the CDC metadata
    // column) are overwritten outright when the incoming upsert wins.
    val columnsToUpdateWhenIncomingWinsUpsert: Map[String, Column] =
      microbatchDf.columns
        .filterNot(c => keyNames.exists(resolver(_, c)))
        .map(constructTargetColumnAssignmentsFromMicrobatch)
        .toMap

    val columnsToInsertOnNewKey: Map[String, Column] =
      microbatchDf.columns
        .map(constructTargetColumnAssignmentsFromMicrobatch)
        .toMap

    microbatchDf
      .mergeInto(destinationTableStr, keysMatch)
      .whenMatched(incomingWinsDelete)
      .delete()
      .whenMatched(incomingWinsUpsert)
      .update(columnsToUpdateWhenIncomingWinsUpsert)
      // New key: only insert upserts; deletes for absent keys are no-ops for the target table
      // merge, and instead would have been inserted as tombstones into the auxiliary table.
      .whenNotMatched(microbatchDeleteVersionField.isNull)
      // When inserting a brand new row for a new key, construct column mappings from microbatch.
      // The microbatch's columns may be a strict subset of the target's columns -- e.g. the user
      // narrowed `column_list` between runs, or the source DF dropped a column. The target's
      // columns can never be a strict subset of the microbatch's, however, because SDP's schema
      // evolution always unions old and new schemas onto the target.
      .insert(columnsToInsertOnNewKey)
      .merge()
  }
}

object Scd1BatchProcessor {
  /**
   * Internal columns inserted by AutoCDC reconciliation. Source change-data-feed dataframes must
   * not contain any columns starting with [[AutoCdcReservedNames.prefix]]; the invariant is
   * enforced at [[org.apache.spark.sql.pipelines.graph.AutoCdcMergeFlow]] construction.
   */
  private[autocdc] val winningRowColName: String = s"${AutoCdcReservedNames.prefix}winning_row"

  private[pipelines] val cdcDeleteSequenceFieldName: String = "deleteSequence"
  private[pipelines] val cdcUpsertSequenceFieldName: String = "upsertSequence"

  /** Project the delete sequence out of the CDC metadata column. */
  private[autocdc] def deleteSequenceOf(cdcMetadataCol: Column): Column =
    cdcMetadataCol.getField(cdcDeleteSequenceFieldName)

  /** Project the upsert sequence out of the CDC metadata column. */
  private[autocdc] def upsertSequenceOf(cdcMetadataCol: Column): Column =
    cdcMetadataCol.getField(cdcUpsertSequenceFieldName)

  /**
   * Schema of the CDC metadata struct column for SCD1.
   */
  private[pipelines] def cdcMetadataColSchema(sequencingType: DataType): StructType =
    StructType(
      Seq(
        // The sequencing of the event if it represents a delete, null otherwise.
        StructField(cdcDeleteSequenceFieldName, sequencingType, nullable = true),
        // The sequencing of the event if it represents an upsert, null otherwise.
        StructField(cdcUpsertSequenceFieldName, sequencingType, nullable = true)
      )
    )

  /**
   * Construct the CDC metadata struct column for SCD1, following the exact schema and field
   * ordering defined by [[cdcMetadataColSchema]].
   */
  private[pipelines] def constructCdcMetadataCol(
      deleteSequence: Column,
      upsertSequence: Column,
      sequencingType: DataType): Column = {
    val cdcMetadataFieldsInOrder = cdcMetadataColSchema(sequencingType).fields.map { field =>
      val value = field.name match {
        case `cdcDeleteSequenceFieldName` => deleteSequence
        case `cdcUpsertSequenceFieldName` => upsertSequence
        case other =>
          throw SparkException.internalError(
            s"Unable to construct SCD1 CDC metadata column due to unknown `${other}` field."
          )
      }
      value.cast(field.dataType).as(field.name)
    }
    F.struct(cdcMetadataFieldsInOrder.toImmutableArraySeq: _*)
  }
}
