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
 * @param reconciliationStrategy Strategy used to reconcile the microbatch. In the default AutoCDC
 *                               execution mode an event wins wholesale and all columns share its
 *                               row-level version. Modes such as ignore-null can reconcile leaves
 *                               independently because different events may author them, and
 *                               therefore require a different reconciliation strategy.
 */
case class Scd1BatchProcessor(
    changeArgs: ChangeArgs,
    resolvedSequencingType: DataType,
    reconciliationStrategy: Scd1ReconciliationStrategy = Scd1RowLevelReconciliation) {

  /** Reconciles a validated CDC microbatch into the form consumed by the table merges. */
  private[autocdc] def reconcileMicrobatch(
      validatedBatchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame =
    reconciliationStrategy.reconcileMicrobatch(
      changeArgs = changeArgs,
      resolvedSequencingType = resolvedSequencingType,
      validatedBatchDf = validatedBatchDf,
      auxiliaryTableDf = auxiliaryTableDf
    )

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
