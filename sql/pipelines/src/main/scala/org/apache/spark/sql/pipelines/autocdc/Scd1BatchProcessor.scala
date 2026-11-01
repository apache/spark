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
   * Reconciles a validated CDC microbatch into the form consumed by the table merges.
   *
   * @param validatedBatchDf The validated CDC microbatch.
   * @param targetTableIdentifier The identifier of the target table. Leaf-level reconciliation
   *                              aligns the microbatch to its schema.
   * @param auxiliaryTableDf A snapshot of the auxiliary table.
   */
  private[autocdc] def reconcileMicrobatch(
      validatedBatchDf: DataFrame,
      targetTableIdentifier: TableIdentifier,
      auxiliaryTableDf: DataFrame): DataFrame = {
    changeArgs.ignoreNullSelection match {
      case None =>
        Scd1RowLevelReconciliation.reconcileMicrobatch(
          changeArgs = changeArgs,
          resolvedSequencingType = resolvedSequencingType,
          validatedBatchDf = validatedBatchDf,
          auxiliaryTableDf = auxiliaryTableDf
        )
      case Some(_) =>
        Scd1LeafLevelReconciliation.reconcileMicrobatch(
          changeArgs = changeArgs,
          resolvedSequencingType = resolvedSequencingType,
          validatedBatchDf = validatedBatchDf,
          targetTableIdentifier = targetTableIdentifier,
          targetTableDf =
            validatedBatchDf.sparkSession.read.table(targetTableIdentifier.quotedString),
          auxiliaryTableDf = auxiliaryTableDf
        )
    }
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
   *   - There is at most one row per key, representing the key's net change across the
   *     microbatch and auxiliary table: its latest event under row-level reconciliation, or a
   *     synthetic row combining its events under leaf-level reconciliation.
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
    val mergeStrategy = changeArgs.ignoreNullSelection match {
      case None =>
        new Scd1RowLevelTargetMerge(
          changeArgs = changeArgs,
          reconciledMicrobatchDf = reconciledMicrobatchDf,
          targetTableIdentifier = targetTableIdentifier
        )
      case Some(_) =>
        new Scd1LeafLevelTargetMerge(
          changeArgs = changeArgs,
          resolvedSequencingType = resolvedSequencingType,
          reconciledMicrobatchDf = reconciledMicrobatchDf,
          targetTableIdentifier = targetTableIdentifier
        )
    }
    mergeStrategy.merge()
  }
}

object Scd1BatchProcessor {
  private[pipelines] val cdcDeleteSequenceFieldName: String = "deleteSequence"
  private[pipelines] val cdcUpsertSequenceFieldName: String = "upsertSequence"
  private[pipelines] val versionMapFieldName: String = "__VERSION_MAP"

  /** Project the delete sequence out of the CDC metadata column. */
  private[autocdc] def deleteSequenceOf(cdcMetadataCol: Column): Column =
    cdcMetadataCol.getField(cdcDeleteSequenceFieldName)

  /** Project the upsert sequence out of the CDC metadata column. */
  private[autocdc] def upsertSequenceOf(cdcMetadataCol: Column): Column =
    cdcMetadataCol.getField(cdcUpsertSequenceFieldName)

  /**
   * When a row last authored any of the user-data leaves at `leafPaths`, according to its CDC
   * metadata: the greatest sequence at which it authored one of them. A delete authors every leaf
   * at its delete sequence, and an upsert without a version map authors every leaf at its upsert
   * sequence.
   *
   * @param cdcMetadataCol The row's CDC metadata.
   * @param leafPaths The name parts of the leaves, as keyed in the version map.
   * @return The sequence at which the row last authored any of the leaves, or null if it authored
   *         none of them. With no leaves, such as for an empty struct, this is the row's delete
   *         sequence for a delete and its upsert sequence otherwise.
   */
  private[autocdc] def lastAuthoredIn(
      cdcMetadataCol: Column,
      leafPaths: Seq[Seq[String]]): Column = {
    val deleteSequence = deleteSequenceOf(cdcMetadataCol)
    val upsertSequence = upsertSequenceOf(cdcMetadataCol)
    val versionMap = cdcMetadataCol.getField(versionMapFieldName)

    val leafSequences =
      leafPaths.map(leafPath => versionMap(Scd1VersionMap.serializeKey(leafPath)))
    val greatestLeafSequence = leafSequences match {
      // An empty struct has no leaves, and so no version-map entries.
      case Seq() => upsertSequence
      // `greatest` requires at least two arguments. If there's only sequence, trivially return it.
      case Seq(leafSequence) => leafSequence
      case _ => F.greatest(leafSequences: _*)
    }

    F.when(deleteSequence.isNotNull, deleteSequence)
      .when(versionMap.isNull, upsertSequence)
      .otherwise(greatestLeafSequence)
  }

  /**
   * Schema of the CDC metadata struct column for SCD1.
   *
   * The CDC metadata of every microbatch, target, and auxiliary-table row represents either a
   * delete or an upsert, never both: exactly one of its delete and upsert sequences is non-null,
   * and only an upsert may carry a version map.
   */
  private[pipelines] def cdcMetadataColSchema(sequencingType: DataType): StructType =
    StructType(
      Seq(
        // The sequencing of the event if it represents a delete, null otherwise.
        StructField(cdcDeleteSequenceFieldName, sequencingType, nullable = true),
        // The sequencing of the event if it represents an upsert, null otherwise.
        StructField(cdcUpsertSequenceFieldName, sequencingType, nullable = true),
        // On target rows, a null version map indicates row-level reconciliation; a non-null map
        // indicates per-leaf reconciliation. Delete rows, including auxiliary-table tombstones,
        // always use a null map: a delete's sequence applies to every leaf, so a map would only
        // repeat it.
        StructField(
          versionMapFieldName,
          Scd1VersionMap.mapType(sequencingType),
          nullable = true
        )
      )
    )

  /**
   * Construct the CDC metadata struct column for SCD1, following the exact schema and field
   * ordering defined by [[cdcMetadataColSchema]]. Values for microbatch, target, and
   * auxiliary-table rows must uphold its contract.
   */
  private[pipelines] def constructCdcMetadataCol(
      deleteSequence: Column,
      upsertSequence: Column,
      versionMap: Column,
      sequencingType: DataType): Column = {
    val cdcMetadataFieldsInOrder = cdcMetadataColSchema(sequencingType).fields.map { field =>
      val value = field.name match {
        case `cdcDeleteSequenceFieldName` => deleteSequence
        case `cdcUpsertSequenceFieldName` => upsertSequence
        case `versionMapFieldName` => versionMap
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
