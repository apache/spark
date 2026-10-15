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
import org.apache.spark.util.ArrayImplicits._

/**
 * Shared transformations used by SCD1 microbatch reconciliation implementations.
 *
 * Reconciliation requires an orderable sequencing expression and non-null sequencing and key
 * values in every row. The validated microbatch must contain the key columns and every column
 * needed to evaluate the sequencing, delete, and column-selection expressions. The auxiliary
 * table must contain at most one row per key and at least the key columns and CDC metadata.
 */
private[pipelines] object Scd1ReconciliationSupport {

  private[autocdc] val persistedTombstoneSequenceColName: String =
    s"${AutoCdcReservedNames.prefix}persisted_tombstone_sequence"

  /**
   * Appends CDC metadata to each microbatch row.
   *
   * This must run before column selection because the sequencing and delete expressions may
   * reference columns that selection removes. A row is a delete only when the delete condition
   * evaluates to true; a false or null result makes it an upsert.
   */
  private[autocdc] def extendMicrobatchRowsWithCdcMetadata(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      validatedMicrobatch: DataFrame): DataFrame = {
    val rowDeleteSequence: Column = changeArgs.deleteCondition match {
      case Some(deleteCondition) =>
        F.when(deleteCondition, changeArgs.sequencing)
      case None =>
        F.lit(null)
    }

    val rowUpsertSequence: Column =
      // A row that is not a delete must be an upsert, these are mutually exclusive and a complete
      // set of CDC event types.
      F.when(rowDeleteSequence.isNull, changeArgs.sequencing)

    validatedMicrobatch.withColumn(
      AutoCdcReservedNames.cdcMetadataColName,
      Scd1BatchProcessor.constructCdcMetadataCol(
        deleteSequence = rowDeleteSequence,
        upsertSequence = rowUpsertSequence,
        versionMap = F.lit(null),
        sequencingType = resolvedSequencingType
      )
    )
  }

  /**
   * Applies the user-defined column selection while preserving the CDC metadata column.
   *
   * Requires CDC metadata to be present because selection may remove columns used to construct it.
   */
  private[autocdc] def projectTargetColumnsOntoMicrobatch(
      changeArgs: ChangeArgs,
      microbatchWithCdcMetadataDf: DataFrame): DataFrame = {
    val resolver = microbatchWithCdcMetadataDf.sparkSession.sessionState.conf.resolver
    val userColumnsInMicrobatchSchema = ColumnSelection.applyToSchema(
      schemaName = "microbatch",
      schema = microbatchWithCdcMetadataDf.schema,
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName(AutoCdcReservedNames.cdcMetadataColName))
        )
      ),
      resolver = resolver
    )
    val userSelectedColumnsInMicrobatchSchema = ColumnSelection.applyToSchema(
      schemaName = "microbatch",
      schema = userColumnsInMicrobatchSchema,
      columnSelection = changeArgs.columnSelection,
      resolver = resolver
    )
    val finalColumnsInMicrobatchToSelect =
      userSelectedColumnsInMicrobatchSchema.fieldNames.map { columnName =>
        F.col(QuotingUtils.quoteIdentifier(columnName))
      } :+ F.col(AutoCdcReservedNames.cdcMetadataColName)

    microbatchWithCdcMetadataDf.select(
      finalColumnsInMicrobatchToSelect.toImmutableArraySeq: _*
    )
  }

  /**
   * Projects an auxiliary table to its keys and each key's persisted tombstone sequence.
   */
  private[autocdc] def tombstoneSequenceByKey(
      changeArgs: ChangeArgs,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val cdcMetadata = auxiliaryTableDf.col(AutoCdcReservedNames.cdcMetadataColName)
    val tombstoneSequence = Scd1BatchProcessor.deleteSequenceOf(cdcMetadata)
      .as(persistedTombstoneSequenceColName)

    auxiliaryTableDf.select(
      (changeArgs.keys.map(key => F.col(key.quoted)) :+ tombstoneSequence): _*)
  }

  /** Builds the equality condition for key columns on two aliased DataFrames. */
  private[autocdc] def keyColumnsMatch(
      changeArgs: ChangeArgs,
      leftAlias: String,
      rightAlias: String): Column =
    changeArgs.keys
      .map { key =>
        F.col(QuotingUtils.quoteNameParts(Seq(leftAlias, key.name))) ===
          F.col(QuotingUtils.quoteNameParts(Seq(rightAlias, key.name)))
      }
      .reduce(_ && _)

  /**
   * Whether a persisted tombstone is strictly newer than every event for a microbatch key.
   *
   * A microbatch event wins a sequencing tie with a tombstone.
   */
  private[autocdc] def tombstoneSupersedesMicrobatch(
      microbatchDeleteSequence: Column,
      microbatchUpsertSequence: Column,
      tombstoneSequence: Column): Column =
    tombstoneSequence.isNotNull &&
      F.greatest(microbatchDeleteSequence, microbatchUpsertSequence) < tombstoneSequence
}

/** Row-level SCD1 reconciliation. */
private[pipelines] object Scd1RowLevelReconciliation {

  private[autocdc] val winningRowColName: String = s"${AutoCdcReservedNames.prefix}winning_row"

  /**
   * Resolves the CDC events for each key using row-level authorship.
   *
   * Inputs must satisfy the contract documented by [[Scd1ReconciliationSupport]].
   * The event with the greatest sequencing value for each key is retained, its CDC metadata and
   * configured column selection are projected, and events superseded by auxiliary-table tombstones
   * are removed.
   *
   * @return A dataframe containing the selected user columns followed by the CDC metadata column.
   */
  def reconcileMicrobatch(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      validatedBatchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val deduplicated = deduplicateMicrobatch(
      changeArgs = changeArgs,
      validatedMicrobatch = validatedBatchDf
    )
    val withCdcMetadata = Scd1ReconciliationSupport.extendMicrobatchRowsWithCdcMetadata(
      changeArgs = changeArgs,
      resolvedSequencingType = resolvedSequencingType,
      validatedMicrobatch = deduplicated
    )
    val projected = Scd1ReconciliationSupport.projectTargetColumnsOntoMicrobatch(
      changeArgs = changeArgs,
      microbatchWithCdcMetadataDf = withCdcMetadata
    )
    applyTombstonesToMicrobatch(
      changeArgs = changeArgs,
      microbatchDf = projected,
      auxiliaryTableDf = auxiliaryTableDf
    )
  }

  /**
   * Deduplicates the microbatch by key, keeping the event with the greatest sequencing value.
   *
   * Selection between events with equal keys and sequencing values is undefined.
   */
  private[autocdc] def deduplicateMicrobatch(
      changeArgs: ChangeArgs,
      validatedMicrobatch: DataFrame): DataFrame = {
    val allMicrobatchColumns =
      validatedMicrobatch.columns
        .map(colName => F.col(QuotingUtils.quoteIdentifier(colName)))
        .toImmutableArraySeq

    validatedMicrobatch
      .groupBy(changeArgs.keys.map(k => F.col(k.quoted)): _*)
      .agg(
        F.max_by(F.struct(allMicrobatchColumns: _*), changeArgs.sequencing)
          .as(winningRowColName)
      )
      .select(F.col(s"$winningRowColName.*"))
  }

  /**
   * Left anti-joins the microbatch with matching auxiliary-table tombstones that have greater
   * sequencing values.
   */
  private[autocdc] def applyTombstonesToMicrobatch(
      changeArgs: ChangeArgs,
      microbatchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val microbatchAlias = "microbatch"
    val tombstoneAlias = "tombstone"
    val aliasedMicrobatchDf = microbatchDf.alias(microbatchAlias)
    val tombstoneSequenceByKey =
      Scd1ReconciliationSupport.tombstoneSequenceByKey(changeArgs, auxiliaryTableDf)
        .alias(tombstoneAlias)

    val cdcMetadata = AutoCdcReservedNames.cdcMetadataColName
    val microbatchCdcMetadata =
      F.col(QuotingUtils.quoteNameParts(Seq(microbatchAlias, cdcMetadata)))
    val microbatchDeleteSequence =
      Scd1BatchProcessor.deleteSequenceOf(microbatchCdcMetadata)
    val microbatchUpsertSequence =
      Scd1BatchProcessor.upsertSequenceOf(microbatchCdcMetadata)
    val tombstoneSequence = F.col(QuotingUtils.quoteNameParts(
      Seq(tombstoneAlias, Scd1ReconciliationSupport.persistedTombstoneSequenceColName)))

    val keysMatch =
      Scd1ReconciliationSupport.keyColumnsMatch(changeArgs, microbatchAlias, tombstoneAlias)

    val microbatchRowDeletedByTombstone =
      Scd1ReconciliationSupport.tombstoneSupersedesMicrobatch(
        microbatchDeleteSequence,
        microbatchUpsertSequence,
        tombstoneSequence)

    aliasedMicrobatchDf.join(
      right = tombstoneSequenceByKey,
      joinExprs = keysMatch && microbatchRowDeletedByTombstone,
      joinType = "left_anti"
    )
  }
}

/**
 * Leaf-level SCD1 reconciliation.
 *
 * Each key's user-data fields are reconciled independently. Each field passes through the
 * following states in order, and each state is derived only from the one before it:
 *
 *  1. Field to reconcile: a leaf of a column selected for ignore-null, or a whole column outside
 *     the selection. The set of fields depends only on the schema and the ignore-null selection.
 *  2. Authorship candidates: each of the key's microbatch events either authors the field, with a
 *     value and the event's sequence, or leaves it unauthored. A delete authors null. An upsert
 *     authors a whole column unless the microbatch omits all of its leaves, and a selected leaf
 *     only when it provides the leaf.
 *  3. Latest microbatch authorship: the candidate with the greatest sequence, or none if no event
 *     authored the field.
 *  4. Reconciled field: the newer of the latest microbatch authorship and the key's persisted
 *     tombstone, if any, which authored null for every field at its sequence. The microbatch wins
 *     a tie.
 *  5. Output: the reconciled fields are reassembled into the key's columns, rebuilding each
 *     selected struct from the leaves beneath it. Each field also records its authoring sequence
 *     for every leaf it covers in the key's version map. When a tombstone wins, this sequence
 *     comes from the auxiliary table rather than from a microbatch event.
 */
private[pipelines] object Scd1LeafLevelReconciliation {

  private val aggregatedValueFieldName: String = "value"
  private val aggregatedSequenceFieldName: String = "sequence"
  private val aggregatedDeleteSequenceColName: String =
    s"${AutoCdcReservedNames.prefix}aggregated_delete_sequence"
  private val aggregatedUpsertSequenceColName: String =
    s"${AutoCdcReservedNames.prefix}aggregated_upsert_sequence"
  private val latestDeleteUserDataColName: String =
    s"${AutoCdcReservedNames.prefix}latest_delete_user_data"
  private def latestAuthorshipColName(index: Int): String =
    s"${AutoCdcReservedNames.prefix}aggregated_field_$index"

  /**
   * Resolves the CDC events for each key using per-leaf authorship.
   *
   * Inputs must satisfy the contract documented by [[Scd1ReconciliationSupport]].
   * The selected microbatch columns are aligned to the target schema, stamped with per-leaf
   * authorship, collapsed into one synthetic row per key, and reconciled with persisted tombstones.
   *
   * @param targetTableDf A snapshot of the target table whose schema defines the target-aligned
   *                      leaves; its rows are ignored.
   * @return A dataframe containing the target-aligned user columns followed by the CDC metadata
   *         column.
   */
  def reconcileMicrobatch(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      validatedBatchDf: DataFrame,
      targetTableDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val withCdcMetadata = Scd1ReconciliationSupport.extendMicrobatchRowsWithCdcMetadata(
      changeArgs = changeArgs,
      resolvedSequencingType = resolvedSequencingType,
      validatedMicrobatch = validatedBatchDf
    )
    val projected = Scd1ReconciliationSupport.projectTargetColumnsOntoMicrobatch(
      changeArgs = changeArgs,
      microbatchWithCdcMetadataDf = withCdcMetadata
    )
    val aligned = alignMicrobatchToTargetSchema(
      microbatchDf = projected,
      targetTableDf = targetTableDf
    )
    val withVersionMap = extendMicrobatchRowsWithVersionMap(
      changeArgs = changeArgs,
      resolvedSequencingType = resolvedSequencingType,
      alignedMicrobatchDf = aligned,
      unalignedMicrobatchSchema = projected.schema
    )
    collapseMicrobatchRowsPerKey(
      changeArgs = changeArgs,
      resolvedSequencingType = resolvedSequencingType,
      microbatchDf = withVersionMap,
      auxiliaryTableDf = auxiliaryTableDf
    )
  }

  /**
   * Aligns microbatch rows with the persisted target schema without adding target rows.
   *
   * Matching fields use the target's order and spelling. Target fields missing from the microbatch,
   * including nested fields, are filled with nulls. Microbatch-only fields are retained after the
   * target fields.
   *
   * Version maps are built from aligned rows, so each key is spelled as in the target schema and
   * every target leaf receives an entry, even when the microbatch omits it.
   *
   * @param microbatchDf The microbatch rows to align.
   * @param targetTableDf A target-table snapshot whose schema provides the canonical field order
   *                      and spelling. Its rows are ignored.
   * @return The microbatch rows aligned with the target schema, with microbatch-only fields
   *         retained and no rows added from the target.
   */
  private[autocdc] def alignMicrobatchToTargetSchema(
      microbatchDf: DataFrame,
      targetTableDf: DataFrame): DataFrame =
    targetTableDf.limit(0).unionByName(microbatchDf, allowMissingColumns = true)

  /**
   * Populates the version map for upsert rows, if ignore-null is being used.
   *
   * The caller must supply rows whose schema already reflects target column selection and
   * target-schema alignment. An upsert does not author the leaves that alignment added, since its
   * event did not carry them.
   *
   * @param changeArgs The CDC configuration providing keys and the ignore-null selection.
   * @param resolvedSequencingType The resolved type of the sequencing expression and version-map
   *                               values.
   * @param alignedMicrobatchDf Microbatch rows already target-selected and target-schema-aligned,
   *                            with the canonical CDC metadata column populated.
   * @param unalignedMicrobatchSchema The schema of the microbatch rows before target-schema
   *                                  alignment. The leaves of `alignedMicrobatchDf` that it lacks
   *                                  were added by alignment.
   * @return `alignedMicrobatchDf` unchanged when ignore-null is disabled; otherwise, the same rows
   *         but with version maps populated for upsert rows.
   */
  private[autocdc] def extendMicrobatchRowsWithVersionMap(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      alignedMicrobatchDf: DataFrame,
      unalignedMicrobatchSchema: StructType): DataFrame =
    changeArgs.ignoreNullSelection match {
      case None => alignedMicrobatchDf
      case Some(ignoreNullSelection) =>
        val resolver = alignedMicrobatchDf.sparkSession.sessionState.conf.resolver
        val cdcMetadataCol = F.col(AutoCdcReservedNames.cdcMetadataColName)
        val upsertSequence = Scd1BatchProcessor.upsertSequenceOf(cdcMetadataCol)
        val versionMap = F.when(
          upsertSequence.isNotNull,
          Scd1VersionMap.buildVersionMap(
            schema = AutoCdcSchemaUtils.excludeColumns(
              schema = alignedMicrobatchDf.schema,
              // Keys and CDC metadata columns are not eligible for optional authorship. Drop them
              // from the user schema that the version map will be constructed from.
              columnNamesToExclude =
                changeArgs.keys.map(_.name) :+ AutoCdcReservedNames.cdcMetadataColName,
              resolver = resolver
            ),
            ignoreNullSelection = ignoreNullSelection,
            omittedLeafPaths = AutoCdcSchemaUtils
              .fieldPathsMissingFrom(
                alignedMicrobatchDf.schema, unalignedMicrobatchSchema, resolver)
              .toSet,
            upsertSequence = upsertSequence,
            sequencingType = resolvedSequencingType,
            resolver = resolver
          )
        )

        alignedMicrobatchDf.withColumn(
          AutoCdcReservedNames.cdcMetadataColName,
          cdcMetadataCol.withField(Scd1BatchProcessor.versionMapFieldName, versionMap)
        )
    }

  /**
   * For every key, combines all microbatch rows into a synthetic row representing that key's
   * winning leaf authorships, reconciled with any persisted tombstone for the key.
   *
   * Columns selected for ignore-null are reconciled leaf by leaf: upsert events refer to their
   * version map to determine which leaves they author, and a null version map means every leaf is
   * authored by the upsert. Every other column is reconciled as a whole value, which an upsert
   * authors if its version map records any of the column's leaves as authored. Delete events
   * always author null values for every leaf. A persisted tombstone authors
   * null for leaves whose winning microbatch authorship predates it. Selection between microbatch
   * events with equal sequencing values is undefined, while microbatch authorship wins a tie with
   * a persisted tombstone.
   *
   * @param changeArgs The CDC configuration providing the key columns and the ignore-null
   *                   selection.
   * @param resolvedSequencingType The resolved type of CDC sequencing values.
   * @param microbatchDf Microbatch rows whose CDC metadata and version maps have already been
   *                     populated. Version-map keys must match the DataFrame's column names.
   * @param auxiliaryTableDf Auxiliary-table rows containing the key columns and CDC metadata for
   *                         persisted tombstones.
   * @return At most one surviving row per key. Keys whose events all predate a matching tombstone
   *         are omitted. A surviving row represents a delete if the key's greatest microbatch
   *         delete sequence is greater than its greatest microbatch upsert sequence, and an
   *         upsert otherwise. An upsert row carries its greatest upsert sequence and an entry for
   *         every user-data leaf in the aggregated version map, with a null delete sequence. A
   *         delete row carries its greatest delete sequence, with a null upsert sequence and
   *         version map, and the user data of the delete event at that sequence.
   */
  private[autocdc] def collapseMicrobatchRowsPerKey(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      microbatchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val resolver = microbatchDf.sparkSession.sessionState.conf.resolver
    val userDataSchema = AutoCdcSchemaUtils.excludeColumns(
      schema = microbatchDf.schema,
      columnNamesToExclude =
        changeArgs.keys.map(_.name) :+ AutoCdcReservedNames.cdcMetadataColName,
      resolver = resolver
    )
    val microbatchCdcMetadata = microbatchDf.col(AutoCdcReservedNames.cdcMetadataColName)
    val deleteSequence = Scd1BatchProcessor.deleteSequenceOf(microbatchCdcMetadata)
    val upsertSequence = Scd1BatchProcessor.upsertSequenceOf(microbatchCdcMetadata)

    val fieldsToReconcile = Scd1FieldToReconcile.fromSchema(
      schema = userDataSchema,
      ignoreNullSelection = changeArgs.ignoreNullSelection,
      resolver = resolver
    )
    val latestAuthorshipColNames = fieldsToReconcile.indices.map(latestAuthorshipColName)

    // Per AutoCDC key, track the largest row-wide upsert and delete sequences, and the user data of
    // the latest delete event. Additionally, per reconciled field per key, track the last sequence
    // to author that specific field within this microbatch along with the value it authored.
    val userData = F.struct(userDataSchema.fieldNames.toImmutableArraySeq.map { name =>
      microbatchDf.col(QuotingUtils.quoteIdentifier(name))
    }: _*)
    val aggregateColumns = Seq(
      F.max(deleteSequence).as(aggregatedDeleteSequenceColName),
      F.max(upsertSequence).as(aggregatedUpsertSequenceColName),
      F.max_by(userData, deleteSequence).as(latestDeleteUserDataColName)
    ) ++ fieldsToReconcile.zip(latestAuthorshipColNames).map {
      case (fieldToReconcile, authorshipColName) =>
        latestMicrobatchAuthorship(fieldToReconcile, microbatchDf).as(authorshipColName)
    }

    // One row per key with the maximum row-wide sequences and a
    // {latest authored value, authoring sequence} struct pair for every reconciled field.
    val aggregatedPerKeyDf = microbatchDf
      .groupBy(changeArgs.keys.map(key => F.col(key.quoted)): _*)
      .agg(aggregateColumns.head, aggregateColumns.tail: _*)

    val aggregatedWithTombstoneDf = joinAndFilterOnTombstones(
      changeArgs = changeArgs,
      aggregatedPerKeyDf = aggregatedPerKeyDf,
      auxiliaryTableDf = auxiliaryTableDf
    )

    // Per field, reconcile the latest microbatch authorship with the key's persisted tombstone.
    val persistedTombstoneSequence =
      aggregatedWithTombstoneDf.col(
        Scd1ReconciliationSupport.persistedTombstoneSequenceColName)
    val collapsedFields = fieldsToReconcile.zip(latestAuthorshipColNames).map {
      case (fieldToReconcile, authorshipColName) =>
        reconcileCollapsedField(
          fieldToReconcile = fieldToReconcile,
          microbatchAuthorship =
            aggregatedWithTombstoneDf.col(QuotingUtils.quoteIdentifier(authorshipColName)),
          persistedTombstoneSequence = persistedTombstoneSequence
        )
    }
    val collapsedFieldsByTopLevelName = collapsedFields.groupBy(_.fieldToReconcile.path.head)

    // Whether each collapsed row represents the key's net delete rather than its net upsert. A
    // delete wins only when it is sequenced strictly after the upsert, so upserts win ties.
    val aggregatedDeleteSequence = F.col(aggregatedDeleteSequenceColName)
    val aggregatedUpsertSequence = F.col(aggregatedUpsertSequenceColName)
    val collapsedRowRepresentsDelete = aggregatedDeleteSequence.isNotNull &&
      (aggregatedUpsertSequence.isNull || aggregatedDeleteSequence > aggregatedUpsertSequence)
    val latestDeleteUserData = F.col(latestDeleteUserDataColName)

    // A net delete's user data is never stored, but must still satisfy the schema, so it carries
    // the values of the key's latest delete event rather than the nulls that delete authored.
    def collapsedValueOf(collapsedField: Scd1ReconciledField): Column = F.when(
      collapsedRowRepresentsDelete,
      collapsedField.fieldToReconcile.path.foldLeft(latestDeleteUserData)(_.getField(_))
    ).otherwise(collapsedField.reconciledValue)

    // Reconstruct the `microbatchDf` but using the aggregated results per key. The resulting
    // dataframe has the same shape as the `microbatchDf`, but a single row per key, representing
    // the latest authored values per column in the microbatch. This implicitly drops all temporary
    // projected columns, such as `persistedTombstoneSequenceColName`.
    aggregatedWithTombstoneDf.select(
      microbatchDf.schema.fields.toImmutableArraySeq.map { field =>
        val isCdcMetadataField = resolver(AutoCdcReservedNames.cdcMetadataColName, field.name)
        lazy val isKeyField = changeArgs.keys.exists(key => resolver(key.name, field.name))

        if (isCdcMetadataField) {
          // A collapsed row represents the key's net change across the microbatch: either a net
          // upsert or a net delete, never both. Its CDC metadata upholds the same invariant as a
          // row-level CDC event: exactly one of the delete and upsert sequences is non-null, and
          // only upserts carry a version map. Nulling the shadowed sequence loses nothing. A net
          // delete is the key's latest event and authors every leaf, so the key's upserts no
          // longer matter. A net upsert's version map already records every leaf that a delete
          // still authors, so its delete sequence is redundant.
          Scd1BatchProcessor.constructCdcMetadataCol(
            deleteSequence = F.when(collapsedRowRepresentsDelete, aggregatedDeleteSequence),
            upsertSequence = F.when(!collapsedRowRepresentsDelete, aggregatedUpsertSequence),
            versionMap = F.when(
              !collapsedRowRepresentsDelete,
              Scd1ReconciledField.versionMapFrom(collapsedFields, resolvedSequencingType)
            ),
            sequencingType = resolvedSequencingType
          ).as(field.name, field.metadata)
        } else if (isKeyField) {
          // Pass key columns through as-is.
          F.col(QuotingUtils.quoteIdentifier(field.name)).as(field.name, field.metadata)
        } else {
          // Every other column is a top-level user data column; construct the last-authored value
          // per column per key. A column outside the ignore-null selection takes its value whole,
          // while a column selected for ignore-null is rebuilt from the last-authored value per
          // leaf.
          collapsedFieldsByTopLevelName.getOrElse(field.name, Seq.empty) match {
            case Seq(column) if !column.isIgnoreNullLeaf =>
              IgnoreNullColumnReconstruction.conformToField(
                collapsedValueOf(column), Seq(field.name), field)
            case leaves =>
              IgnoreNullColumnReconstruction.reconstructColumn(
                path = Seq(field.name),
                field = field,
                leafValues =
                  leaves.map(leaf => leaf.fieldToReconcile.path -> collapsedValueOf(leaf)))
          }
        }
      }: _*
    )
  }

  /**
   * Builds the aggregate expression for a field's latest authorship among a key's microbatch
   * events.
   *
   * `microbatchDf` must contain the column at `fieldToReconcile.path`, canonical CDC metadata
   * with upsert/delete sequences, and version maps for leaf-level upserts.
   *
   * @param fieldToReconcile The field whose authorship to aggregate.
   * @param microbatchDf The DataFrame against which the aggregate expression is resolved.
   * @return An unaliased aggregate expression producing a struct with fields `value` (the latest
   *         authored value, typed to match the field) and `sequence` (the sequence it was
   *         authored at). The struct is null when no event authored the field.
   */
  private def latestMicrobatchAuthorship(
      fieldToReconcile: Scd1FieldToReconcile,
      microbatchDf: DataFrame): Column = {
    val currentValue = microbatchDf.col(QuotingUtils.quoteNameParts(fieldToReconcile.path))
    val cdcMetadata =
      microbatchDf.col(AutoCdcReservedNames.cdcMetadataColName)
    val deleteSequence = Scd1BatchProcessor.deleteSequenceOf(cdcMetadata)

    // Column expression for the sequence that a row authors this field, across both delete and
    // upsert events - delete events always "author" nulls. Null if this row does not author the
    // field.
    val effectiveAuthorshipSequence =
      Scd1BatchProcessor.lastAuthoredIn(cdcMetadata, fieldToReconcile.leafPaths)

    // The effective value this field would author, if it is indeed authoring the field.
    val effectiveAuthoredValue = F.when(deleteSequence.isNull, currentValue)

    // Aggregate the (last authored value, sequence authored at) per field.
    F.max_by(
      F.struct(
        effectiveAuthoredValue.as(aggregatedValueFieldName),
        effectiveAuthorshipSequence.as(aggregatedSequenceFieldName)
      ),
      effectiveAuthorshipSequence
    )
  }

  /**
   * Joins rows already aggregated per key with persisted tombstones for those keys.
   *
   * Aggregated rows whose greatest row-wide microbatch sequence predates the persisted tombstone
   * are removed. Surviving rows retain their existing columns and gain
   * [[Scd1ReconciliationSupport.persistedTombstoneSequenceColName]], which downstream field
   * reconciliation uses as an authorship baseline.
   *
   * @param changeArgs The CDC configuration providing the key columns.
   * @param aggregatedPerKeyDf One row per key, containing
   *                           [[aggregatedDeleteSequenceColName]] and
   *                           [[aggregatedUpsertSequenceColName]].
   * @param auxiliaryTableDf Auxiliary-table rows containing the key columns and CDC metadata for
   *                         persisted tombstones.
   * @return Surviving aggregated rows with the tombstone sequence appended, null for keys without
   *         one.
   */
  private def joinAndFilterOnTombstones(
      changeArgs: ChangeArgs,
      aggregatedPerKeyDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val aggregatedAlias = "aggregatedMicrobatch"
    val tombstoneAlias = "tombstone"
    val tombstoneSequenceByKey =
      Scd1ReconciliationSupport.tombstoneSequenceByKey(changeArgs, auxiliaryTableDf)
    val tombstoneSequence = F.col(QuotingUtils.quoteNameParts(
      Seq(tombstoneAlias, Scd1ReconciliationSupport.persistedTombstoneSequenceColName)))
    val keysMatch =
      Scd1ReconciliationSupport.keyColumnsMatch(changeArgs, aggregatedAlias, tombstoneAlias)
    val aggregatedDeleteSequence = F.col(
      QuotingUtils.quoteNameParts(Seq(aggregatedAlias, aggregatedDeleteSequenceColName)))
    val aggregatedUpsertSequence = F.col(
      QuotingUtils.quoteNameParts(Seq(aggregatedAlias, aggregatedUpsertSequenceColName)))

    // Keep keys with no tombstone or at least one microbatch event at or after the tombstone.
    // Earlier-only rows remain deleted by the tombstone and are dropped.
    aggregatedPerKeyDf
      .alias(aggregatedAlias)
      .join(
        right = tombstoneSequenceByKey.alias(tombstoneAlias),
        joinExprs = keysMatch,
        joinType = "left_outer"
      )
      .where(!Scd1ReconciliationSupport.tombstoneSupersedesMicrobatch(
        aggregatedDeleteSequence,
        aggregatedUpsertSequence,
        tombstoneSequence))
      .select(
        (aggregatedPerKeyDf.schema.fields.toImmutableArraySeq.map { field =>
          F.col(QuotingUtils.quoteNameParts(Seq(aggregatedAlias, field.name)))
            .as(field.name, field.metadata)
        } :+ tombstoneSequence.as(
          Scd1ReconciliationSupport.persistedTombstoneSequenceColName)): _*
      )
  }

  /**
   * Reconciles one field of a key's row collapsed by [[collapseMicrobatchRowsPerKey]] from two
   * authorship candidates: the microbatch's latest authorship of the field, and the key's
   * persisted tombstone, which authored null for every field at its sequence. The newer candidate
   * wins, and the microbatch wins a tie.
   *
   * @param fieldToReconcile The field to reconcile.
   * @param microbatchAuthorship A struct column with fields `value` (the latest value the
   *                             microbatch authored) and `sequence` (the sequence it was authored
   *                             at). The struct is null when no event authored the field, which
   *                             reads the same as a null value and a null sequence.
   * @param persistedTombstoneSequence The sequence of the key's persisted tombstone, which
   *                                   authored null for every field, or null if the key has none.
   * @return The winning candidate's value and the sequence it was authored at.
   */
  private def reconcileCollapsedField(
      fieldToReconcile: Scd1FieldToReconcile,
      microbatchAuthorship: Column,
      persistedTombstoneSequence: Column): Scd1ReconciledField = {
    val microbatchAuthoredValue = microbatchAuthorship.getField(aggregatedValueFieldName)
    val microbatchAuthorshipSequence = microbatchAuthorship.getField(aggregatedSequenceFieldName)

    val tombstoneWins = persistedTombstoneSequence.isNotNull &&
      (microbatchAuthorshipSequence.isNull ||
        microbatchAuthorshipSequence < persistedTombstoneSequence)

    Scd1ReconciledField(
      fieldToReconcile = fieldToReconcile,
      reconciledValue = F.when(tombstoneWins, F.lit(null)).otherwise(microbatchAuthoredValue),
      valueAuthoredAtSequence =
        F.when(tombstoneWins, persistedTombstoneSequence).otherwise(microbatchAuthorshipSequence))
  }
}
