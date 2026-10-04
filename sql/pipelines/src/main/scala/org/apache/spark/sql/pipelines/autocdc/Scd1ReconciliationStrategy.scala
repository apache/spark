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
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.{DataFrame, ExpressionUtils}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

/** Strategy for reconciling an SCD1 microbatch. */
private[pipelines] trait Scd1ReconciliationStrategy {

  /**
   * Resolves the CDC events for each key and removes events superseded by recorded tombstones.
   *
   * The sequencing expression must have an orderable data type, and every row must have non-null
   * sequencing and key values. These invariants are required for per-key ordering and matching.
   *
   * @param validatedBatchDf A CDC microbatch satisfying the invariants above and containing the key
   *                         columns and every column needed to evaluate the sequencing, delete, and
   *                         column-selection expressions.
   * @param auxiliaryTableDf A snapshot of the auxiliary table containing at least the key columns
   *                         and the CDC metadata column.
   * @return A dataframe containing the selected user columns followed by the CDC metadata column.
   */
  def reconcileMicrobatch(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      validatedBatchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame

  /**
   * Appends CDC metadata to each microbatch row.
   *
   * This must run before column selection because the sequencing and delete expressions may
   * reference columns that selection removes. A row is a delete only when the delete condition
   * evaluates to true; a false or null result makes it an upsert.
   */
  protected[autocdc] final def extendMicrobatchRowsWithCdcMetadata(
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
  protected[autocdc] final def projectTargetColumnsOntoMicrobatch(
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
}

/** Row-level SCD1 reconciliation. */
private[pipelines] object Scd1RowLevelReconciliation extends Scd1ReconciliationStrategy {

  private[autocdc] val winningRowColName: String = s"${AutoCdcReservedNames.prefix}winning_row"

  /**
   * Keeps the event with the greatest sequencing value for each key, adds its CDC metadata,
   * applies the configured column selection, and removes events superseded by auxiliary-table
   * tombstones.
   */
  override def reconcileMicrobatch(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      validatedBatchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val deduplicated = deduplicateMicrobatch(
      changeArgs = changeArgs,
      validatedMicrobatch = validatedBatchDf
    )
    val withCdcMetadata = extendMicrobatchRowsWithCdcMetadata(
      changeArgs = changeArgs,
      resolvedSequencingType = resolvedSequencingType,
      validatedMicrobatch = deduplicated
    )
    val projected = projectTargetColumnsOntoMicrobatch(
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
      .map { key =>
        F.col(s"microbatch.${key.quoted}") === F.col(s"auxiliaryTable.${key.quoted}")
      }
      .reduce(_ && _)

    val microbatchRowDeletedByTombstone = effectiveSeq < tombstoneDeleteSeq

    aliasedMicrobatchDf.join(
      right = aliasedAuxiliaryTableDf,
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
 *     authors a whole column always, and a selected leaf only when it provides the leaf.
 *  3. Latest microbatch authorship: the candidate with the greatest sequence, or none if no event
 *     authored the field. This is the field's reconciled value.
 *  4. Output: the reconciled fields are reassembled into the key's columns, where a selected
 *     struct is null when every leaf beneath it is null. Each field also records its authoring
 *     sequence for every leaf it covers in the key's version map.
 */
private[pipelines] object Scd1LeafLevelReconciliation {

  private val aggregatedValueFieldName: String = "value"
  private val aggregatedSequenceFieldName: String = "sequence"
  private val aggregatedDeleteSequenceColName: String =
    s"${AutoCdcReservedNames.prefix}aggregated_delete_sequence"
  private val aggregatedUpsertSequenceColName: String =
    s"${AutoCdcReservedNames.prefix}aggregated_upsert_sequence"
  private def latestAuthorshipColName(index: Int): String =
    s"${AutoCdcReservedNames.prefix}aggregated_field_$index"

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
   * target-schema alignment.
   *
   * @param changeArgs The CDC configuration providing keys and the ignore-null selection.
   * @param resolvedSequencingType The resolved type of the sequencing expression and version-map
   *                               values.
   * @param alignedDf Microbatch rows already target-selected and target-schema-aligned, with the
   *                  canonical CDC metadata column populated.
   * @return `alignedDf` unchanged when ignore-null is disabled; otherwise, the same rows but with
   *         version maps populated for upsert rows.
   */
  private[autocdc] def extendMicrobatchRowsWithVersionMap(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      alignedDf: DataFrame): DataFrame =
    changeArgs.ignoreNullSelection match {
      case None => alignedDf
      case Some(ignoreNullSelection) =>
        val resolver = alignedDf.sparkSession.sessionState.conf.resolver
        val cdcMetadataCol = F.col(AutoCdcReservedNames.cdcMetadataColName)
        val upsertSequence = Scd1BatchProcessor.upsertSequenceOf(cdcMetadataCol)
        val versionMap = F.when(
          upsertSequence.isNotNull,
          Scd1VersionMap.buildVersionMap(
            schema = AutoCdcSchemaUtils.excludeColumns(
              schema = alignedDf.schema,
              // Keys and CDC metadata columns are not eligible for optional authorship. Drop them
              // from the user schema that the version map will be constructed from.
              columnNamesToExclude =
                changeArgs.keys.map(_.name) :+ AutoCdcReservedNames.cdcMetadataColName,
              resolver = resolver
            ),
            ignoreNullSelection = ignoreNullSelection,
            upsertSequence = upsertSequence,
            sequencingType = resolvedSequencingType,
            resolver = resolver
          )
        )

        alignedDf.withColumn(
          AutoCdcReservedNames.cdcMetadataColName,
          cdcMetadataCol.withField(Scd1BatchProcessor.versionMapFieldName, versionMap)
        )
    }

  /**
   * For every key, combines all rows into a synthetic row representing that key's winning leaf
   * authorships from the microbatch.
   *
   * Columns selected for ignore-null are reconciled leaf by leaf: upsert events refer to their
   * version map to determine which leaves they author, and a null version map means every leaf is
   * authored by the upsert. Every other column is reconciled as a whole value, which every upsert
   * authors. Delete events always author null values for every leaf. Selection between events
   * with equal sequencing values is undefined.
   *
   * @param changeArgs The CDC configuration providing the key columns and the ignore-null
   *                   selection.
   * @param resolvedSequencingType The resolved type of CDC sequencing values.
   * @param microbatchDf Microbatch rows whose CDC metadata and version maps have already been
   *                     populated. Version-map keys must match the DataFrame's column names.
   * @return One row per key, representing either an upsert or a delete as determined by
   *         [[Scd1BatchProcessor.representsDelete]] on the key's greatest delete and upsert
   *         sequences. An upsert row carries its greatest upsert sequence and an entry for every
   *         user-data leaf in the aggregated version map, with a null delete sequence. A delete
   *         row carries only its greatest delete sequence, with a null upsert sequence and
   *         version map.
   */
  private[autocdc] def collapseMicrobatchRowsPerKey(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      microbatchDf: DataFrame): DataFrame = {
    val resolver = microbatchDf.sparkSession.sessionState.conf.resolver
    val userDataSchema = AutoCdcSchemaUtils.excludeColumns(
      schema = microbatchDf.schema,
      columnNamesToExclude =
        changeArgs.keys.map(_.name) :+ AutoCdcReservedNames.cdcMetadataColName,
      resolver = resolver
    )
    val cdcMetadata = microbatchDf.col(AutoCdcReservedNames.cdcMetadataColName)
    val deleteSequence = Scd1BatchProcessor.deleteSequenceOf(cdcMetadata)
    val upsertSequence = Scd1BatchProcessor.upsertSequenceOf(cdcMetadata)

    val fieldsToReconcile = Scd1FieldToReconcile.fromSchema(
      schema = userDataSchema,
      ignoreNullSelection = changeArgs.ignoreNullSelection,
      resolver = resolver
    )
    val latestAuthorshipColNames = fieldsToReconcile.indices.map(latestAuthorshipColName)

    // Per AutoCDC key, track the largest row-wide upsert and delete sequences. Additionally, per
    // reconciled field per key, track the last sequence to author that specific field within this
    // microbatch along with the value it authored.
    val aggregateColumns = Seq(
      F.max(deleteSequence).as(aggregatedDeleteSequenceColName),
      F.max(upsertSequence).as(aggregatedUpsertSequenceColName)
    ) ++ fieldsToReconcile.zip(latestAuthorshipColNames).map {
      case (fieldToReconcile, authorshipColName) =>
        latestMicrobatchAuthorship(fieldToReconcile, microbatchDf).as(authorshipColName)
    }

    // One row per key with the maximum row-wide sequences and a
    // {latest authored value, authoring sequence} struct pair for every reconciled field.
    val aggregatedPerKeyDf = microbatchDf
      .groupBy(changeArgs.keys.map(key => F.col(key.quoted)): _*)
      .agg(aggregateColumns.head, aggregateColumns.tail: _*)

    // Per field, read the latest microbatch authorship from the key's aggregated row.
    val collapsedFields = fieldsToReconcile.zip(latestAuthorshipColNames).map {
      case (fieldToReconcile, authorshipColName) =>
        CollapsedField(
          fieldToReconcile = fieldToReconcile,
          microbatchAuthorship =
            aggregatedPerKeyDf.col(QuotingUtils.quoteIdentifier(authorshipColName))
        )
    }
    val collapsedFieldsByTopLevelName = collapsedFields.groupBy(_.path.head)

    // Whether each collapsed row represents the key's net delete rather than its net upsert.
    val aggregatedDeleteSequence = F.col(aggregatedDeleteSequenceColName)
    val aggregatedUpsertSequence = F.col(aggregatedUpsertSequenceColName)
    val collapsedRowRepresentsDelete =
      Scd1BatchProcessor.representsDelete(aggregatedDeleteSequence, aggregatedUpsertSequence)

    // Reconstruct the `microbatchDf` but using the aggregated results per key. The resulting
    // dataframe has the same shape as the `microbatchDf`, but a single row per key, representing
    // the latest authored values per column in the microbatch.
    aggregatedPerKeyDf.select(
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
              versionMapFrom(collapsedFields, resolvedSequencingType)
            ),
            sequencingType = resolvedSequencingType
          ).as(field.name, field.metadata)
        } else if (isKeyField) {
          // Pass key columns through as-is.
          F.col(QuotingUtils.quoteIdentifier(field.name)).as(field.name, field.metadata)
        } else {
          // Every other column is a top-level user data column; construct the last-authored value
          // per column per key. A struct column selected for ignore-null is recursively
          // reconstructed from the last-authored value per leaf.
          reconstructColumn(
            path = Seq(field.name),
            field = field,
            fieldsBeneath = collapsedFieldsByTopLevelName.getOrElse(field.name, Seq.empty)
          )
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
   * @param fieldToReconcile The field whose authorship to aggregate. Its schema field types the
   *                         null that deletes author.
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
    val upsertSequence = Scd1BatchProcessor.upsertSequenceOf(cdcMetadata)
    val versionMap = cdcMetadata.getField(Scd1BatchProcessor.versionMapFieldName)

    // Column expression for the upsert sequence a row authors this field's value at, null if
    // the row isn't an upsert or doesn't author the field.
    val upsertAuthorshipSequence =
      fieldToReconcile.upsertAuthorshipSequence(upsertSequence, versionMap)

    // Column expression for the sequence that a row authors this field, across both delete and
    // upsert events - delete events always "author" nulls. Null if this row does not author the
    // field.
    val effectiveAuthorshipSequence =
      F.when(deleteSequence.isNotNull, deleteSequence).otherwise(upsertAuthorshipSequence)

    // The effective value this field would author, if it is indeed authoring the field.
    val effectiveAuthoredValue =
      F.when(deleteSequence.isNotNull, F.lit(null).cast(fieldToReconcile.field.dataType))
        .otherwise(currentValue)

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
   * Reconstructs `field` from the collapsed fields beneath it, or returns its reconciled value if
   * it was reconciled as a whole.
   *
   * A nullable struct reconstructed from leaves is null iff every reconciled leaf beneath it is
   * null. Only columns selected for ignore-null are reconciled leaf by leaf, and for them this is
   * exactly leaf-level authorship: an upsert authors only the non-null leaves it provides, so a
   * reconciled leaf is non-null iff an upsert authored it, and that upsert necessarily provided
   * every struct enclosing the leaf. An upsert that provided a struct whose leaves are all null
   * authored none of them, so it has no claim on the struct. This relies on the ignore-null
   * selection accepting only top-level columns, so every leaf beneath such a struct is selected.
   */
  private def reconstructColumn(
      path: Seq[String],
      field: StructField,
      fieldsBeneath: Seq[CollapsedField]): Column = {
    if (fieldsBeneath.isEmpty) {
      throwMissingFields(path)
    }

    val reconstructed = field.dataType match {
      case struct: StructType if !fieldsBeneath.exists(_.path == path) =>
        val fieldsByChildName = fieldsBeneath.groupBy(_.path(path.length))
        val rebuilt = F.struct(struct.fields.toImmutableArraySeq.map { childField =>
          val childPath = path :+ childField.name
          fieldsByChildName
            .get(childField.name)
            .map(reconstructColumn(childPath, childField, _))
            .getOrElse(throwMissingFields(childPath))
            .as(childField.name, childField.metadata)
        }: _*)

        if (field.nullable) {
          val anyLeafIsNotNull = fieldsBeneath.map(_.reconciledValue.isNotNull).reduce(_ || _)
          F.when(anyLeafIsNotNull, rebuilt).otherwise(F.lit(null).cast(field.dataType))
        } else {
          rebuilt
        }
      case _ =>
        fieldsBeneath.head.reconciledValue
    }

    val nullabilityChecked =
      if (field.nullable) {
        reconstructed
      } else {
        ExpressionUtils.column(
          AssertNotNull(ExpressionUtils.expression(reconstructed), path))
      }
    nullabilityChecked.cast(field.dataType).as(field.name, field.metadata)
  }

  /** Builds a version map containing one entry for every user-data leaf. */
  private def versionMapFrom(
      collapsedFields: Seq[CollapsedField],
      sequencingType: DataType): Column = {
    val entries = collapsedFields.flatMap { collapsedField =>
      collapsedField.leafPaths.flatMap { leafPath =>
        Seq(F.lit(Scd1VersionMap.serializeKey(leafPath)), collapsedField.valueAuthoredAtSequence)
      }
    }
    F.map(entries: _*).cast(Scd1VersionMap.mapType(sequencingType))
  }

  /**
   * One field of a key's row collapsed by [[collapseMicrobatchRowsPerKey]], holding the
   * microbatch's latest authorship of the field.
   *
   * The reconciled value may be null, since deletes and upserts can both author null. A non-null
   * [[reconciledValue]] has a non-null [[valueAuthoredAtSequence]], and an unauthored field, whose
   * sequence is null, has a null value.
   *
   * @param fieldToReconcile The field this reconciles.
   * @param microbatchAuthorship A struct column with fields `value` (the latest value the
   *                             microbatch authored) and `sequence` (the sequence it was authored
   *                             at). The struct is null when no event authored the field, which
   *                             reads the same as a null value and a null sequence.
   */
  private case class CollapsedField(
      private val fieldToReconcile: Scd1FieldToReconcile,
      private val microbatchAuthorship: Column) {

    /** The field's name parts within the user-data schema, where [[reconciledValue]] belongs. */
    def path: Seq[String] = fieldToReconcile.path

    /**
     * The name parts of every user-data leaf at or beneath [[path]]. The version map records each
     * of them as authored at [[valueAuthoredAtSequence]].
     */
    def leafPaths: Seq[Seq[String]] = fieldToReconcile.leafPaths

    /** The reconciled value selected for this field. */
    val reconciledValue: Column = microbatchAuthorship.getField(aggregatedValueFieldName)

    /** The sequence at which [[reconciledValue]] was authored, or null if it remains unauthored. */
    val valueAuthoredAtSequence: Column =
      microbatchAuthorship.getField(aggregatedSequenceFieldName)
  }

  private def throwMissingFields(path: Seq[String]): Nothing =
    throw SparkException.internalError(
      s"Cannot construct column ${QuotingUtils.quoteNameParts(path)} because it has no " +
        "reconciled fields.")
}
