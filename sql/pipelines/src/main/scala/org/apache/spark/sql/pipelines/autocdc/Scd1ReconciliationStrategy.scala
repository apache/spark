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

/** Leaf-level SCD1 reconciliation. */
private[pipelines] object Scd1LeafLevelReconciliation extends Scd1ReconciliationStrategy {

  private val aggregatedLeafValueFieldName: String = "value"
  private val aggregatedLeafSequenceFieldName: String = "sequence"
  private val aggregatedDeleteSequenceColName: String =
    s"${AutoCdcReservedNames.prefix}aggregated_delete_sequence"
  private val aggregatedUpsertSequenceColName: String =
    s"${AutoCdcReservedNames.prefix}aggregated_upsert_sequence"

  override def reconcileMicrobatch(
      changeArgs: ChangeArgs,
      resolvedSequencingType: DataType,
      batchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame =
    throw new NotImplementedError("SCD1 leaf-level reconciliation is not implemented")

  /**
   * Aligns microbatch rows with the persisted target schema without adding target rows.
   *
   * Matching fields use the target's order and spelling. Target fields missing from the microbatch,
   * including nested fields, are filled with nulls. Microbatch-only fields are retained after the
   * target fields.
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
   * Upsert events refer to their version map to determine which leaves they author; a null
   * version map means every leaf is authored by the upsert. Delete events always author null
   * values for every leaf. Selection between events with equal sequencing values is undefined.
   *
   * @param changeArgs The CDC configuration providing the key columns.
   * @param resolvedSequencingType The resolved type of CDC sequencing values.
   * @param microbatchDf Microbatch rows whose CDC metadata and version maps have already been
   *                     populated. Version-map keys must match the DataFrame's column names.
   * @return One row per key, with the greatest delete and upsert sequences and an entry for every
   *         user-data leaf in the aggregated version map.
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

    val leafAuthorshipContexts =
      AutoCdcSchemaUtils.flattenStructFieldPaths(userDataSchema).zipWithIndex.map {
        case (path, index) =>
          LeafAuthorshipContext(
            path = path,
            index = index,
            microbatchDf = microbatchDf,
            field = userDataSchema.findNestedField(path).get._2
          )
      }

    // Per AutoCDC key, track the largest row-wide upsert and delete sequences. Additionally, per
    // leaf per key, track the last sequence to author that specific leaf within this
    // microbatch along with the column value it authored.
    val aggregateColumns = Seq(
      F.max(deleteSequence).as(aggregatedDeleteSequenceColName),
      F.max(upsertSequence).as(aggregatedUpsertSequenceColName)
    ) ++ leafAuthorshipContexts.map(_.namedLatestAuthorshipCol)

    // One row per key with the maximum row-wide sequences and a
    // {latest authored value, authoring sequence} struct pair for every leaf.
    val aggregatedPerKeyDf = microbatchDf
      .groupBy(changeArgs.keys.map(key => F.col(key.quoted)): _*)
      .agg(aggregateColumns.head, aggregateColumns.tail: _*)

    // Per leaf, wrap aggregation result in an accessor class to abstract away retrieval for when
    // the leaf was last authored, and with what value.
    val aggregatedLeaves =
      leafAuthorshipContexts.map(LeafAuthorshipResult(_, aggregatedPerKeyDf))

    // Reconstruct the `microbatchDf` but using the aggregated results per key. The resulting
    // dataframe has the same shape as the `microbatchDf`, but a single row per key, representing
    // the latest authored values per column in the microbatch.
    aggregatedPerKeyDf.select(
      microbatchDf.schema.fields.toImmutableArraySeq.map { field =>
        val isCdcMetadataField = resolver(AutoCdcReservedNames.cdcMetadataColName, field.name)
        lazy val isKeyField = changeArgs.keys.exists(key => resolver(key.name, field.name))

        if (isCdcMetadataField) {
          // Reconstruct the CDC metadata column using the aggregated row-wide upsert/delete
          // sequences, as well as the aggregated version map.
          Scd1BatchProcessor.constructCdcMetadataCol(
            deleteSequence = F.col(aggregatedDeleteSequenceColName),
            upsertSequence = F.col(aggregatedUpsertSequenceColName),
            versionMap = versionMapFrom(aggregatedLeaves, resolvedSequencingType),
            sequencingType = resolvedSequencingType
          ).as(field.name, field.metadata)
        } else if (isKeyField) {
          // Pass key columns through as-is.
          F.col(QuotingUtils.quoteIdentifier(field.name)).as(field.name, field.metadata)
        } else {
          // Every other column is a top-level user data column; construct the last-authored value
          // per column per key. If the top level column is a struct, recursively reconstruct it,
          // respecting last-authored value per leaf.
          aggregatedLeaves
            .groupBy(_.path.head)
            .get(field.name)
            .map(reconstructColumnFromLeaves(Seq(field.name), field, _))
            .getOrElse(throwMissingAggregatedLeaves(Seq(field.name)))
        }
      }: _*
    )
  }

  /**
   * Rebuilds a column from the aggregated authorship results for the leaves beneath it.
   *
   * @param path The field's name parts.
   * @param field The field to construct, including its data type, nullability, and metadata.
   * @param leavesBeneath The authorship results at or below `path`. Must not be empty.
   * @return A column named and typed according to `field`.
   */
  private def reconstructColumnFromLeaves(
      path: Seq[String],
      field: StructField,
      leavesBeneath: Seq[LeafAuthorshipResult]): Column = {
    if (leavesBeneath.isEmpty) {
      throwMissingAggregatedLeaves(path)
    }

    val aggregated = field.dataType match {
      case struct: StructType =>
        val leavesByChildName = leavesBeneath.groupBy(_.path(path.length))
        val rebuilt = F.struct(
          struct.fields.toImmutableArraySeq.map { childField =>
            val childPath = path :+ childField.name
            leavesByChildName
              .get(childField.name)
              .map(reconstructColumnFromLeaves(childPath, childField, _))
              .getOrElse(throwMissingAggregatedLeaves(childPath))
              .as(childField.name, childField.metadata)
          }: _*
        )

        // If this [maybe-nested] struct is defined as nullable but none of its children are
        // authoring, the entire struct should be nulled to represent no authorship.
        // If the struct is defined as non-nullable, then regardless of whether its children are
        // authoring, we still need to create the struct and continue recursing its children.
        if (field.nullable) {
          val anyChildLeafAuthors =
            leavesBeneath.map(_.valueAuthoredAtSequence.isNotNull).reduce(_ || _)

          F.when(anyChildLeafAuthors, rebuilt).otherwise(F.lit(null).cast(field.dataType))
        } else {
          rebuilt
        }
      case _ =>
        leavesBeneath.head.authoredValue
    }

    aggregated.as(field.name, field.metadata)
  }

  /**
   * Builds a version map containing one entry for every aggregated leaf.
   *
   * @param aggregatedLeaves The leaf authorship results whose keys and authored-at sequences become
   *                         version-map entries.
   * @param sequencingType The data type of each authorship sequence in the version map.
   */
  private def versionMapFrom(
      aggregatedLeaves: Seq[LeafAuthorshipResult],
      sequencingType: DataType): Column = {
    val entries = aggregatedLeaves.flatMap { leaf =>
      Seq(F.lit(leaf.versionMapKey), leaf.valueAuthoredAtSequence)
    }
    F.map(entries: _*).cast(Scd1VersionMap.mapType(sequencingType))
  }

  /**
   * Wraps the aggregate column expression used to determine a leaf's net authorship across a
   * microbatch, and the canonical name under which the expression should be projected.
   *
   * @param path The leaf's name parts within the target-aligned row.
   * @param index An integer temporarily and uniquely identifying this leaf, for this
   *              reconciliation pass.
   * @param latestAuthorshipCol An unaliased aggregate expression that produces a struct with
   *                            fields `value` (the last authored leaf value, typed to match the
   *                            leaf) and `sequence` (the sequencing clock of the authoring event).
   */
  private case class LeafAuthorshipContext(
      path: Seq[String],
      index: Int,
      latestAuthorshipCol: Column) {
    if (latestAuthorshipCol == null) throwNullAuthorshipColumn(path)

    val namedLatestAuthorshipCol: Column =
      latestAuthorshipCol.as(LeafAuthorshipContext.latestAuthorshipColName(index))
  }

  private object LeafAuthorshipContext {

    /**
     * Builds the per-leaf aggregate expression from `microbatchDf`.
     *
     * `microbatchDf` must contain the leaf column at `path`, canonical CDC metadata with
     * upsert/delete sequences, and version maps for leaf-level upserts.
     *
     * @param path The leaf's name parts.
     * @param index An integer uniquely identifying this leaf within the aggregation.
     * @param microbatchDf The DataFrame against which the aggregate expression is resolved.
     * @param field The leaf's schema field, used to type null values for deletes.
     */
    def apply(
        path: Seq[String],
        index: Int,
        microbatchDf: DataFrame,
        field: StructField): LeafAuthorshipContext = {
      val versionMapKey = Scd1VersionMap.serializeKey(path)
      val currentValue = microbatchDf.col(versionMapKey)
      val cdcMetadata =
        microbatchDf.col(AutoCdcReservedNames.cdcMetadataColName)
      val deleteSequence = Scd1BatchProcessor.deleteSequenceOf(cdcMetadata)
      val upsertSequence = Scd1BatchProcessor.upsertSequenceOf(cdcMetadata)
      val versionMap = cdcMetadata.getField(Scd1BatchProcessor.versionMapFieldName)

      // Column expression for the upsert sequence a row authors this leaf value, null if the row
      // isn't an upsert or doesn't author the leaf. If the version map is null for a row, it is
      // using row-wide authorship.
      val upsertAuthorshipSequence = F.when(versionMap.isNull, upsertSequence)
        .otherwise(versionMap(versionMapKey))

      // Column expression for the sequence that a row authors this leaf, across both delete and
      // upsert events - delete events always "author" nulls. Null if this row does not author the
      // leaf.
      val effectiveAuthorshipSequence =
        F.when(deleteSequence.isNotNull, deleteSequence).otherwise(upsertAuthorshipSequence)

      // The effective value this leaf would author, if it is indeed authoring the leaf.
      val effectiveAuthoredValue =
        F.when(deleteSequence.isNotNull, F.lit(null).cast(field.dataType))
          .otherwise(currentValue)

      // Aggregate the (last authored value, sequence authored at) per leaf.
      val latestAuthorshipCol = F.max_by(
        F.struct(
          effectiveAuthoredValue.as(aggregatedLeafValueFieldName),
          effectiveAuthorshipSequence.as(aggregatedLeafSequenceFieldName)
        ),
        effectiveAuthorshipSequence
      )

      LeafAuthorshipContext(path, index, latestAuthorshipCol)
    }

    def latestAuthorshipColName(index: Int): String =
      s"${AutoCdcReservedNames.prefix}aggregated_leaf_$index"
  }

  /**
   * Accessor for the authorship result of one leaf column in an aggregated DataFrame.
   *
   * @param path The leaf's name parts.
   * @param latestAuthorshipCol A struct column with fields `value` (the authored leaf value) and
   *                            `sequence` (the sequencing clock of the authoring event). The
   *                            struct may not be null, but its values can be.
   */
  private case class LeafAuthorshipResult(
      path: Seq[String],
      private val latestAuthorshipCol: Column) {
    if (latestAuthorshipCol == null) throwNullAuthorshipColumn(path)

    val versionMapKey: String = Scd1VersionMap.serializeKey(path)

    /**
     * The latest value authored for this leaf. It is meaningful only when
     * [[valueAuthoredAtSequence]] is non-null, and may itself be null when the latest authoring
     * event explicitly authored null.
     */
    def authoredValue: Column =
      latestAuthorshipCol.getField(aggregatedLeafValueFieldName)

    /**
     * The sequence at which [[authoredValue]] was authored. A null sequence means no row in the
     * aggregation authored this leaf, so [[authoredValue]] must be disregarded.
     */
    def valueAuthoredAtSequence: Column =
      latestAuthorshipCol.getField(aggregatedLeafSequenceFieldName)
  }

  private object LeafAuthorshipResult {

    /**
     * Constructs the [[LeafAuthorshipResult]] accessor that retrieves the authorship result for a
     * leaf from an aggregated DataFrame.
     *
     * @param context The leaf whose authorship result to access.
     * @param aggregatedDf A DataFrame containing `context.latestAuthorshipCol` evaluated under
     *                     the name [[LeafAuthorshipContext.latestAuthorshipColName]].
     */
    def apply(context: LeafAuthorshipContext, aggregatedDf: DataFrame): LeafAuthorshipResult =
      LeafAuthorshipResult(
        context.path,
        aggregatedDf.col(QuotingUtils.quoteIdentifier(
          LeafAuthorshipContext.latestAuthorshipColName(context.index)))
      )
  }

  private def throwMissingAggregatedLeaves(path: Seq[String]): Nothing =
    throw SparkException.internalError(
      s"Cannot construct aggregated column ${QuotingUtils.quoteNameParts(path)} because it has " +
        "no aggregated leaves.")

  private def throwNullAuthorshipColumn(path: Seq[String]): Nothing =
    throw SparkException.internalError(
      s"Aggregated leaf ${QuotingUtils.quoteNameParts(path)} has a null authorship column.")
}
