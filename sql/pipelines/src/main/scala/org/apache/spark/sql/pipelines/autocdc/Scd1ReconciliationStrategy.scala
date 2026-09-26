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
import org.apache.spark.sql.types.DataType
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
