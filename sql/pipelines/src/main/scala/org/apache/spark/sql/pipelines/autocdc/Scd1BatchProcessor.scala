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
import org.apache.spark.sql.{functions => F, AnalysisException}
import org.apache.spark.sql.Column
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
  def deduplicateMicrobatch(validatedMicrobatch: DataFrame): DataFrame = {
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
  def extendMicrobatchRowsWithCdcMetadata(validatedMicrobatch: DataFrame): DataFrame = {
    // Proactively validate the reserved CDC metadata column does not exist in the microbatch.
    validateCdcMetadataColumnNotPresent(validatedMicrobatch)

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
      Scd1BatchProcessor.cdcMetadataColName,
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
  def projectTargetColumnsOntoMicrobatch(microbatchWithCdcMetadataDf: DataFrame): DataFrame = {
    val ignoreColumnNameCase =
      !microbatchWithCdcMetadataDf.sparkSession.sessionState.conf.caseSensitiveAnalysis

    // The schema of the microbatch less the system-projected CDC metadata column, i.e. the
    // original microbatch schema.
    val userColumnsInMicrobatchSchema =
      StructType(
        microbatchWithCdcMetadataDf.schema.fields.filterNot { field =>
          if (ignoreColumnNameCase) {
            field.name.equalsIgnoreCase(Scd1BatchProcessor.cdcMetadataColName)
          } else {
            field.name.equals(Scd1BatchProcessor.cdcMetadataColName)
          }
        }
      )

    val userSelectedColumnsInMicrobatchSchema =
      ColumnSelection.applyToSchema(
        schemaName = "microbatch",
        schema = userColumnsInMicrobatchSchema,
        columnSelection = changeArgs.columnSelection,
        ignoreCase = ignoreColumnNameCase
      )

    // In addition to the explicit user-selected columns, re-project the operational CDC metadata
    // column as the last column.
    val finalColumnsInMicrobatchToSelect =
      userSelectedColumnsInMicrobatchSchema.fieldNames.map(colName => {
        // Spark drops backticks in the schema, quote all identifiers for safety before executing
        // select. Identifiers could have special characters such as '.'.
        F.col(QuotingUtils.quoteIdentifier(colName))
      }) :+ F.col(
        Scd1BatchProcessor.cdcMetadataColName
      )

    microbatchWithCdcMetadataDf.select(
      finalColumnsInMicrobatchToSelect.toImmutableArraySeq: _*
    )
  }

  private def validateCdcMetadataColumnNotPresent(microbatch: DataFrame): Unit = {
    val microbatchSqlConf = microbatch.sparkSession.sessionState.conf
    val resolver = microbatchSqlConf.resolver

    microbatch.schema.fieldNames
      .find(resolver(_, Scd1BatchProcessor.cdcMetadataColName))
      .foreach { conflictingColumnName =>
        throw new AnalysisException(
          errorClass = "AUTOCDC_RESERVED_COLUMN_NAME_CONFLICT",
          messageParameters = Map(
            "caseSensitivity" -> CaseSensitivityLabels.of(microbatchSqlConf.caseSensitiveAnalysis),
            "columnName" -> conflictingColumnName,
            "schemaName" -> "microbatch",
            "reservedColumnName" -> Scd1BatchProcessor.cdcMetadataColName
          )
        )
      }
  }
}

object Scd1BatchProcessor {
  // Columns prefixed with `__spark_autocdc_` are reserved for internal SDP AutoCDC processing.
  private[autocdc] val winningRowColName: String = "__spark_autocdc_winning_row"
  private[autocdc] val cdcMetadataColName: String = "__spark_autocdc_metadata"

  private[autocdc] val cdcDeleteSequenceFieldName: String = "deleteSequence"
  private[autocdc] val cdcUpsertSequenceFieldName: String = "upsertSequence"

  /**
   * Schema of the CDC metadata struct column for SCD1.
   */
  private def cdcMetadataColSchema(sequencingType: DataType): StructType =
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
  private[autocdc] def constructCdcMetadataCol(
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
