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
   */
  def extendMicrobatchRowsWithCdcMetadata(microbatchDf: DataFrame): DataFrame = {
    // Proactively validate the reserved CDC metadata column does not exist in the microbatch.
    validateCdcMetadataColumnNotPresent(microbatchDf)

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

    microbatchDf.withColumn(
      Scd1BatchProcessor.cdcMetadataColName,
      Scd1BatchProcessor.constructCdcMetadataCol(
        deleteSequence = rowDeleteSequence,
        upsertSequence = rowUpsertSequence,
        sequencingType = resolvedSequencingType
      )
    )
  }

  private def validateCdcMetadataColumnNotPresent(microbatchDf: DataFrame): Unit = {
    val ignoreColumnNameCase =
      !microbatchDf.sparkSession.sessionState.conf.caseSensitiveAnalysis

    microbatchDf.schema.fieldNames
      .find { fieldName =>
        if (ignoreColumnNameCase) {
          fieldName.equalsIgnoreCase(Scd1BatchProcessor.cdcMetadataColName)
        } else {
          fieldName.equals(Scd1BatchProcessor.cdcMetadataColName)
        }
      }
      .foreach { conflictingColumnName =>
        throw new AnalysisException(
          errorClass = "AUTOCDC_RESERVED_COLUMN_NAME_CONFLICT",
          messageParameters = Map(
            "caseSensitivity" -> CaseSensitivityLabels.of(!ignoreColumnNameCase),
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
