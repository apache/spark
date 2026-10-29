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

import org.apache.spark.sql.{functions => F, AnalysisException, Column}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.classic.DataFrame

/**
 * Per-microbatch input validation shared by SCD merge executors. Throws with a clear,
 * user-actionable error if the batch violates the CDC contract.
 *
 * @param destinationIdentifier The identifier of the target table, used for error messages.
 * @param changeArgs The user-specified AutoCDC parameters.
 * @param batchDf The incoming microbatch to validate.
 * @param batchId The structured-streaming batch id, used for error messages.
 */
case class ScdBatchValidator(
    destinationIdentifier: TableIdentifier,
    changeArgs: ChangeArgs,
    batchDf: DataFrame,
    batchId: Long) {

  /**
   * Validates that the sequencing column is orderable and that no row has a null sequencing
   * value or a null value in any key column. The per-row checks are folded into a single
   * aggregation so the microbatch is scanned exactly once.
   */
  def validateMicrobatch(): Unit = {
    val seqType = batchDf.select(changeArgs.sequencing).schema.head.dataType
    if (!RowOrdering.isOrderable(seqType)) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_MICROBATCH_VALIDATION.NON_ORDERABLE_SEQUENCE",
        messageParameters = Map(
          "tableName" -> destinationIdentifier.quotedString,
          "batchId" -> batchId.toString,
          "dataType" -> seqType.catalogString
        )
      )
    }

    val sequencingNullCount: Column =
      F.count(F.when(changeArgs.sequencing.isNull, F.lit(1))).as("__autocdc_seq_null_count")
    val perKeyNullCount: Seq[Column] = changeArgs.keys.map { key =>
      F.count(F.when(F.col(key.quoted).isNull, F.lit(1)))
        .as(s"__autocdc_key_null_count_${key.name}")
    }
    // The null count aggregations are laid out in the returned dataframe as:
    // [# rows with null sequence, # rows with null for key1, ..., # rows with null for keyN].
    val nullCountsResultDf =
      batchDf.agg(sequencingNullCount, perKeyNullCount: _*).head()

    val numRowsWithNullSequence = nullCountsResultDf.getLong(0)
    if (numRowsWithNullSequence > 0) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE",
        messageParameters = Map(
          "tableName" -> destinationIdentifier.quotedString,
          "batchId" -> batchId.toString,
          "nullCount" -> numRowsWithNullSequence.toString
        )
      )
    }

    val keysWithNullEntries = changeArgs.keys.zipWithIndex.flatMap { case (key, idx) =>
      val rowCountForKey = nullCountsResultDf.getLong(idx + 1)
      Option.when(rowCountForKey > 0)(key -> rowCountForKey)
    }
    if (keysWithNullEntries.nonEmpty) {
      val nullKeyCounts = keysWithNullEntries
        .map { case (key, count) => s"${key.quoted}=$count" }
        .mkString(", ")

      throw new AnalysisException(
        errorClass = "AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY",
        messageParameters = Map(
          "tableName" -> destinationIdentifier.quotedString,
          "batchId" -> batchId.toString,
          "nullKeyCounts" -> nullKeyCounts
        )
      )
    }
  }
}
