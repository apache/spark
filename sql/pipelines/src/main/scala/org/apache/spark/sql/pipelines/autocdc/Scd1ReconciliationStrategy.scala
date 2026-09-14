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

import org.apache.spark.sql.classic.DataFrame

/** Strategy for reconciling an SCD1 microbatch. */
private[pipelines] trait Scd1ReconciliationStrategy {

  /**
   * Resolves the CDC events for each key and removes events superseded by recorded tombstones.
   *
   * @param batchDf A validated CDC microbatch containing the key columns and every column needed
   *                to evaluate the sequencing, delete, and column-selection expressions.
   * @param auxiliaryTableDf A snapshot of the auxiliary table containing at least the key columns
   *                         and the CDC metadata column.
   * @return A dataframe containing the selected user columns followed by the CDC metadata column.
   */
  def reconcileMicrobatch(
      processor: Scd1BatchProcessor,
      batchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame
}

/** Row-level SCD1 reconciliation. */
private[pipelines] object Scd1RowLevelReconciliation extends Scd1ReconciliationStrategy {

  /**
   * Keeps the event with the greatest sequencing value for each key, adds its CDC metadata,
   * applies the configured column selection, and removes events superseded by auxiliary-table
   * tombstones.
   */
  override def reconcileMicrobatch(
      processor: Scd1BatchProcessor,
      batchDf: DataFrame,
      auxiliaryTableDf: DataFrame): DataFrame = {
    val deduplicated = processor.deduplicateMicrobatch(validatedMicrobatch = batchDf)
    val withCdcMetadata =
      processor.extendMicrobatchRowsWithCdcMetadata(validatedMicrobatch = deduplicated)
    val projected = processor.projectTargetColumnsOntoMicrobatch(
      microbatchWithCdcMetadataDf = withCdcMetadata
    )
    processor.applyTombstonesToMicrobatch(
      microbatchDf = projected,
      auxiliaryTableDf = auxiliaryTableDf
    )
  }
}
