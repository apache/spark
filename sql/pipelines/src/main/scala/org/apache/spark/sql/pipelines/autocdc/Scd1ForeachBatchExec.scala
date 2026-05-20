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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.DataFrame

/**
 * Exposes an API to execute one SCD Type 1 AutoCDC microbatch reconciliation on a
 * foreachBatch streaming query.
 */
case class Scd1ForeachBatchExec(
    batchProcessor: Scd1BatchProcessor,
    auxiliaryTableIdentifier: TableIdentifier,
    targetTableIdentifier: TableIdentifier) {

  /**
   * Process a single CDC microbatch and merge it into the auxiliary and target tables.
   */
  def execute(batchDf: DataFrame, batchId: Long): Unit = {
    ScdBatchValidator(
      destinationIdentifier = targetTableIdentifier,
      changeArgs = batchProcessor.changeArgs,
      batchDf = batchDf,
      batchId = batchId
    ).validateMicrobatch()

    val deduplicatedMicrobatch = batchProcessor.deduplicateMicrobatch(
      validatedMicrobatch = batchDf
    )

    val microbatchWithCdcMetadata = batchProcessor.extendMicrobatchRowsWithCdcMetadata(
      microbatchDf = deduplicatedMicrobatch
    )

    val projectedMicrobatch = batchProcessor.projectTargetColumnsOntoMicrobatch(
      microbatchWithCdcMetadataDf = microbatchWithCdcMetadata
    )

    val reconciledMicrobatch = batchProcessor.applyTombstonesToMicrobatch(
      microbatchDf = projectedMicrobatch,
      auxiliaryTableDf = batchDf.sparkSession.read.table(
        auxiliaryTableIdentifier.quotedString
      )
    )

    batchProcessor.mergeMicrobatchOntoAuxiliaryTable(
      reconciledMicrobatchDf = reconciledMicrobatch,
      auxiliaryTableIdentifier = auxiliaryTableIdentifier
    )

    batchProcessor.mergeMicrobatchOntoTarget(
      reconciledMicrobatchDf = reconciledMicrobatch,
      targetTableIdentifier = targetTableIdentifier
    )
  }
}
