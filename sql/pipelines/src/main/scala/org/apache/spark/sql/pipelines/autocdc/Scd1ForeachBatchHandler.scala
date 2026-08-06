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
case class Scd1ForeachBatchHandler(
    batchProcessor: Scd1BatchProcessor,
    auxiliaryTableIdentifier: TableIdentifier,
    targetTableIdentifier: TableIdentifier) {

  /**
   * Process a single CDC microbatch and merge it into the auxiliary and target tables.
   *
   * Idempotent under same-`batchId` replay: both merges are gated on sequence inequalities,
   * so a partial failure between them is reconciled correctly when foreachBatch retries the
   * whole batch.
   */
  def execute(batchDf: DataFrame, batchId: Long): Unit = {
    ScdBatchValidator(
      destinationIdentifier = targetTableIdentifier,
      changeArgs = batchProcessor.changeArgs,
      batchDf = batchDf,
      batchId = batchId
    ).validateMicrobatch()

    val reconciledMicrobatch = batchProcessor.reconcileMicrobatch(
      batchDf = batchDf,
      // Aux holds at most one row per currently-active tombstone (revived keys are GC'd
      // by mergeMicrobatchOntoAuxiliaryTable), so it generally stays small enough for a broadcast
      // join. Future optimizations: key-pruned reads, table format-aware clustering and tombstone
      // TTL.
      auxiliaryTableDf = batchDf.sparkSession.read.table(
        auxiliaryTableIdentifier.quotedString
      )
    )

    batchProcessor.mergeMicrobatchOntoAuxiliaryTable(
      reconciledMicrobatchDf = reconciledMicrobatch,
      auxiliaryTableIdentifier = auxiliaryTableIdentifier
    )

    // Failure between these two merges is safe under foreachBatch retry: the aux merge
    // only ever mutates a tombstone when this batch's event makes it stale (strictly newer
    // delete advances it) or redundant (`>=` upsert revives the key, GC'ing the tombstone),
    // so on retry those preconditions no longer hold against the just-advanced aux state -
    // the aux merge is a no-op and the target merge replays as if for the first time.
    batchProcessor.mergeMicrobatchOntoTarget(
      reconciledMicrobatchDf = reconciledMicrobatch,
      targetTableIdentifier = targetTableIdentifier
    )
  }
}
