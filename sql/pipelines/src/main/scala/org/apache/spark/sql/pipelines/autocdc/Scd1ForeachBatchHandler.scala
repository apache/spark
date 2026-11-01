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
   * Process a single CDC microbatch and merge it into the target and auxiliary tables.
   *
   * Idempotent under same-`batchId` replay: when foreachBatch retries the whole batch after a
   * failure at any point, the retry leaves both tables as an uninterrupted run would.
   */
  def execute(batchDf: DataFrame, batchId: Long): Unit = {
    ScdBatchValidator(
      destinationIdentifier = targetTableIdentifier,
      changeArgs = batchProcessor.changeArgs,
      batchDf = batchDf,
      batchId = batchId
    ).validateMicrobatch()

    // Reconciliation reduces the microbatch to at most one row per key, its net event: the change
    // the key's microbatch events make once its persisted tombstone, if any, is applied. A net
    // event is either a net delete or a net upsert, and a key whose microbatch events all predate
    // its tombstone has none. Row-level reconciliation takes the key's latest event as its net
    // event, while leaf-level reconciliation combines its events field by field.
    val reconciledMicrobatch = batchProcessor.reconcileMicrobatch(
      validatedBatchDf = batchDf,
      // Aux holds at most one row per currently-active tombstone (revived keys are GC'd
      // by mergeMicrobatchOntoAuxiliaryTable), so it generally stays small enough for a broadcast
      // join. Future optimizations: key-pruned reads, table format-aware clustering and tombstone
      // TTL.
      auxiliaryTableDf = batchDf.sparkSession.read.table(
        auxiliaryTableIdentifier.quotedString
      )
    )

    // Merge order and retry safety. Each merge commits atomically, so a full foreachBatch retry
    // can follow a failure that occurs before either merge, after the first, or after both but
    // before the batch is recorded as complete. Each merge reads only the reconciled rows and its
    // own table, and applies an incoming change only if it is at least as new as what that table
    // records, so replaying it with the same reconciled rows leaves its table unchanged.
    // Microbatch reconciliation, which every retry reruns before merging, reads at most the
    // target's schema, never its rows, so the target merge never changes what a retry reconciles.
    // It does read the aux tombstones, which the aux merge advances for a newer incoming net
    // delete, adds for an untracked key's incoming net delete, and removes when the key's
    // incoming net upsert supersedes them.
    //
    // Row-level reconciliation: either merge order is safe. A tombstone's only effect is to
    // discard the key's net event when that event is strictly older. A tombstone that the aux
    // merge advances or adds carries the net delete's own sequence, and one that it removes is
    // superseded by the net upsert, so in neither case would the tombstone discard the event. The
    // aux merge therefore never changes what a retry reconciles either, so a retry after any
    // failure reconciles the same rows and replays each completed merge without effect.
    //
    // Leaf-level reconciliation: the target must merge first. During microbatch reconciliation, a
    // tombstone authors null for each field at its sequence, so it can outrank an individual
    // field's older authorship within a key's net upsert, even though the net upsert as a whole
    // supersedes the tombstone. That same net upsert is what makes the aux merge remove it. A
    // removed tombstone can thus have shaped the batch's reconciled rows, and a retry would
    // reconcile them without it.
    //
    // Merging the aux table after the target table is safe in every failure case:
    //   - Failure before the target merge commits: neither table has changed, so a retry runs as
    //     if for the first time.
    //   - Failure after the target merge but before the aux merge commits: the aux table is
    //     unchanged, so a retry reconciles the same rows, replays the target merge without effect,
    //     and runs the aux merge anew.
    //   - Failure after both merges: a retry reconciles without the removed tombstones, giving
    //     the fields they authored older authorship or none. The target's version maps already
    //     record at least the tombstones' sequences for those fields, so the target keeps them,
    //     and everything else replays without effect.
    //
    // Merging the aux table first would be unsafe: a failure between the merges would remove the
    // tombstones before the target merge records the fields they authored, and a retry would
    // write the older values they nulled.
    batchProcessor.mergeMicrobatchOntoTarget(
      reconciledMicrobatchDf = reconciledMicrobatch,
      targetTableIdentifier = targetTableIdentifier
    )

    batchProcessor.mergeMicrobatchOntoAuxiliaryTable(
      reconciledMicrobatchDf = reconciledMicrobatch,
      auxiliaryTableIdentifier = auxiliaryTableIdentifier
    )
  }
}
