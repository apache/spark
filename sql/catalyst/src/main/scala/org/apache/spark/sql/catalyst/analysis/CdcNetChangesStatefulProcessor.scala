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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkException
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.connector.catalog.Changelog
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.StructType

/**
 * StatefulProcessor that incrementalises CDC net-change computation for streaming reads.
 *
 * The batch path (`ResolveChangelogTable.injectNetChangeComputation`) uses a Catalyst
 * `Window` partitioned by `rowId` and ordered by `(_commit_version, change_type_rank)` to
 * extract the first and last events per row identity, then applies the SPIP collapse
 * matrix on `(existedBefore, existsAfter)`. That `Window` is rejected on streaming
 * queries (`NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING`).
 *
 * This processor reuses the same SPIP collapse matrix with `transformWithState`, applied
 * per watermark window rather than over the full requested version range. Per-row-identity
 * state stores the first event ever observed and the most-recent event observed; an event
 * time timer keyed on `_commit_timestamp` advances with each batch and fires once the
 * global watermark passes the latest event time observed for the key, at which point the
 * SPIP matrix is evaluated and the net result is emitted. See the paragraph below for how
 * the per-window collapse differs from batch netChanges' range-scoped collapse.
 *
 * Output schema: identical to the connector's changelog schema.
 *
 * Streaming netChanges is incremental: per-row-identity state is cleared once its current
 * net result is emitted (timer fire or end-of-stream flush). Subsequent commits on the same
 * identity arrive against empty state and produce additional output rows independently. This
 * differs from batch netChanges, which collapses every change for a row identity across the
 * entire requested version range; the streaming path cannot retract previously emitted output
 * to match that range-scoped collapse. For example, with id=1 inserted at v1 and deleted at
 * v3 and an unrelated commit at v2 in between, batch netChanges over [v1..v3] emits nothing
 * for id=1, while streaming emits an `insert` (after v2 advances the watermark past v1) and
 * later a `delete` (after end-of-stream or another commit advances the watermark past v3).
 *
 * End-of-stream flushes all pending timers, so a bounded stream's output matches a batch
 * netChanges only when no row identity is touched again after its first emission.
 *
 * @param inputSchema    schema of the rows fed into this processor; the connector's
 *                       changelog schema (data columns + `_change_type` +
 *                       `_commit_version` + `_commit_timestamp`) optionally extended with
 *                       rowId helper columns added by
 *                       [[org.apache.spark.sql.catalyst.analysis.ResolveChangelogTable]].
 * @param computeUpdates whether `(existedBefore, existsAfter) = (true, true)` should be
 *                       relabeled as `update_preimage` / `update_postimage` (true) or kept
 *                       as `delete` / `insert` (false), matching the batch contract.
 */
private[analysis] class CdcNetChangesStatefulProcessor(
    inputSchema: StructType,
    computeUpdates: Boolean)
  extends StatefulProcessor[Row, Row, Row] {

  @transient private var firstEvent: ValueState[Row] = _
  @transient private var lastEvent: ValueState[Row] = _

  // Hoisted out of `relabel` so we don't pay a linear `fieldIndex` scan per emitted row.
  private val changeTypeIdx: Int = inputSchema.fieldIndex("_change_type")
  private val commitVersionIdx: Int = inputSchema.fieldIndex("_commit_version")

  // `_commit_version` is connector-defined and is restricted to LongType or StringType
  // (validated in `ChangelogTable.validateSchema`). We still route through Catalyst's
  // type-aware interpreted ordering for symmetry with the batch path's `SortOrder` on
  // the same attribute.
  private val versionDataType = inputSchema(commitVersionIdx).dataType
  private val versionToCatalyst: Any => Any =
    CatalystTypeConverters.createToCatalystConverter(versionDataType)
  private val versionInternalOrdering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(versionDataType)
  private val versionOrdering: Ordering[Row] = new Ordering[Row] {
    override def compare(a: Row, b: Row): Int = {
      val av = versionToCatalyst(a.get(commitVersionIdx))
      val bv = versionToCatalyst(b.get(commitVersionIdx))
      versionInternalOrdering.compare(av, bv)
    }
  }

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    val handle = getHandle
    val rowEncoder: Encoder[Row] = ExpressionEncoder(inputSchema)
    firstEvent = handle.getValueState[Row]("firstEvent", rowEncoder, TTLConfig.NONE)
    lastEvent = handle.getValueState[Row]("lastEvent", rowEncoder, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: Row,
      inputRows: Iterator[Row],
      timerValues: TimerValues): Iterator[Row] = {
    val handle = getHandle
    // Sort by (_commit_version, change_type rank) -- pre-events (delete /
    // update_preimage) before post-events (insert / update_postimage) within a single
    // commit version, matching the batch path's `(_commit_version, change_type_rank)`
    // ordering. We compose the type-aware Catalyst version ordering with the rank
    // ordering as a tiebreaker.
    val sorted = inputRows.toSeq.sorted(versionOrdering.orElse(Ordering.by { row =>
      row.getAs[String](changeTypeIdx) match {
        case Changelog.CHANGE_TYPE_UPDATE_PREIMAGE | Changelog.CHANGE_TYPE_DELETE => 0
        case Changelog.CHANGE_TYPE_INSERT | Changelog.CHANGE_TYPE_UPDATE_POSTIMAGE => 1
        case _ => throw new SparkException(
          errorClass = "CHANGELOG_CONTRACT_VIOLATION.UNEXPECTED_CHANGE_TYPE",
          messageParameters = Map.empty,
          cause = null)
      }
    }))
    if (sorted.isEmpty) return Iterator.empty

    if (!firstEvent.exists()) {
      firstEvent.update(sorted.head)
    }
    lastEvent.update(sorted.last)

    // Re-arm the per-key event-time timer to the latest observed `_commit_timestamp`.
    // Without dropping any existing timers we'd risk an earlier timer firing first and
    // emitting state that later events would then re-populate, producing duplicate
    // output for the same row identity.
    //
    // A NULL `_commit_timestamp` cannot be turned into a timer epoch and would NPE on
    // `getTime()`. The `Changelog` Javadoc requires non-NULL `_commit_timestamp` on
    // streaming reads engaging post-processing, so we surface the contract violation
    // with a clear error class rather than failing the micro-batch with an opaque NPE.
    val ts = sorted.last.getAs[java.sql.Timestamp]("_commit_timestamp")
    if (ts == null) {
      throw new SparkException(
        errorClass = "CHANGELOG_CONTRACT_VIOLATION.NULL_COMMIT_TIMESTAMP",
        messageParameters = Map.empty,
        cause = null)
    }
    val newTimerMs = ts.getTime
    val existing = handle.listTimers().toList
    existing.foreach(handle.deleteTimer)
    handle.registerTimer(newTimerMs)

    Iterator.empty
  }

  override def handleExpiredTimer(
      key: Row,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[Row] = {
    if (!firstEvent.exists()) return Iterator.empty

    val first = firstEvent.get()
    val last = lastEvent.get()
    val firstChangeType = first.getAs[String]("_change_type")
    val lastChangeType = last.getAs[String]("_change_type")

    val existedBefore =
      firstChangeType == Changelog.CHANGE_TYPE_DELETE ||
        firstChangeType == Changelog.CHANGE_TYPE_UPDATE_PREIMAGE
    val existsAfter =
      lastChangeType == Changelog.CHANGE_TYPE_INSERT ||
        lastChangeType == Changelog.CHANGE_TYPE_UPDATE_POSTIMAGE

    val (preLabel, postLabel) =
      if (computeUpdates) {
        (Changelog.CHANGE_TYPE_UPDATE_PREIMAGE, Changelog.CHANGE_TYPE_UPDATE_POSTIMAGE)
      } else {
        (Changelog.CHANGE_TYPE_DELETE, Changelog.CHANGE_TYPE_INSERT)
      }

    val out: Iterator[Row] = (existedBefore, existsAfter) match {
      case (false, false) => Iterator.empty
      case (false, true) => Iterator(relabel(last, Changelog.CHANGE_TYPE_INSERT))
      case (true, false) => Iterator(relabel(first, Changelog.CHANGE_TYPE_DELETE))
      case (true, true) => Iterator(relabel(first, preLabel), relabel(last, postLabel))
    }

    firstEvent.clear()
    lastEvent.clear()
    out
  }

  private def relabel(row: Row, newChangeType: String): Row = {
    val values = row.toSeq.toArray
    values(changeTypeIdx) = newChangeType
    new GenericRowWithSchema(values, inputSchema)
  }
}
