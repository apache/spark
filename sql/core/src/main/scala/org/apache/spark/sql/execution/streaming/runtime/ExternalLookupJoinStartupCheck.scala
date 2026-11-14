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

package org.apache.spark.sql.execution.streaming.runtime

import scala.util.control.NonFatal

import org.apache.spark.SparkThrowable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.ExternalLookupJoinEligibility
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetSeqMetadata,
  OffsetSeqMetadataBase}
import org.apache.spark.sql.internal.SQLConf

/**
 * Plans the external lookup joins of a streaming query when its execution starts, in the query
 * thread before it creates its sources, so that a join that cannot be planned stops the query at
 * once instead of failing the first micro-batch that runs, which may come much later. `start()`
 * has returned by then, so a planning call to the connector that takes long does not block it.
 * Whether a join has a lookup predicate, and which ones the connector selects, depend on the
 * condition as the optimizer leaves it, for example after it simplifies the condition. So the
 * query is optimized once here, with the optimizer of its micro-batches and with their
 * configuration, and the result is discarded.
 *
 * Only the lookup sides are planned. The other subtrees without a streaming relation, and the
 * subtrees of subquery plans without an outer reference, become placeholders of their outputs, as
 * in the optimization for the column pruning of streaming scans (see
 * `StreamingV2ScanPushDown.forStartup`), so that their scans are neither built nor planned at the
 * start, as for a query without a lookup join.
 *
 * Only the planning errors of the lookup join and of its connector contract stop the query, those
 * of the classes in `lookupErrorClasses`. Any other error, such as one that an ordinary join raises
 * too, or an exception that the connector throws while Spark plans the lookup, is left to the
 * micro-batch, which plans the query as before.
 */
private[sql] object ExternalLookupJoinStartupCheck extends Logging {
  private val lookupErrorClasses = Set(
    "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED",
    "INVALID_EXTERNAL_LOOKUP_HINT",
    "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP",
    // The default Scan.toExternalLookup() of a scan that does not override it.
    "DATA_SOURCE_EXTERNAL_LOOKUP_NOT_SUPPORTED")

  /**
   * @param session the session of the stream
   * @param plan the analyzed plan of the query
   * @param offsetLogMetadata the metadata of the last entry of the offset log of the query, if any
   * @param startTimeMs the time at which the query starts, the batch timestamp of a first batch
   */
  def apply(
      session: SparkSession,
      plan: LogicalPlan,
      offsetLogMetadata: Option[OffsetSeqMetadataBase],
      startTimeMs: Long): Unit = {
    if (!session.sessionState.conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_STARTUP_CHECK_ENABLED) ||
        ExternalLookupJoinEligibility.lookupSidePlans(plan).isEmpty) {
      return
    }
    session.withActive {
      try {
        SQLConf.withExistingConf(startupConf(session, offsetLogMetadata)) {
          // As in a micro-batch, the current time and date are the batch timestamp, of the last
          // batch in the offset log, if any, which stays unknown until the optimization ends.
          // Without startup values, date and timestamp conversions fold, as in the first
          // micro-batch.
          val batchTimestampMs =
            offsetLogMetadata.map(_.batchTimestampMs).getOrElse(startTimeMs)
          val rewritten = MicroBatchExecution.withBatchTimestamp(plan, batchTimestampMs)
          // A micro-batch analyzes its plan again with its batch timestamp, which fails as it
          // does for an ordinary stream-static join, for the current time in a join condition.
          // That error stays with the micro-batch, so the planning here, which would report
          // another one, is skipped.
          if (analyzes(session, rewritten)) {
            val normalized = QueryExecution.normalize(session, rewritten)
            session.sessionState.optimizer.execute(
              StreamingV2ScanPushDown.forStartup(normalized, withStartupValues = false))
          }
        }
      } catch {
        case e: SparkThrowable if lookupErrorClasses.contains(errorClass(e)) => throw e
        case NonFatal(e) =>
          logWarning("Could not plan the external lookup joins of the streaming query when " +
            "it starts. Its first micro-batch plans them.", e)
      }
    }
  }

  /**
   * The configuration of the micro-batches: the session's, with the values that the offset log
   * pins for a restarted query, and without cost-based join reordering, as StreamExecution runs
   * them. Visible for tests.
   */
  private[sql] def startupConf(
      session: SparkSession,
      offsetLogMetadata: Option[OffsetSeqMetadataBase]): SQLConf = {
    val conf = session.sessionState.conf.clone()
    offsetLogMetadata.foreach(OffsetSeqMetadata.setConfWithoutLogging(_, conf))
    conf.setConf(SQLConf.CBO_ENABLED, false)
    conf
  }

  /** Whether `plan` passes the analysis checks; logs why not otherwise. */
  private def analyzes(session: SparkSession, plan: LogicalPlan): Boolean =
    try {
      session.sessionState.analyzer.checkAnalysis(plan)
      true
    } catch {
      case NonFatal(e) =>
        logWarning("The streaming query does not pass analysis with the batch timestamp of a " +
          "micro-batch, so its external lookup joins are not planned when it starts. Its first " +
          "micro-batch reports the error.", e)
        false
    }

  private def errorClass(e: SparkThrowable): String =
    Option(e.getCondition).map(_.takeWhile(_ != '.')).orNull
}
