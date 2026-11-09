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
import org.apache.spark.sql.catalyst.analysis.ExternalLookupJoinEligibility.NotLookupJoin
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.checkpointing.OffsetSeqMetadata
import org.apache.spark.sql.internal.SQLConf

/**
 * Plans the external lookup joins of a streaming query when it starts, in the thread that starts
 * it, so that a join that cannot be planned fails `start()` instead of the first micro-batch that
 * runs, which may come much later. Whether a join has a lookup predicate, and which ones the
 * connector selects, depend on the condition as the optimizer leaves it, for example after it
 * simplifies the condition. So the query is optimized once here, with the optimizer of its
 * micro-batches and with their configuration, and the result is discarded.
 *
 * Only the lookup sides are planned. The other subtrees without a streaming relation, and the
 * subtrees of subquery plans without an outer reference, become placeholders of their outputs, as
 * in the optimization for the column pruning of streaming scans (see
 * `StreamingV2ScanPushDown.forStartup`), so that their scans are neither built nor planned at the
 * start, as for a query without a lookup join.
 *
 * Only the planning errors of the lookup join and of its connector contract fail the start. Any
 * other error, such as one that an ordinary join raises too, is left to the micro-batch, which
 * plans the query as before.
 */
private[sql] object ExternalLookupJoinStartupCheck extends Logging {
  private val lookupErrorClasses = Set(
    "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED",
    "INVALID_EXTERNAL_LOOKUP_HINT",
    "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP",
    // The default Scan.toExternalLookup() of a scan that does not override it.
    "DATA_SOURCE_EXTERNAL_LOOKUP_NOT_SUPPORTED")

  def apply(session: SparkSession, write: WriteToStream): Unit = {
    val plan = write.inputQuery
    if (!session.sessionState.conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_STARTUP_CHECK_ENABLED) ||
        lookupSidePlans(plan).isEmpty) {
      return
    }
    session.withActive {
      try {
        SQLConf.withExistingConf(startupConf(session, write)) {
          val normalized = QueryExecution.normalize(session, plan)
          // The values that depend on when they are evaluated fold as in the first micro-batch.
          session.sessionState.optimizer.execute(
            StreamingV2ScanPushDown.forStartup(normalized, withStartupValues = false))
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
   * The configuration of the micro-batches of `write`: the session's, with the values that the
   * offset log pins for a restarted query, and without cost-based join reordering, as
   * StreamExecution runs them. Visible for tests.
   */
  private[sql] def startupConf(session: SparkSession, write: WriteToStream): SQLConf = {
    val conf = session.sessionState.conf.clone()
    new StreamingQueryCheckpointMetadata(session, write.resolvedCheckpointLocation, readOnly = true)
      .offsetLog.getLatest().flatMap(_._2.metadataOpt)
      .foreach(OffsetSeqMetadata.setConfWithoutLogging(_, conf))
    conf.setConf(SQLConf.CBO_ENABLED, false)
    conf
  }

  // The lookup sides of the joins in `plan` that must run as external lookup joins.
  private[sql] def lookupSidePlans(plan: LogicalPlan): Seq[LogicalPlan] = plan.collect {
    case join: Join if ExternalLookupJoinEligibility(join) != NotLookupJoin => join.right
  }

  private def errorClass(e: SparkThrowable): String =
    Option(e.getCondition).map(_.takeWhile(_ != '.')).orNull
}
