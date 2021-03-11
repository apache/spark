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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.types.BooleanType

/**
 * Checks the capabilities of Data Source V2 tables, and fail problematic queries earlier.
 */
object TableCapabilityCheck extends (LogicalPlan => Unit) {
  import DataSourceV2Implicits._

  private def failAnalysis(msg: String): Unit = throw new AnalysisException(msg)

  private def supportsBatchWrite(table: Table): Boolean = {
    table.supportsAny(BATCH_WRITE, V1_BATCH_WRITE)
  }

  override def apply(plan: LogicalPlan): Unit = {
    plan foreach {
      case r: DataSourceV2Relation if !r.table.supports(BATCH_READ) =>
        failAnalysis(s"Table ${r.table.name()} does not support batch scan.")

      case r: StreamingRelationV2 if !r.table.supportsAny(MICRO_BATCH_READ, CONTINUOUS_READ) =>
        throw new AnalysisException(s"Table ${r.table.name()} does not support either " +
          "micro-batch or continuous scan.")

      // TODO: check STREAMING_WRITE capability. It's not doable now because we don't have a
      //       a logical plan for streaming write.
      case AppendData(r: DataSourceV2Relation, _, _, _) if !supportsBatchWrite(r.table) =>
        failAnalysis(s"Table ${r.table.name()} does not support append in batch mode.")

      case OverwritePartitionsDynamic(r: DataSourceV2Relation, _, _, _)
        if !r.table.supports(BATCH_WRITE) || !r.table.supports(OVERWRITE_DYNAMIC) =>
        failAnalysis(s"Table ${r.table.name()} does not support dynamic overwrite in batch mode.")

      case OverwriteByExpression(r: DataSourceV2Relation, expr, _, _, _) =>
        expr match {
          case Literal(true, BooleanType) =>
            if (!supportsBatchWrite(r.table) ||
                !r.table.supportsAny(TRUNCATE, OVERWRITE_BY_FILTER)) {
              failAnalysis(
                s"Table ${r.table.name()} does not support truncate in batch mode.")
            }
          case _ =>
            if (!supportsBatchWrite(r.table) || !r.table.supports(OVERWRITE_BY_FILTER)) {
              failAnalysis(s"Table ${r.table.name()} does not support " +
                "overwrite by filter in batch mode.")
            }
        }

      case _ => // OK
    }

    // The streaming sources in a query should all support micro-batch scan, or all support
    // continuous scan.
    val streamingSources = plan.collect {
      case r: StreamingRelationV2 => r.table
    }
    val v1StreamingRelations = plan.collect {
      case r: StreamingRelation => r
    }

    if (streamingSources.length + v1StreamingRelations.length > 1) {
      val allSupportsMicroBatch = streamingSources.forall(_.supports(MICRO_BATCH_READ))
      // v1 streaming data source only supports micro-batch.
      val allSupportsContinuous = streamingSources.forall(_.supports(CONTINUOUS_READ)) &&
        v1StreamingRelations.isEmpty
      if (!allSupportsMicroBatch && !allSupportsContinuous) {
        val microBatchSources =
          streamingSources.filter(_.supports(MICRO_BATCH_READ)).map(_.name()) ++
            v1StreamingRelations.map(_.sourceName)
        val continuousSources = streamingSources.filter(_.supports(CONTINUOUS_READ)).map(_.name())
        throw new AnalysisException(
          "The streaming sources in a query do not have a common supported execution mode.\n" +
            "Sources support micro-batch: " + microBatchSources.mkString(", ") + "\n" +
            "Sources support continuous: " + continuousSources.mkString(", "))
      }
    }
  }
}
