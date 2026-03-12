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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.internal.{Logging, LogKeys, MessageWithContext}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.RealTimeStreamScanExec
import org.apache.spark.sql.execution.streaming.operators.stateful._

object RealTimeModeAllowlist extends Logging {
  private val allowedSinks = Set(
    "org.apache.spark.sql.execution.streaming.ConsoleTable$",
    "org.apache.spark.sql.execution.streaming.sources.ContinuousMemorySink",
    "org.apache.spark.sql.execution.streaming.sources.ForeachWriterTable",
    "org.apache.spark.sql.kafka010.KafkaSourceProvider$KafkaTable"
  )

  private val allowedOperators = Set(
    "org.apache.spark.sql.execution.AppendColumnsExec",
    "org.apache.spark.sql.execution.CollectMetricsExec",
    "org.apache.spark.sql.execution.ColumnarToRowExec",
    "org.apache.spark.sql.execution.DeserializeToObjectExec",
    "org.apache.spark.sql.execution.ExpandExec",
    "org.apache.spark.sql.execution.FileSourceScanExec",
    "org.apache.spark.sql.execution.FilterExec",
    "org.apache.spark.sql.execution.GenerateExec",
    "org.apache.spark.sql.execution.InputAdapter",
    "org.apache.spark.sql.execution.LocalTableScanExec",
    "org.apache.spark.sql.execution.MapElementsExec",
    "org.apache.spark.sql.execution.MapPartitionsExec",
    "org.apache.spark.sql.execution.PlanLater",
    "org.apache.spark.sql.execution.ProjectExec",
    "org.apache.spark.sql.execution.RangeExec",
    "org.apache.spark.sql.execution.SerializeFromObjectExec",
    "org.apache.spark.sql.execution.UnionExec",
    "org.apache.spark.sql.execution.WholeStageCodegenExec",
    "org.apache.spark.sql.execution.datasources.v2.RealTimeStreamScanExec",
    "org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec",
    "org.apache.spark.sql.execution.exchange.BroadcastExchangeExec",
    "org.apache.spark.sql.execution.exchange.ReusedExchangeExec",
    "org.apache.spark.sql.execution.joins.BroadcastHashJoinExec",
    classOf[EventTimeWatermarkExec].getName
  )

  private def classNamesString(classNames: Seq[String]): MessageWithContext = {
    val sortedClassNames = classNames.sorted
    var message = log"${MDC(LogKeys.CLASS_NAME, sortedClassNames.head)}"
    sortedClassNames.tail.foreach(
      name => message += log", ${MDC(LogKeys.CLASS_NAME, name)}"
    )
    if (sortedClassNames.size > 1) {
      message + log" are"
    } else {
      message + log" is"
    }
  }

  private def notInRTMAllowlistException(
      errorType: String,
      classNames: Seq[String]): SparkIllegalArgumentException = {
    assert(classNames.nonEmpty)
    new SparkIllegalArgumentException(
      errorClass = "STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST",
      messageParameters = Map(
        "errorType" -> errorType,
        "message" -> classNamesString(classNames).message
      )
    )
  }

  def checkAllowedSink(sink: Table, throwException: Boolean): Unit = {
    if (!allowedSinks.contains(sink.getClass.getName)) {
      if (throwException) {
        throw notInRTMAllowlistException("sink", Seq(sink.getClass.getName))
      } else {
        logWarning(
          log"The sink: " + classNamesString(Seq(sink.getClass.getName)) +
          log" not in the sink allowlist for Real-Time Mode."
        )
      }
    }
  }

  // Collect ALL nodes whose entire subtree contains RealTimeStreamScanExec.
  private def collectRealtimeNodes(root: SparkPlan): Seq[SparkPlan] = {

    def collectNodesWhoseSubtreeHasRTS(n: SparkPlan): (Boolean, List[SparkPlan]) = {
      n match {
        case _: RealTimeStreamScanExec =>
          // Subtree has RTS, but we don't collect the RTS node itself.
          (true, Nil)
        case _ if n.children.isEmpty =>
          (false, Nil)
        case _ =>
          val kidResults = n.children.map(collectNodesWhoseSubtreeHasRTS)
          val anyChildHasRTS = kidResults.exists(_._1)
          val collectedKids = kidResults.iterator.flatMap(_._2).toList
          val collectedHere = if (anyChildHasRTS) n :: collectedKids else collectedKids
          (anyChildHasRTS, collectedHere)
      }
    }

    collectNodesWhoseSubtreeHasRTS(root)._2
  }

  def checkAllowedPhysicalOperator(operator: SparkPlan, throwException: Boolean): Unit = {
    val nodesToCheck = collectRealtimeNodes(operator)
    val violations = nodesToCheck
      .collect {
        case node =>
          if (allowedOperators.contains(node.getClass.getName)) {
            None
          } else {
            Some(node.getClass.getName)
          }
      }
      .flatten
      .distinct

    if (violations.nonEmpty) {
      if (throwException) {
        throw notInRTMAllowlistException("operator", violations.toSet.toSeq)
      } else {
        logWarning(
          log"The operator(s): " + classNamesString(violations.toSet.toSeq) +
          log" not in the operator allowlist for Real-Time Mode."
        )
      }
    }
  }
}
