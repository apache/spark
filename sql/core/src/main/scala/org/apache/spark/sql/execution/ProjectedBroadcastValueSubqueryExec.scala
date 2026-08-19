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

package org.apache.spark.sql.execution

import java.util.concurrent.{Future => JFuture}

import scala.concurrent.duration.Duration

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Expression, NamedExpression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.BroadcastValueResult.{Available, Unavailable}
import org.apache.spark.sql.execution.joins.HashedRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.ThreadUtils

/** Collects pruning values from the full rows of an already required hash broadcast. */
case class ProjectedBroadcastValueSubqueryExec(
    name: String,
    valueExpression: Expression,
    child: SparkPlan) extends BaseSubqueryExec with UnaryExecNode {

  override def output: Seq[Attribute] = {
    val outputName = valueExpression match {
      case named: NamedExpression => named.name
      case Cast(named: NamedExpression, _, _, _) => named.name
      case _ => "key"
    }
    Seq(AttributeReference(outputName, valueExpression.dataType, valueExpression.nullable)())
  }

  override lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "projectionDisabled" -> SQLMetrics.createMetric(sparkContext, "projection disabled"),
    "projectionErrors" -> SQLMetrics.createMetric(sparkContext, "projection errors"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"))

  override def doCanonicalize(): SparkPlan = {
    ProjectedBroadcastValueSubqueryExec(
      "dpp",
      QueryPlan.normalizeExpressions(valueExpression, child.output),
      child.canonicalized)
  }

  @transient
  private lazy val relationFuture: JFuture[BroadcastValueResult[Array[InternalRow]]] = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLExecution.withThreadLocalCaptured[BroadcastValueResult[Array[InternalRow]]](
      session, SubqueryBroadcastExec.executionContext) {
      SQLExecution.withExecutionId(session, executionId) {
        val beforeCollect = System.nanoTime()
        val broadcast = child.executeBroadcast[HashedRelation]()
        val limits = BroadcastValueProjectionLimits(
          maxInputRows = conf.dynamicPartitionPruningBroadcastProjectionMaxRows.toLong,
          maxOutputBytes = conf.dynamicPartitionPruningBroadcastProjectionMaxBytes,
          maxSourceBytes = conf.dynamicPartitionPruningBroadcastProjectionMaxSourceBytes)
        val result = BroadcastValueProjector.collectExactValueDomain(
          broadcast,
          child,
          valueExpression,
          limits,
          onInputRow = () => longMetric("numInputRows") += 1,
          onError = () => longMetric("projectionErrors") += 1)

        longMetric("collectTime") += (System.nanoTime() - beforeCollect) / 1000000
        result match {
          case Available(rows) =>
            longMetric("numOutputRows") += rows.length
            longMetric("dataSize") +=
              rows.iterator.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
          case Unavailable =>
            longMetric("projectionDisabled") += 1
        }
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        result
      }
    }
  }

  override protected def doPrepare(): Unit = {
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.executeCodePathUnsupportedError(
      "ProjectedBroadcastValueSubqueryExec")
  }

  override def executeCollect(): Array[InternalRow] = executeCollectResult() match {
    case Available(rows) => rows
    case Unavailable =>
      throw SparkException.internalError(
        "An unavailable projected broadcast value domain must be consumed through " +
          "executeCollectResult.")
  }

  private[execution] def executeCollectResult(): BroadcastValueResult[Array[InternalRow]] =
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[id=#$id]")

  override protected def withNewChildInternal(
      newChild: SparkPlan): ProjectedBroadcastValueSubqueryExec = copy(child = newChild)
}

object ProjectedBroadcastValueSubqueryExec {
  private[execution] def resultOf(
      plan: BaseSubqueryExec): Option[BroadcastValueResult[Array[InternalRow]]] = {
    plan match {
      case projected: ProjectedBroadcastValueSubqueryExec =>
        Some(projected.executeCollectResult())
      case ReusedSubqueryExec(child) => resultOf(child)
      case _ => None
    }
  }
}
