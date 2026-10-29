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

package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter}
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{ExternalLookupReaderFactory, Scan}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.SupportsCustomDriverMetrics
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.Utils

/**
 * Executes synchronous, parameterized external lookup joins without a shuffle. Each task joins
 * its input rows in batches, through an [[ExternalLookupJoinEvaluatorFactory]].
 */
case class ExternalLookupJoinExec(
    lookupParameters: Seq[Expression],
    lookupInput: Seq[Attribute],
    lookupOutput: Seq[Attribute],
    nullSafeParameters: Seq[Boolean],
    joinType: JoinType,
    residualCondition: Option[Expression],
    @transient scan: Scan,
    readerFactory: ExternalLookupReaderFactory,
    maxParameterRowsPerRequest: Int,
    child: SparkPlan) extends UnaryExecNode with SupportsCustomDriverMetrics {

  override def output: Seq[Attribute] = joinType match {
    case Inner => child.output ++ lookupOutput
    case LeftOuter => child.output ++ lookupOutput.map(_.withNullability(true))
    case _ => child.output ++ lookupOutput
  }

  override def references: AttributeSet =
    AttributeSet(lookupParameters.flatMap(_.references)) ++
      residualCondition.map(_.references.intersect(child.outputSet)).getOrElse(AttributeSet.empty)

  // A connector's custom metrics are registered under prefixed keys, so that they never collide
  // with the operator's own metrics; the UI shows the connector's descriptions.
  override lazy val customMetrics: Map[String, SQLMetric] =
    scan.supportedCustomMetrics().map { metric =>
      ExternalLookupJoinExec.sourceMetricKey(metric.name()) ->
        SQLMetrics.createV2CustomMetric(sparkContext, metric)
    }.toMap

  override protected lazy val sparkMetrics: Map[String, SQLMetric] =
    ExternalLookupJoinExec.OPERATOR_METRICS.map { metric =>
      val sqlMetric = if (metric.isTiming) {
        SQLMetrics.createNanoTimingMetric(sparkContext, metric.description)
      } else {
        SQLMetrics.createMetric(sparkContext, metric.description)
      }
      metric.name -> sqlMetric
    }.toMap

  override protected def doExecute(): RDD[InternalRow] = {
    postDriverMetrics(scan.reportDriverMetrics().map { metric =>
      new CustomTaskMetric {
        override def name(): String = ExternalLookupJoinExec.sourceMetricKey(metric.name())
        override def value(): Long = metric.value()
      }
    })
    val evaluatorFactory = new ExternalLookupJoinEvaluatorFactory(
      lookupParameters,
      nullSafeParameters,
      child.output,
      lookupOutput,
      output,
      joinType,
      residualCondition,
      readerFactory,
      math.min(ExternalLookupJoinExec.MAX_PARAMETER_ROWS_PER_REQUEST, maxParameterRowsPerRequest),
      ExternalLookupJoinMetrics(
        outputRows = longMetric("numOutputRows"),
        inputRows = longMetric("numInputRows"),
        inputBatches = longMetric("numInputBatches"),
        requests = longMetric("numLookupRequests"),
        parameterRows = longMetric("numLookupParameterRows"),
        lookupTime = longMetric("lookupTime")),
      new ExternalLookupReaderMetrics(customMetrics))
    if (conf.usePartitionEvaluator) {
      child.execute().mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      child.execute().mapPartitionsWithIndexInternal { (index, input) =>
        evaluatorFactory.createEvaluator().eval(index, input)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExternalLookupJoinExec =
    copy(child = newChild)

  override def simpleString(maxFields: Int): String = {
    Utils.redact(conf.stringRedactionPattern, s"$nodeName ${scan.description()}")
  }

  override protected def stringArgs: Iterator[Any] =
    Iterator(child, simpleString(conf.maxToStringFields))
}

object ExternalLookupJoinExec {
  /**
   * Spark's cap on the parameter rows of one request, whatever the connector allows: it bounds
   * the stream rows that a task buffers while one request is in flight.
   */
  val MAX_PARAMETER_ROWS_PER_REQUEST = 1000

  private case class OperatorMetric(name: String, description: String, isTiming: Boolean = false)

  // Every description except the standard output row count starts with "external lookup", so the
  // operator's metrics read apart from the connector's custom metrics shown next to them.
  private val OPERATOR_METRICS = Seq(
    OperatorMetric("numOutputRows", "number of output rows"),
    OperatorMetric("numInputRows", "external lookup input rows"),
    OperatorMetric("numInputBatches", "external lookup input batches"),
    OperatorMetric("numLookupRequests", "external lookup requests"),
    OperatorMetric("numLookupParameterRows", "external lookup parameter rows"),
    OperatorMetric("lookupTime", "external lookup time", isTiming = true))

  /** The key of a connector's custom metric, which no operator metric uses. */
  private[sql] def sourceMetricKey(name: String): String = s"source.$name"

  private[sql] def closeResources(
      closeCursor: => Unit,
      updateMetrics: => Unit,
      closeReader: => Unit): Unit = {
    Utils.tryWithSafeFinally(closeCursor) {
      Utils.tryWithSafeFinally(updateMetrics) {
        closeReader
      }
    }
  }
}

/** The operator metrics that the tasks of an [[ExternalLookupJoinExec]] update. */
private[joins] case class ExternalLookupJoinMetrics(
    outputRows: SQLMetric,
    inputRows: SQLMetric,
    inputBatches: SQLMetric,
    requests: SQLMetric,
    parameterRows: SQLMetric,
    lookupTime: SQLMetric)
