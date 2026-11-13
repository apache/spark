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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeSeq,
  AttributeSet, BoundReference, Expression, ExprUtils, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti, LeftOuter, LeftSemi, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupFilter, ExternalLookupJoinShapes,
  ExternalLookupProject, ExternalLookupTransform, LookupKeyComparison}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{ExternalLookupReaderFactory, Scan}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.SupportsCustomDriverMetrics
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Executes synchronous, parameterized external lookup joins without a shuffle. Each task joins
 * its input rows in batches, through an [[ExternalLookupJoinEvaluatorFactory]].
 */
case class ExternalLookupJoinExec(
    tableName: String,
    lookupParameters: Seq[Expression],
    lookupInput: Seq[Attribute],
    lookupOutput: Seq[Attribute],
    lookupTransforms: Seq[ExternalLookupTransform],
    lookupKeys: Seq[Attribute],
    lookupKeyComparisons: Seq[LookupKeyComparison],
    joinType: JoinType,
    residualCondition: Option[Expression],
    @transient scan: Scan,
    readerFactory: ExternalLookupReaderFactory,
    maxParameterRowsPerRequest: Int,
    child: SparkPlan) extends UnaryExecNode with SupportsCustomDriverMetrics with PredicateHelper {

  // The connector's value rows follow the read schema, which `lookupInput` describes, and the
  // lookup-side stages turn them into rows of `transformedLookup`. The lookup columns keep their
  // analyzed order, and take the types and nullability of those rows. In `lookupInput`, the
  // nullability at every nested level combines the table's and the read schema's: a field is NOT
  // NULL when either says so.
  private lazy val transformedLookup = lookupTransforms.lastOption.map(_.output)
    .getOrElse(lookupInput)
  private lazy val inputByLookupOutput =
    AttributeMap(transformedLookup.map(attr => attr -> attr))
  private lazy val lookupColumns = lookupOutput.map(inputByLookupOutput)

  override def output: Seq[Attribute] = joinType match {
    case LeftOuter => child.output ++ lookupColumns.map(_.withNullability(true))
    case LeftSemi | LeftAnti => child.output
    case _ => child.output ++ lookupColumns
  }

  // The lookup-side attributes are not in the child's output, so the default canonicalization
  // gives each top-level one the next id, whichever column it is: `output` could then not find a
  // lookup column in the read schema, and nodes that keep different lookup columns of one type
  // would canonicalize alike. Each is normalized by its place in the child's output, the read
  // schema and the outputs of the lookup-side stages instead.
  override protected def doCanonicalize(): SparkPlan = {
    val attributes: AttributeSeq =
      child.output ++ ExternalLookupTransform.attributes(lookupInput, lookupTransforms)
    def normalize[T <: Expression](expression: T): T =
      QueryPlan.normalizeExpressions(expression, attributes)
    copy(
      lookupParameters = lookupParameters.map(normalize),
      lookupInput = lookupInput.map(normalize),
      lookupOutput = lookupOutput.map(normalize),
      lookupTransforms = lookupTransforms.map(ExternalLookupTransform.canonicalized(_, attributes)),
      lookupKeys = lookupKeys.map(normalize),
      residualCondition = residualCondition.map(normalize),
      child = child.canonicalized)
  }

  // Each task joins its input rows where they are, so the stream side's partitioning holds.
  override def outputPartitioning: Partitioning = child.outputPartitioning

  // The conjuncts of the residual condition that read only stream columns, and the rest. A stream
  // row that fails the first cannot match, so the join checks them on each stream row before the
  // lookup, and does not request the row's key. The join then evaluates them for rows without a
  // match too, and before the conjuncts written ahead of them, so only conjuncts that can never
  // fail are checked early: those that ExprUtils.canEvaluateUnconditionally accepts, as for the
  // streamed side of other joins. `Expression.throwable` is not enough, because most expressions
  // that can fail, such as an ANSI cast, do not report it. A conjunct that reads no column stays
  // with the rest: the optimizer has already folded it if it could.
  private def splitResidualCondition: (Option[Expression], Option[Expression]) = {
    val (streamConjuncts, joinedConjuncts) =
      residualCondition.toSeq.flatMap(splitConjunctivePredicates).partition { conjunct =>
        conjunct.references.nonEmpty && conjunct.references.subsetOf(child.outputSet) &&
          ExprUtils.canEvaluateUnconditionally(conjunct)
      }
    (streamConjuncts.reduceOption(And), joinedConjuncts.reduceOption(And))
  }

  // Spark does not trust the connector to evaluate the selected lookup predicates as Spark does,
  // so it checks them itself on each row that the connector returns. The check reads that row
  // after the parameter row of its request, and compares lookup key i with parameter i as its
  // selected predicate does.
  private def lookupKeyCheck: Option[Expression] = lookupParameters.indices.map { index =>
    val parameter = lookupParameters(index)
    val parameterValue = BoundReference(index, parameter.dataType, parameter.nullable)
    val keyOrdinal = lookupInput.indexWhere(_.exprId == lookupKeys(index).exprId)
    val key = lookupInput(keyOrdinal)
    val keyValue = BoundReference(lookupParameters.length + keyOrdinal, key.dataType, key.nullable)
    lookupKeyComparisons(index)(keyValue, parameterValue)
  }.reduceOption(And)

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
    val (streamCondition, joinedCondition) = splitResidualCondition
    val evaluatorFactory = new ExternalLookupJoinEvaluatorFactory(
      tableName,
      lookupParameters,
      lookupKeyComparisons.map(_.nullSafe),
      lookupKeyCheck,
      child.output,
      lookupInput,
      lookupTransforms,
      transformedLookup,
      output,
      joinType,
      streamCondition,
      joinedCondition.map(_.transform {
        case attr: Attribute => inputByLookupOutput.getOrElse(attr, attr)
      }),
      readerFactory,
      math.min(ExternalLookupJoinExec.MAX_PARAMETER_ROWS_PER_REQUEST, maxParameterRowsPerRequest),
      ExternalLookupJoinShapes.oneParameterRowPerRequest(lookupParameters, lookupKeyComparisons),
      conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_MAX_BATCH_BYTES),
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

  // As for other joins, the join type, the selected lookup predicates, each a lookup column
  // compared with its parameter, the lookup-side stages and the residual condition, then the
  // connector's description, which may hold a secret and is redacted.
  override def simpleString(maxFields: Int): String = {
    val selectedPredicates = lookupParameters.indices.map { index =>
      lookupKeyComparisons(index)(lookupKeys(index), lookupParameters(index))
    }
    val predicates = truncatedString(selectedPredicates, "[", ", ", "]", maxFields)
    val stages = if (lookupTransforms.isEmpty) "" else {
      val described = lookupTransforms.map {
        case ExternalLookupProject(projectList, _) =>
          s"Project ${truncatedString(projectList, "[", ", ", "]", maxFields)}"
        case ExternalLookupFilter(condition, _) => s"Filter $condition"
      }
      s", ${truncatedString(described, "[", ", ", "]", maxFields)}"
    }
    val residual = residualCondition.map(condition => s", $condition").getOrElse("")
    Utils.redact(conf.stringRedactionPattern,
      s"$nodeName $joinType, $predicates$stages$residual, ${scan.description()}")
  }

  override protected def stringArgs: Iterator[Any] =
    Iterator(child, simpleString(conf.maxToStringFields))
}

object ExternalLookupJoinExec {
  /**
   * Spark's cap on the stream rows of one request, whatever the connector allows, and so on its
   * parameter rows. It bounds the stream rows that a task buffers while one request is in flight,
   * also when they share one parameter row, as the rows of a range request do.
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
