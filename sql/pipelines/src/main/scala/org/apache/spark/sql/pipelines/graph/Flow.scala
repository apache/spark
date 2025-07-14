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

package org.apache.spark.sql.pipelines.graph

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.pipelines.AnalysisWarning
import org.apache.spark.sql.pipelines.util.InputReadOptions
import org.apache.spark.sql.types.StructType

/**
 * Contains the catalog and database context information for query execution.
 */
case class QueryContext(currentCatalog: Option[String], currentDatabase: Option[String])

/**
 * A [[Flow]] is a node of data transformation in a dataflow graph. It describes the movement
 * of data into a particular dataset.
 */
trait Flow extends GraphElement with Logging {

  /** The [[FlowFunction]] containing the user's query. */
  def func: FlowFunction

  val identifier: TableIdentifier

  /**
   * The dataset that this Flow represents a write to.
   */
  val destinationIdentifier: TableIdentifier

  /**
   * Whether this is a ONCE flow. ONCE flows should run only once per full refresh.
   */
  def once: Boolean = false

  /** The current query context (catalog and database) when the query is defined. */
  def queryContext: QueryContext

  def sqlConf: Map[String, String]
}

/** A wrapper for a resolved internal input that includes the alias provided by the user. */
case class ResolvedInput(input: Input, aliasIdentifier: AliasIdentifier)

/** A wrapper for the lambda function that defines a [[Flow]]. */
trait FlowFunction extends Logging {

  /**
   * This function defines the transformations performed by a flow, expressed as a DataFrame.
   *
   * @param allInputs the set of identifiers for all the [[Input]]s defined in the
   *                  [[DataflowGraph]].
   * @param availableInputs the list of all [[Input]]s available to this flow
   * @param configuration the spark configurations that apply to this flow.
   * @param queryContext The context of the query being evaluated.
   * @return the inputs actually used, and the DataFrame expression for the flow
   */
  def call(
      allInputs: Set[TableIdentifier],
      availableInputs: Seq[Input],
      configuration: Map[String, String],
      queryContext: QueryContext
  ): FlowFunctionResult
}

/**
 * Holds the DataFrame returned by a [[FlowFunction]] along with the inputs used to
 * construct it.
 * @param batchInputs the complete inputs read by the flow
 * @param streamingInputs the incremental inputs read by the flow
 * @param usedExternalInputs the identifiers of the external inputs read by the flow
 * @param dataFrame the DataFrame expression executed by the flow if the flow can be resolved
 */
case class FlowFunctionResult(
    requestedInputs: Set[TableIdentifier],
    batchInputs: Set[ResolvedInput],
    streamingInputs: Set[ResolvedInput],
    usedExternalInputs: Set[TableIdentifier],
    dataFrame: Try[DataFrame],
    sqlConf: Map[String, String],
    analysisWarnings: Seq[AnalysisWarning] = Nil) {

  /**
   * Returns the names of all of the [[Input]]s used when resolving this [[Flow]]. If the
   * flow failed to resolve, we return the all the datasets that were requested when evaluating the
   * flow.
   */
  def inputs: Set[TableIdentifier] = {
    (batchInputs ++ streamingInputs).map(_.input.identifier)
  }

  /** Returns errors that occurred when attempting to analyze this [[Flow]]. */
  def failure: Seq[Throwable] = {
    dataFrame.failed.toOption.toSeq
  }

  /** Whether this [[Flow]] is successfully analyzed. */
  final def resolved: Boolean = failure.isEmpty // don't override this, override failure
}

/** A [[Flow]] whose output schema and dependencies aren't known. */
case class UnresolvedFlow(
    identifier: TableIdentifier,
    destinationIdentifier: TableIdentifier,
    func: FlowFunction,
    queryContext: QueryContext,
    sqlConf: Map[String, String],
    override val once: Boolean,
    override val origin: QueryOrigin
) extends Flow

/**
 * A [[Flow]] whose flow function has been invoked, meaning either:
 *  - Its output schema and dependencies are known.
 *  - It failed to resolve.
 */
trait ResolutionCompletedFlow extends Flow {
  def flow: UnresolvedFlow
  def funcResult: FlowFunctionResult

  val identifier: TableIdentifier = flow.identifier
  val destinationIdentifier: TableIdentifier = flow.destinationIdentifier
  def func: FlowFunction = flow.func
  def queryContext: QueryContext = flow.queryContext
  def sqlConf: Map[String, String] = funcResult.sqlConf
  def origin: QueryOrigin = flow.origin
}

/** A [[Flow]] whose flow function has failed to resolve. */
class ResolutionFailedFlow(val flow: UnresolvedFlow, val funcResult: FlowFunctionResult)
    extends ResolutionCompletedFlow {
  assert(!funcResult.resolved)

  def failure: Seq[Throwable] = funcResult.failure
}

/** A [[Flow]] whose flow function has successfully resolved. */
trait ResolvedFlow extends ResolutionCompletedFlow with Input {
  assert(funcResult.resolved)

  /** The logical plan for this flow's query. */
  def df: DataFrame = funcResult.dataFrame.get

  /** Returns the schema of the output of this [[Flow]]. */
  def schema: StructType = df.schema
  override def load(readOptions: InputReadOptions): DataFrame = df
  def inputs: Set[TableIdentifier] = funcResult.inputs
}

/** A [[Flow]] that represents stateful movement of data to some target. */
class StreamingFlow(
    val flow: UnresolvedFlow,
    val funcResult: FlowFunctionResult,
    val mustBeAppend: Boolean = false
) extends ResolvedFlow {}

/** A [[Flow]] that declares exactly what data should be in the target table. */
class CompleteFlow(
    val flow: UnresolvedFlow,
    val funcResult: FlowFunctionResult,
    val mustBeAppend: Boolean = false
) extends ResolvedFlow {}

/** A [[Flow]] that reads source[s] completely and appends data to the target, just once.
 */
class AppendOnceFlow(
    val flow: UnresolvedFlow,
    val funcResult: FlowFunctionResult
) extends ResolvedFlow {

  override val once = true
}
