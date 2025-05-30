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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{CTESubstitution, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.classic.{DataFrame, Dataset, DataStreamReader, SparkSession}
import org.apache.spark.sql.pipelines.AnalysisWarning
import org.apache.spark.sql.pipelines.graph.GraphIdentifierManager.{ExternalDatasetIdentifier, InternalDatasetIdentifier}
import org.apache.spark.sql.pipelines.util.{BatchReadOptions, InputReadOptions, StreamingReadOptions}


object FlowAnalysis {
    /**
     * Creates a [[FlowFunction]] that attempts to analyze the provided LogicalPlan
     * using the existing resolved inputs.
     * - If all upstream inputs have been resolved, then analysis succeeds and the
     *   function returns a [[FlowFunctionResult]] containing the dataframe.
     * - If any upstream inputs are unresolved, then the function throws an exception.
     *
     * @param plan The user-supplied LogicalPlan defining a flow.
     * @return A FlowFunction that attempts to analyze the provided LogicalPlan.
     */
  def createFlowFunctionFromLogicalPlan(plan: LogicalPlan): FlowFunction = {
    new FlowFunction {
      override def call(
          allInputs: Set[TableIdentifier],
          availableInputs: Seq[Input],
          confs: Map[String, String],
          queryContext: QueryContext
      ): FlowFunctionResult = {
        val ctx = FlowAnalysisContext(
          allInputs = allInputs,
          availableInputs = availableInputs,
          queryContext = queryContext,
          spark = SparkSession.active
        )
        val df = try {
          confs.foreach { case (k, v) => ctx.setConf(k, v) }
          Try(FlowAnalysis.analyze(ctx, plan))
        } finally {
          ctx.restoreOriginalConf()
        }
        FlowFunctionResult(
          requestedInputs = ctx.requestedInputs.toSet,
          batchInputs = ctx.batchInputs.toSet,
          streamingInputs = ctx.streamingInputs.toSet,
          usedExternalInputs = ctx.externalInputs.toSet,
          dataFrame = df,
          sqlConf = confs,
          analysisWarnings = ctx.analysisWarnings.toList
        )
      }
    }
  }

  /**
   * Constructs an analyzed [[DataFrame]] from a [[LogicalPlan]] by resolving Pipelines specific
   * TVFs and datasets that cannot be resolved directly by Catalyst.
   *
   * This function shouldn't call any singleton as it will break concurrent access to graph
   * analysis; or any thread local variables as graph analysis and this function will use
   * different threads in python repl.
   *
   * @param plan     The [[LogicalPlan]] defining a flow.
   * @return An analyzed [[DataFrame]].
   */
  private def analyze(
      context: FlowAnalysisContext,
      plan: LogicalPlan
  ): DataFrame = {
    // Users can define CTEs within their CREATE statements. For example,
    //
    // CREATE STREAMING TABLE a
    // WITH b AS (
    //    SELECT * FROM STREAM upstream
    // )
    // SELECT * FROM b
    //
    // The relation defined using the WITH keyword is not included in the children of the main
    // plan so the specific analysis we do below will not be applied to those relations.
    // Instead, we call an analyzer rule to inline all of the CTE relations in the main plan before
    // we do analysis. This rule would be called during analysis anyways, but we just call it
    // earlier so we only need to apply analysis to a single logical plan.
    val planWithInlinedCTEs = CTESubstitution(plan)

    val spark = context.spark
    // Traverse the user's query plan and recursively resolve nodes that reference Pipelines
    // features that the Spark analyzer is unable to resolve
    val resolvedPlan = planWithInlinedCTEs transformWithSubqueries {
        // Streaming read on another dataset
        // This branch will be hit for the following kinds of queries:
        // - SELECT ... FROM STREAM(t1)
        // - SELECT ... FROM STREAM t1
        case u: UnresolvedRelation if u.isStreaming =>
          readStreamInput(
            context,
            name = IdentifierHelper.toQuotedString(u.multipartIdentifier),
            spark.readStream,
            streamingReadOptions = StreamingReadOptions()
          ).queryExecution.analyzed

        // Batch read on another dataset in the pipeline
        case u: UnresolvedRelation =>
          readBatchInput(
            context,
            name = IdentifierHelper.toQuotedString(u.multipartIdentifier),
            batchReadOptions = BatchReadOptions()
          ).queryExecution.analyzed
      }
    Dataset.ofRows(spark, resolvedPlan)

  }

  /**
   * Internal helper to reference the batch dataset (i.e., non-streaming dataset) with the given
   * name.
   * 1. The dataset can be a table, view, or a named flow.
   * 2. The dataset can be a dataset defined in the same DataflowGraph or a table in the external
   * catalog.
   * All the public APIs that read from a dataset should call this function to read the dataset.
   *
   * @param name the name of the Dataset to be read.
   * @param batchReadOptions Options for this batch read
   * @return batch DataFrame that represents data from the specified Dataset.
   */
  final private def readBatchInput(
      context: FlowAnalysisContext,
      name: String,
      batchReadOptions: BatchReadOptions
  ): DataFrame = {
    GraphIdentifierManager.parseAndQualifyInputIdentifier(context, name) match {
      case inputIdentifier: InternalDatasetIdentifier =>
        readGraphInput(context, inputIdentifier, batchReadOptions)

      case inputIdentifier: ExternalDatasetIdentifier =>
        readExternalBatchInput(
          context,
          inputIdentifier = inputIdentifier,
          name = name
        )
    }
  }

  /**
   * Internal helper to reference the streaming dataset with the given name.
   * 1. The dataset can be a table, view, or a named flow.
   * 2. The dataset can be a dataset defined in the same DataflowGraph or a table in the external
   * catalog.
   * All the public APIs that read from a dataset should call this function to read the dataset.
   *
   * @param name the name of the Dataset to be read.
   * @param streamReader The [[DataStreamReader]] that may hold read options specified by the user.
   * @param streamingReadOptions Options for this streaming read.
   * @return streaming DataFrame that represents data from the specified Dataset.
   */
  final private def readStreamInput(
      context: FlowAnalysisContext,
      name: String,
      streamReader: DataStreamReader,
      streamingReadOptions: StreamingReadOptions
  ): DataFrame = {
    GraphIdentifierManager.parseAndQualifyInputIdentifier(context, name) match {
      case inputIdentifier: InternalDatasetIdentifier =>
        readGraphInput(
          context,
          inputIdentifier,
          streamingReadOptions
        )

      case inputIdentifier: ExternalDatasetIdentifier =>
        readExternalStreamInput(
          context,
          inputIdentifier = inputIdentifier,
          streamReader = streamReader,
          name = name
        )
    }
  }

  /**
   * Internal helper to reference dataset defined in the same [[DataflowGraph]].
   *
   * @param inputIdentifier The identifier of the Dataset to be read.
   * @param readOptions Options for this read (may be either streaming or batch options)
   * @return streaming or batch DataFrame that represents data from the specified Dataset.
   */
  final private def readGraphInput(
      ctx: FlowAnalysisContext,
      inputIdentifier: InternalDatasetIdentifier,
      readOptions: InputReadOptions
  ): DataFrame = {
    val datasetIdentifier = inputIdentifier.identifier

    ctx.requestedInputs += datasetIdentifier

    val i = if (!ctx.allInputs.contains(datasetIdentifier)) {
      // Dataset not defined in the dataflow graph
      throw GraphErrors.pipelineLocalDatasetNotDefinedError(datasetIdentifier.unquotedString)
    } else if (!ctx.availableInput.contains(datasetIdentifier)) {
      // Dataset defined in the dataflow graph but not yet resolved
      throw UnresolvedDatasetException(datasetIdentifier)
    } else {
      // Dataset is resolved, so we can read from it
      ctx.availableInput(datasetIdentifier)
    }

    val inputDF = i.load(readOptions)
    i match {
      // If the referenced input is a [[Flow]], because the query plans will be fused
      // together, we also need to fuse their confs.
      case f: Flow => f.sqlConf.foreach { case (k, v) => ctx.setConf(k, v) }
      case _ =>
    }

    val incompatibleViewReadCheck =
      ctx.spark.conf.get("pipelines.incompatibleViewCheck.enabled", "true").toBoolean

    // Wrap the DF in an alias so that columns in the DF can be referenced with
    // the following in the query:
    // - <catalog>.<schema>.<dataset>.<column>
    // - <schema>.<dataset>.<column>
    // - <dataset>.<column>
    val aliasIdentifier = AliasIdentifier(
      name = datasetIdentifier.table,
      qualifier = Seq(datasetIdentifier.catalog, datasetIdentifier.database).flatten
    )

    readOptions match {
      case sro: StreamingReadOptions =>
        if (!inputDF.isStreaming && incompatibleViewReadCheck) {
          throw new AnalysisException(
            "INCOMPATIBLE_BATCH_VIEW_READ",
            Map("datasetIdentifier" -> datasetIdentifier.toString)
          )
        }

        if (sro.droppedUserOptions.nonEmpty) {
          ctx.analysisWarnings += AnalysisWarning.StreamingReaderOptionsDropped(
            sourceName = datasetIdentifier.unquotedString,
            droppedOptions = sro.droppedUserOptions.keys.toSeq
          )
        }
        ctx.streamingInputs += ResolvedInput(i, aliasIdentifier)
      case _ =>
        if (inputDF.isStreaming && incompatibleViewReadCheck) {
          throw new AnalysisException(
            "INCOMPATIBLE_STREAMING_VIEW_READ",
            Map("datasetIdentifier" -> datasetIdentifier.toString)
          )
        }
        ctx.batchInputs += ResolvedInput(i, aliasIdentifier)
    }
    Dataset.ofRows(
      ctx.spark,
      SubqueryAlias(identifier = aliasIdentifier, child = inputDF.queryExecution.logical)
    )
  }

  /**
   * Internal helper to reference batch dataset (i.e., non-streaming dataset) defined in an external
   * catalog or as a path.
   *
   * @param inputIdentifier The identifier of the dataset to be read.
   * @return streaming or batch DataFrame that represents data from the specified Dataset.
   */
  final private def readExternalBatchInput(
      context: FlowAnalysisContext,
      inputIdentifier: ExternalDatasetIdentifier,
      name: String): DataFrame = {

    val spark = context.spark
    context.externalInputs += inputIdentifier.identifier
    spark.read.table(inputIdentifier.identifier.quotedString)
  }

  /**
   * Internal helper to reference dataset defined in an external catalog or as a path.
   *
   * @param inputIdentifier The identifier of the dataset to be read.
   * @param streamReader The [[DataStreamReader]] that may hold additional read options specified by
   *                     the user.
   * @return streaming or batch DataFrame that represents data from the specified Dataset.
   */
  final private def readExternalStreamInput(
      context: FlowAnalysisContext,
      inputIdentifier: ExternalDatasetIdentifier,
      streamReader: DataStreamReader,
      name: String): DataFrame = {

    context.externalInputs += inputIdentifier.identifier
    streamReader.table(inputIdentifier.identifier.quotedString)
  }
}
