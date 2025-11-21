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

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{CTESubstitution, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.classic.{DataFrame, DataFrameReader, Dataset, DataStreamReader, SparkSession}
import org.apache.spark.sql.pipelines.graph.GraphIdentifierManager.{ExternalDatasetIdentifier, InternalDatasetIdentifier}


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
          queryContext: QueryContext,
          queryOrigin: QueryOrigin
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
          sqlConf = confs
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
          val resolved = readStreamInput(
            context,
            name = IdentifierHelper.toQuotedString(u.multipartIdentifier),
            streamReader = spark.readStream.options(u.options)
          ).queryExecution.analyzed
          // Spark Connect requires the PLAN_ID_TAG to be propagated to the resolved plan
          // to allow correct analysis of the parent plan that contains this subquery
          resolved.mergeTagsFrom(u)
          resolved
        // Batch read on another dataset in the pipeline
        case u: UnresolvedRelation =>
          val resolved = readBatchInput(
            context,
            name = IdentifierHelper.toQuotedString(u.multipartIdentifier),
            batchReader = spark.read.options(u.options)
          ).queryExecution.analyzed
          // Spark Connect requires the PLAN_ID_TAG to be propagated to the resolved plan
          // to allow correct analysis of the parent plan that contains this subquery
          resolved.mergeTagsFrom(u)
          resolved
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
   * @param batchReader the batch dataframe reader, possibly with options, to execute the read
   *                    with.
   * @return batch DataFrame that represents data from the specified Dataset.
   */
  final private def readBatchInput(
      context: FlowAnalysisContext,
      name: String,
      batchReader: DataFrameReader
  ): DataFrame = {
    GraphIdentifierManager.parseAndQualifyInputIdentifier(context, name) match {
      case inputIdentifier: InternalDatasetIdentifier =>
        readGraphInput(context, inputIdentifier, isStreamingRead = false)

      case inputIdentifier: ExternalDatasetIdentifier =>
        readExternalBatchInput(
          context,
          inputIdentifier = inputIdentifier,
          name = name,
          batchReader = batchReader
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
   * @return streaming DataFrame that represents data from the specified Dataset.
   */
  final private def readStreamInput(
      context: FlowAnalysisContext,
      name: String,
      streamReader: DataStreamReader
  ): DataFrame = {
    GraphIdentifierManager.parseAndQualifyInputIdentifier(context, name) match {
      case inputIdentifier: InternalDatasetIdentifier =>
        readGraphInput(
          context,
          inputIdentifier,
          isStreamingRead = true
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
   * @param isStreamingRead Whether this is a streaming read or batch read.
   * @return streaming or batch DataFrame that represents data from the specified Dataset.
   */
  final private def readGraphInput(
      ctx: FlowAnalysisContext,
      inputIdentifier: InternalDatasetIdentifier,
      isStreamingRead: Boolean
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

    val inputDF = Try {
      i.load(asStreaming = isStreamingRead)
    } match {
      case Success(df) => df
      case Failure(ex: AnalysisException) => ex.errorClass match {
        // Views are simply resolved as the flows they read from during graph construction, so we
        // know a flow load exception here directly corresponds to a reading a view specifically.
        // Rethrow relevant exceptions appropriately, with the view's identifier.
        case Some("INCOMPATIBLE_FLOW_READ.BATCH_READ_ON_STREAMING_FLOW") =>
          throw new AnalysisException(
            "INCOMPATIBLE_BATCH_VIEW_READ",
            Map("datasetIdentifier" -> datasetIdentifier.toString)
          )
        case Some("INCOMPATIBLE_FLOW_READ.STREAMING_READ_ON_BATCH_FLOW") =>
          throw new AnalysisException(
            "INCOMPATIBLE_STREAMING_VIEW_READ",
            Map("datasetIdentifier" -> datasetIdentifier.toString)
          )
        case _ =>
          throw ex
      }
      case Failure(ex: Throwable) =>
        throw ex
    }

    i match {
      // If the referenced input is a [[Flow]], because the query plans will be fused
      // together, we also need to fuse their confs.
      case f: Flow => f.sqlConf.foreach { case (k, v) => ctx.setConf(k, v) }
      case _ =>
    }

    // Wrap the DF in an alias so that columns in the DF can be referenced with
    // the following in the query:
    // - <catalog>.<schema>.<dataset>.<column>
    // - <schema>.<dataset>.<column>
    // - <dataset>.<column>
    val aliasIdentifier = AliasIdentifier(
      name = datasetIdentifier.table,
      qualifier = Seq(datasetIdentifier.catalog, datasetIdentifier.database).flatten
    )

    if (isStreamingRead) {
      ctx.streamingInputs += ResolvedInput(i, aliasIdentifier)
    } else {
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
      name: String,
      batchReader: DataFrameReader): DataFrame = {

    context.externalInputs += inputIdentifier.identifier
    batchReader.table(inputIdentifier.identifier.quotedString)
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
