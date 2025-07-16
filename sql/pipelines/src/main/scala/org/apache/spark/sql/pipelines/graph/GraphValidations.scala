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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.graph.DataflowGraph.mapUnique
import org.apache.spark.sql.pipelines.util.SchemaInferenceUtils

/** Validations performed on a `DataflowGraph`. */
trait GraphValidations extends Logging {
  this: DataflowGraph =>

  /**
   * Validate multi query table correctness.
   */
  protected[pipelines] def validateMultiQueryTables(): Map[TableIdentifier, Seq[Flow]] = {
    val multiQueryTables = flowsTo.filter(_._2.size > 1)
    // Non-streaming tables do not support multiflow.
    multiQueryTables
      .find {
        case (dest, flows) =>
          flows.exists(f => !resolvedFlow(f.identifier).df.isStreaming) &&
          table.contains(dest)
      }
      .foreach {
        case (dest, flows) =>
          throw new AnalysisException(
            "MATERIALIZED_VIEW_WITH_MULTIPLE_QUERIES",
            Map(
              "tableName" -> dest.unquotedString,
              "queries" -> flows.map(_.identifier).mkString(",")
            )
          )
      }

    multiQueryTables
  }

  /**
   * Validate that each resolved flow is correctly either a streaming flow or non-streaming flow,
   * depending on the flow type (ex. once flow vs non-once flow) and the dataset type the flow
   * writes to (ex. streaming table vs materialized view).
   */
  protected[graph] def validateFlowStreamingness(): Unit = {
    flowsTo.foreach { case (destTableIdentifier, flowsToDataset) =>
      // The identifier should correspond to exactly one of a table or view
      val destTableOpt = table.get(destTableIdentifier)
      val destViewOpt = view.get(destTableIdentifier)

      val resolvedFlowsToDataset: Seq[ResolvedFlow] = flowsToDataset.collect {
        case rf: ResolvedFlow => rf
      }

      resolvedFlowsToDataset.foreach { resolvedFlow: ResolvedFlow =>
        // A flow must be successfully analyzed, thus resolved, in order to determine if it is
        // streaming or not. Unresolved flows will throw an exception anyway via
        // [[validateSuccessfulFlowAnalysis]], so don't check them here.
        if (resolvedFlow.once) {
          // Once flows by definition should be batch flows, not streaming.
          if (resolvedFlow.df.isStreaming) {
            throw new AnalysisException(
              errorClass = "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_ONCE_FLOW",
              messageParameters = Map(
                "flowIdentifier" -> resolvedFlow.identifier.quotedString
              )
            )
          }
        } else {
          destTableOpt.foreach { destTable =>
            if (destTable.isStreamingTable) {
              if (!resolvedFlow.df.isStreaming) {
                throw new AnalysisException(
                  errorClass = "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE",
                  messageParameters = Map(
                    "flowIdentifier" -> resolvedFlow.identifier.quotedString,
                    "tableIdentifier" -> destTableIdentifier.quotedString
                  )
                )
              }
            } else {
              if (resolvedFlow.df.isStreaming) {
                // This check intentionally does NOT prevent materialized views from reading from
                // a streaming table using a _batch_ read, which is still considered valid.
                throw new AnalysisException(
                  errorClass = "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW",
                  messageParameters = Map(
                    "flowIdentifier" -> resolvedFlow.identifier.quotedString,
                    "tableIdentifier" -> destTableIdentifier.quotedString
                  )
                )
              }
            }
          }

          destViewOpt.foreach {
            case _: PersistedView =>
              if (resolvedFlow.df.isStreaming) {
                throw new AnalysisException(
                  errorClass = "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_PERSISTED_VIEW",
                  messageParameters = Map(
                    "flowIdentifier" -> resolvedFlow.identifier.quotedString,
                    "viewIdentifier" -> destTableIdentifier.quotedString
                  )
                )
              }
            case _: TemporaryView =>
              // Temporary views' flows are allowed to be either streaming or batch, so no
              // validation needs to be done for them
          }
        }
      }
    }
  }

  /** Throws an exception if the flows in this graph are not topologically sorted. */
  protected[graph] def validateGraphIsTopologicallySorted(): Unit = {
    val visitedNodes = mutable.Set.empty[TableIdentifier] // Set of visited nodes
    val visitedEdges = mutable.Set.empty[TableIdentifier] // Set of visited edges
    flows.foreach { f =>
      // Unvisited inputs of the current flow
      val unvisitedInputNodes =
        resolvedFlow(f.identifier).inputs -- visitedNodes
      unvisitedInputNodes.headOption match {
        case None =>
          visitedEdges.add(f.identifier)
          if (flowsTo(f.destinationIdentifier).map(_.identifier).forall(visitedEdges.contains)) {
            // A node is marked visited if all its inputs are visited
            visitedNodes.add(f.destinationIdentifier)
          }
        case Some(unvisitedInput) =>
          throw new AnalysisException(
            "PIPELINE_GRAPH_NOT_TOPOLOGICALLY_SORTED",
            Map(
              "flowName" -> f.identifier.unquotedString,
              "inputName" -> unvisitedInput.unquotedString
            )
          )
      }
    }
  }

  /**
   * Validate that all tables are resettable. This is a best-effort check that will only catch
   * upstream tables that are resettable but have a non-resettable downstream dependency.
   */
  protected def validateTablesAreResettable(): Unit = {
    validateTablesAreResettable(tables)
  }

  /** Validate that all specified tables are resettable. */
  protected def validateTablesAreResettable(tables: Seq[Table]): Unit = {
    val tableLookup = mapUnique(tables, "table")(_.identifier)
    val nonResettableTables =
      tables.filter(t => !PipelinesTableProperties.resetAllowed.fromMap(t.properties))
    val upstreamResettableTables = upstreamDatasets(nonResettableTables.map(_.identifier))
      .collect {
        // Filter for upstream datasets that are tables with downstream streaming tables
        case (upstreamDataset, nonResettableDownstreams) if table.contains(upstreamDataset) =>
          nonResettableDownstreams
            .filter(
              t => flowsTo(t).exists(f => resolvedFlow(f.identifier).df.isStreaming)
            )
            .map(id => (tableLookup(upstreamDataset), tableLookup(id).displayName))
      }
      .flatten
      .toSeq
      .filter {
        case (t, _) => PipelinesTableProperties.resetAllowed.fromMap(t.properties)
      } // Filter for resettable

    upstreamResettableTables
      .groupBy(_._2) // Group-by non-resettable downstream tables
      .view
      .mapValues(_.map(_._1))
      .toSeq
      .sortBy(_._2.size) // Output errors from largest to smallest
      .reverse
      .map {
        case (nameForEvent, tables) =>
          throw new AnalysisException(
            "INVALID_RESETTABLE_DEPENDENCY",
            Map(
              "downstreamTable" -> nameForEvent,
              "upstreamResettableTables" -> tables
                .map(_.displayName)
                .sorted
                .map(t => s"'$t'")
                .mkString(", "),
              "resetAllowedKey" -> PipelinesTableProperties.resetAllowed.key
            )
          )
      }
  }

  protected def validateUserSpecifiedSchemas(): Unit = {
    flows.flatMap(f => table.get(f.identifier)).foreach { t: TableInput =>
      // The output inferred schema of a table is the declared schema merged with the
      // schema of all incoming flows. This must be equivalent to the declared schema.
      val inferredSchema = SchemaInferenceUtils
        .inferSchemaFromFlows(
          flowsTo(t.identifier).map(f => resolvedFlow(f.identifier)),
          userSpecifiedSchema = t.specifiedSchema
        )

      t.specifiedSchema.foreach { ss =>
        // Check the inferred schema matches the specified schema. Used to catch errors where the
        // inferred user-facing schema has columns that are not in the specified one.
        if (inferredSchema != ss) {
          val datasetType = GraphElementTypeUtils
            .getDatasetTypeForMaterializedViewOrStreamingTable(
              flowsTo(t.identifier).map(f => resolvedFlow(f.identifier))
            )
          throw GraphErrors.incompatibleUserSpecifiedAndInferredSchemasError(
            t.identifier,
            datasetType,
            ss,
            inferredSchema
          )
        }
      }
    }
  }

  /**
   * Validates that all flows are resolved. If there are unresolved flows,
   * detects a possible cyclic dependency and throw the appropriate exception.
   */
  protected def validateSuccessfulFlowAnalysis(): Unit = {
    // all failed flows with their errors
    val flowAnalysisFailures = resolutionFailedFlows.flatMap(
      f => f.failure.headOption.map(err => (f.identifier, err))
    )
    // only proceed if there are unresolved flows
    if (flowAnalysisFailures.nonEmpty) {
      val failedFlowIdentifiers = flowAnalysisFailures.map(_._1).toSet
      // used to collect the subgraph of only the unresolved flows
      // maps every unresolved flow to the set of unresolved flows writing to one if its inputs
      val failedFlowsSubgraph = mutable.Map[TableIdentifier, Seq[TableIdentifier]]()
      val (downstreamFailures, directFailures) = flowAnalysisFailures.partition {
        case (flowIdentifier, _) =>
          // If a failed flow writes to any of the requested datasets, we mark this flow as a
          // downstream failure
          val failedFlowsWritingToRequestedDatasets =
            resolutionFailedFlow(flowIdentifier).funcResult.requestedInputs
              .flatMap(d => flowsTo.getOrElse(d, Seq()))
              .map(_.identifier)
              .intersect(failedFlowIdentifiers)
              .toSeq
          failedFlowsSubgraph += (flowIdentifier -> failedFlowsWritingToRequestedDatasets)
          failedFlowsWritingToRequestedDatasets.nonEmpty
      }
      // if there are flow that failed due to unresolved upstream flows, check for a cycle
      if (failedFlowsSubgraph.nonEmpty) {
        detectCycle(failedFlowsSubgraph.toMap).foreach {
          case (upstream, downstream) =>
            val upstreamDataset = flow(upstream).destinationIdentifier
            val downstreamDataset = flow(downstream).destinationIdentifier
            throw CircularDependencyException(
              downstreamDataset,
              upstreamDataset
            )
        }
      }
      // otherwise report what flows failed directly vs. depending on a failed flow
      throw UnresolvedPipelineException(
        this,
        directFailures.map { case (id, value) => (id, value) }.toMap,
        downstreamFailures.map { case (id, value) => (id, value) }.toMap
      )
    }
  }

  /**
   * Generic method to detect a cycle in directed graph via DFS traversal.
   * The graph is given as a reverse adjacency map, that is, a map from
   * each node to its ancestors.
   * @return the start and end node of a cycle if found, None otherwise
   */
  private def detectCycle(ancestors: Map[TableIdentifier, Seq[TableIdentifier]])
      : Option[(TableIdentifier, TableIdentifier)] = {
    var cycle: Option[(TableIdentifier, TableIdentifier)] = None
    val visited = mutable.Set[TableIdentifier]()
    def visit(f: TableIdentifier, currentPath: List[TableIdentifier]): Unit = {
      if (cycle.isEmpty && !visited.contains(f)) {
        if (currentPath.contains(f)) {
          cycle = Option((currentPath.head, f))
        } else {
          ancestors(f).foreach(visit(_, f :: currentPath))
          visited += f
        }
      }
    }
    ancestors.keys.foreach(visit(_, Nil))
    cycle
  }

  /** Validates that persisted views don't read from invalid sources */
  protected[graph] def validatePersistedViewSources(): Unit = {
    val viewToFlowMap = ViewHelpers.persistedViewIdentifierToFlow(graph = this)

    persistedViews
      .foreach { persistedView =>
        val flow = viewToFlowMap(persistedView.identifier)
        val funcResult = resolvedFlow(flow.identifier).funcResult
        val inputIdentifiers = (funcResult.batchInputs ++ funcResult.streamingInputs)
          .map(_.input.identifier)

        inputIdentifiers
          .flatMap(view.get)
          .foreach {
            case tempView: TemporaryView =>
              throw new AnalysisException(
                errorClass = "INVALID_TEMP_OBJ_REFERENCE",
                messageParameters = Map(
                  "persistedViewName" -> persistedView.identifier.toString,
                  "temporaryViewName" -> tempView.identifier.toString
                ),
                cause = null
              )
            case _ =>
          }
      }
  }
}
