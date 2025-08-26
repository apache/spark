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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.graph.DataflowGraph.mapUnique
import org.apache.spark.sql.pipelines.util.SchemaMergingUtils
import org.apache.spark.sql.types.StructType

/**
 * DataflowGraph represents the core graph structure for Spark declarative pipelines.
 * It manages the relationships between logical flows, tables, and views, providing
 * operations for graph traversal, validation, and transformation.
 */
case class DataflowGraph(flows: Seq[Flow], tables: Seq[Table], views: Seq[View])
    extends GraphOperations
    with GraphValidations {

  /** Map of [[Output]]s by their identifiers */
  lazy val output: Map[TableIdentifier, Output] = mapUnique(tables, "output")(_.identifier)

  /**
   * [[Flow]]s in this graph that need to get planned and potentially executed when
   * executing the graph. Flows that write to logical views are excluded.
   */
  lazy val materializedFlows: Seq[ResolvedFlow] = {
    resolvedFlows.filter(
      f => output.contains(f.destinationIdentifier)
    )
  }

  /** The identifiers of [[materializedFlows]]. */
  val materializedFlowIdentifiers: Set[TableIdentifier] = materializedFlows.map(_.identifier).toSet

  /** Map of [[Table]]s by their identifiers */
  lazy val table: Map[TableIdentifier, Table] =
    mapUnique(tables, "table")(_.identifier)

  /** Map of [[Flow]]s by their identifier */
  lazy val flow: Map[TableIdentifier, Flow] = {
    // Better error message than using mapUnique.
    val flowsByIdentifier = flows.groupBy(_.identifier)
    flowsByIdentifier
      .find(_._2.size > 1)
      .foreach {
        case (flowIdentifier, flows) =>
          // We don't expect this to ever actually be hit, graph registration should validate for
          // unique flow names.
          throw new AnalysisException(
            errorClass = "PIPELINE_DUPLICATE_IDENTIFIERS.FLOW",
            messageParameters = Map(
              "flowName" -> flowIdentifier.unquotedString,
              "datasetNames" -> flows.map(_.destinationIdentifier).mkString(",")
            )
          )
      }
    // Flows with non-default names shouldn't conflict with table names
    flows
      .filterNot(f => f.identifier == f.destinationIdentifier)
      .filter(f => table.contains(f.identifier))
      .foreach { f =>
        throw new AnalysisException(
          "FLOW_NAME_CONFLICTS_WITH_TABLE",
          Map(
            "flowName" -> f.identifier.toString(),
            "target" -> f.destinationIdentifier.toString(),
            "tableName" -> f.identifier.toString()
          )
        )
      }
    flowsByIdentifier.view.mapValues(_.head).toMap
  }

  /** Map of [[View]]s by their identifiers */
  lazy val view: Map[TableIdentifier, View] = mapUnique(views, "view")(_.identifier)

  /** The [[PersistedView]]s of the graph */
  lazy val persistedViews: Seq[PersistedView] = views.collect {
    case v: PersistedView => v
  }

  /** All the [[Input]]s in the current DataflowGraph. */
  lazy val inputIdentifiers: Set[TableIdentifier] = {
    (flows ++ tables).map(_.identifier).toSet
  }

  /** The [[Flow]]s that write to a given destination. */
  lazy val flowsTo: Map[TableIdentifier, Seq[Flow]] = flows.groupBy(_.destinationIdentifier)

  lazy val resolvedFlows: Seq[ResolvedFlow] = {
    flows.collect { case f: ResolvedFlow => f }
  }

  lazy val resolvedFlow: Map[TableIdentifier, ResolvedFlow] = {
    resolvedFlows.map { f =>
      f.identifier -> f
    }.toMap
  }

  lazy val resolutionFailedFlows: Seq[ResolutionFailedFlow] = {
    flows.collect { case f: ResolutionFailedFlow => f }
  }

  lazy val resolutionFailedFlow: Map[TableIdentifier, ResolutionFailedFlow] = {
    resolutionFailedFlows.map { f =>
      f.identifier -> f
    }.toMap
  }

  /**
   * Used to reanalyze the flow's DF for a given table. This is done by finding all upstream
   * flows (until a table is reached) for the specified source and reanalyzing all upstream
   * flows.
   *
   * @param srcFlow The flow that writes into the table that we will start from when finding
   *                upstream flows
   * @return The reanalyzed flow
   */
  protected[graph] def reanalyzeFlow(srcFlow: Flow): ResolvedFlow = {
    val upstreamDatasetIdentifiers = dfsInternal(
      flowNodes(srcFlow.identifier).output,
      downstream = false,
      stopAtMaterializationPoints = true
    )
    val upstreamFlows =
      resolvedFlows
        .filter(f => upstreamDatasetIdentifiers.contains(f.destinationIdentifier))
        .map(_.flow)
    val upstreamViews = upstreamDatasetIdentifiers.flatMap(identifier => view.get(identifier)).toSeq

    val subgraph = new DataflowGraph(
      flows = upstreamFlows,
      views = upstreamViews,
      tables = Seq(table(srcFlow.destinationIdentifier))
    )
    subgraph.resolve().resolvedFlow(srcFlow.identifier)
  }

  /**
   * A map of the inferred schema of each table, computed by merging the analyzed schemas
   * of all flows writing to that table.
   */
  lazy val inferredSchema: Map[TableIdentifier, StructType] = {
    flowsTo.view.mapValues { flows =>
      flows
        .map { flow =>
          resolvedFlow(flow.identifier).schema
        }
        .reduce(SchemaMergingUtils.mergeSchemas)
    }.toMap
  }

  /** Ensure that the [[DataflowGraph]] is valid and throws errors if not. */
  def validate(): DataflowGraph = {
    validationFailure.toOption match {
      case Some(exception) => throw exception
      case None => this
    }
  }

  /**
   * Validate the current [[DataflowGraph]] and cache the validation failure.
   *
   * To add more validations, add them in a helper function that throws an exception if the
   * validation fails, and invoke the helper function here.
   */
  private lazy val validationFailure: Try[Throwable] = Try {
    validateSuccessfulFlowAnalysis()
    validateUserSpecifiedSchemas()
    // Connecting the graph sorts it topologically
    validateGraphIsTopologicallySorted()
    validateMultiQueryTables()
    validatePersistedViewSources()
    validateEveryDatasetHasFlow()
    validateTablesAreResettable()
    validateFlowStreamingness()
    inferredSchema
  }.failed

  /**
   * Enforce every dataset has at least one input flow. For example its possible to define
   * streaming tables without a query; such tables should still have at least one flow
   * writing to it.
   */
  private def validateEveryDatasetHasFlow(): Unit = {
    (tables.map(_.identifier) ++ views.map(_.identifier)).foreach { identifier =>
      if (!flows.exists(_.destinationIdentifier == identifier)) {
        throw new AnalysisException(
          "PIPELINE_DATASET_WITHOUT_FLOW",
          Map("identifier" -> identifier.quotedString)
        )
      }
    }
  }

  /** Returns true iff all [[Flow]]s are successfully analyzed. */
  def resolved: Boolean =
    flows.forall(f => resolvedFlow.contains(f.identifier))

  def resolve(): DataflowGraph =
    DataflowGraphTransformer.withDataflowGraphTransformer(this) { transformer =>
      val coreDataflowNodeProcessor =
        new CoreDataflowNodeProcessor(rawGraph = this)
      transformer
        .transformDownNodes(coreDataflowNodeProcessor.processNode)
        .getDataflowGraph
    }
}

object DataflowGraph {
  protected[graph] def mapUnique[K, A](input: Seq[A], tpe: String)(f: A => K): Map[K, A] = {
    val grouped = input.groupBy(f)
    grouped.filter(_._2.length > 1).foreach {
      case (name, _) =>
        throw new AnalysisException(
          errorClass = "DUPLICATE_GRAPH_ELEMENT",
          messageParameters = Map("graphElementType" -> tpe, "graphElementName" -> name.toString)
        )
    }
    grouped.view.mapValues(_.head).toMap
  }
}
