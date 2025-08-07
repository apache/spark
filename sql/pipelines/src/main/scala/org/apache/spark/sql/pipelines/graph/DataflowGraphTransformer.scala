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

import java.util.concurrent.{
  ConcurrentHashMap,
  ConcurrentLinkedDeque,
  ConcurrentLinkedQueue,
  ExecutionException,
  Future
}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.util.ThreadUtils

/**
 * Resolves the [[DataflowGraph]] by processing each node in the graph. This class exposes visitor
 * functionality to resolve/analyze graph nodes.
 * We only expose simple visitor abilities to transform different entities of the
 * graph.
 * For advanced transformations we also expose a mechanism to walk the graph over entity by entity.
 *
 * Assumptions:
 * 1. Each output will have at-least 1 flow to it.
 * 2. Each flow may or may not have a destination table. If a flow does not have a destination
 *    table, the destination is a temporary view.
 *
 * The way graph is structured is that flows, tables and sinks all are graph elements or nodes.
 * While we expose transformation functions for each of these entities, we also expose a way to
 * process to walk over the graph.
 *
 * Constructor is private as all usages should be via
 * DataflowGraphTransformer.withDataflowGraphTransformer.
 * @param graph: Any Dataflow Graph
 */
class DataflowGraphTransformer(graph: DataflowGraph) extends AutoCloseable {
  import DataflowGraphTransformer._

  private var tables: Seq[Table] = graph.tables
  private var tableMap: Map[TableIdentifier, Table] = computeTableMap()
  private var flows: Seq[Flow] = graph.flows
  private var flowsTo: Map[TableIdentifier, Seq[Flow]] = computeFlowsTo()
  private var views: Seq[View] = graph.views
  private var viewMap: Map[TableIdentifier, View] = computeViewMap()

  // Fail analysis nodes
  // Failed flows are flows that are failed to resolve or its inputs are not available or its
  // destination failed to resolve.
  private var failedFlows: Seq[ResolutionCompletedFlow] = Seq.empty
  // We define a dataset is failed to resolve if it is a destination of a flow that is unresolved.
  private var failedTables: Seq[Table] = Seq.empty

  private val parallelism = 10

  // Executor used to resolve nodes in parallel. It is lazily initialized to avoid creating it
  // for scenarios its not required. To track if the lazy val was evaluated or not we use a
  // separate variable so we know if we need to shutdown the executor or not.
  private var fixedPoolExecutorInitialized = false
  lazy private val fixedPoolExecutor = {
    fixedPoolExecutorInitialized = true
    ThreadUtils.newDaemonFixedThreadPool(
      parallelism,
      prefix = "data-flow-graph-transformer-"
    )
  }
  private val selfExecutor = ThreadUtils.sameThreadExecutorService()

  private def computeTableMap(): Map[TableIdentifier, Table] = synchronized {
    tables.map(table => table.identifier -> table).toMap
  }

  private def computeViewMap(): Map[TableIdentifier, View] = synchronized {
    views.map(view => view.identifier -> view).toMap
  }

  private def computeFlowsTo(): Map[TableIdentifier, Seq[Flow]] = synchronized {
    flows.groupBy(_.destinationIdentifier)
  }

  def transformTables(transformer: Table => Table): DataflowGraphTransformer = synchronized {
    tables = tables.map(transformer)
    tableMap = computeTableMap()
    this
  }

  private def defaultOnFailedDependentTables(
      failedTableDependencies: Map[TableIdentifier, Seq[Table]]): Unit = {
    require(
      failedTableDependencies.isEmpty,
      "Dependency failure happened and some tables were not resolved"
    )
  }

  /**
   * Example graph: [Flow1, Flow 2] -> ST -> Flow3 -> MV
   * Order of processing: Flow1, Flow2, ST, Flow3, MV.
   * @param transformer function that transforms any graph entity.
   * transformer(
   *    nodeToTransform: GraphElement, upstreamNodes: Seq[GraphElement]
   * ) => transformedNodes: Seq[GraphElement]
   * @return this
   */
  def transformDownNodes(
      transformer: (GraphElement, Seq[GraphElement]) => Seq[GraphElement],
      disableParallelism: Boolean = false): DataflowGraphTransformer = {
    val executor = if (disableParallelism) selfExecutor else fixedPoolExecutor
    val batchSize = if (disableParallelism) 1 else parallelism
    // List of resolved tables, sinks and flows
    val resolvedFlows = new ConcurrentLinkedQueue[ResolutionCompletedFlow]()
    val resolvedTables = new ConcurrentLinkedQueue[Table]()
    val resolvedViews = new ConcurrentLinkedQueue[View]()
    // Flow identifier to a list of transformed flows mapping to track resolved flows
    val resolvedFlowsMap = new ConcurrentHashMap[TableIdentifier, Seq[Flow]]()
    val resolvedFlowDestinationsMap = new ConcurrentHashMap[TableIdentifier, Boolean]()
    val failedFlowsQueue = new ConcurrentLinkedQueue[ResolutionFailedFlow]()
    val failedDependentFlows = new ConcurrentHashMap[TableIdentifier, Seq[ResolutionFailedFlow]]()

    var futures = ArrayBuffer[Future[Unit]]()
    val toBeResolvedFlows = new ConcurrentLinkedDeque[Flow]()
    toBeResolvedFlows.addAll(flows.asJava)

    while (futures.nonEmpty || toBeResolvedFlows.peekFirst() != null) {
      val (done, notDone) = futures.partition(_.isDone)
      // Explicitly call future.get() to propagate exceptions one by one if any
      try {
        done.foreach(_.get())
      } catch {
        case exn: ExecutionException =>
          // Computation threw the exception that is the cause of exn
          throw exn.getCause
      }
      futures = notDone
      val flowOpt = {
        // We only schedule [[batchSize]] number of flows in parallel.
        if (futures.size < batchSize) {
          Option(toBeResolvedFlows.pollFirst())
        } else {
          None
        }
      }
      if (flowOpt.isDefined) {
        val flow = flowOpt.get
        futures.append(
          executor.submit(
            () =>
              try {
                try {
                  // Note: Flow don't need their inputs passed, so for now we send empty Seq.
                  val result = transformer(flow, Seq.empty)
                  require(
                    result.forall(_.isInstanceOf[ResolvedFlow]),
                    "transformer must return a Seq[Flow]"
                  )

                  val transformedFlows = result.map(_.asInstanceOf[ResolvedFlow])
                  resolvedFlowsMap.put(flow.identifier, transformedFlows)
                  resolvedFlows.addAll(transformedFlows.asJava)
                } catch {
                  case e: TransformNodeRetryableException =>
                    val datasetIdentifier = e.datasetIdentifier
                    failedDependentFlows.compute(
                      datasetIdentifier,
                      (_, flows) => {
                        // Don't add the input flow back but the failed flow object
                        // back which has relevant failure information.
                        val failedFlow = e.failedNode
                        if (flows == null) {
                          Seq(failedFlow)
                        } else {
                          flows :+ failedFlow
                        }
                      }
                    )
                    // Between the time the flow started and finished resolving, perhaps the
                    // dependent dataset was resolved
                    resolvedFlowDestinationsMap.computeIfPresent(
                      datasetIdentifier,
                      (_, resolved) => {
                        if (resolved) {
                          // Check if the dataset that the flow is dependent on has been resolved
                          // and if so, remove all dependent flows from the failedDependentFlows and
                          // add them to the toBeResolvedFlows queue for retry.
                          failedDependentFlows.computeIfPresent(
                            datasetIdentifier,
                            (_, toRetryFlows) => {
                              toRetryFlows.foreach(toBeResolvedFlows.addFirst(_))
                              null
                            }
                          )
                        }
                        resolved
                      }
                    )
                  case other: Throwable => throw other
                }
                // If all flows to this particular destination are resolved, move to the destination
                // node transformer
                if (flowsTo(flow.destinationIdentifier).forall({ flowToDestination =>
                    resolvedFlowsMap.containsKey(flowToDestination.identifier)
                  })) {
                  // If multiple flows completed in parallel, ensure we resolve the destination only
                  // once by electing a leader via computeIfAbsent
                  var isCurrentThreadLeader = false
                  resolvedFlowDestinationsMap.computeIfAbsent(flow.destinationIdentifier, _ => {
                    isCurrentThreadLeader = true
                    // Set initial value as false as flow destination is not resolved yet.
                    false
                  })
                  if (isCurrentThreadLeader) {
                    if (tableMap.contains(flow.destinationIdentifier)) {
                      val transformed =
                        transformer(
                          tableMap(flow.destinationIdentifier),
                          flowsTo(flow.destinationIdentifier)
                        )
                      resolvedTables.addAll(
                        transformed.collect { case t: Table => t }.asJava
                      )
                      resolvedFlows.addAll(
                        transformed.collect { case f: ResolvedFlow => f }.asJava
                      )
                    } else {
                      if (viewMap.contains(flow.destinationIdentifier)) {
                        resolvedViews.addAll {
                          val transformed =
                            transformer(
                              viewMap(flow.destinationIdentifier),
                              flowsTo(flow.destinationIdentifier)
                            )
                          transformed.map(_.asInstanceOf[View]).asJava
                        }
                      } else {
                        throw new IllegalArgumentException(
                          s"Unsupported destination ${flow.destinationIdentifier.unquotedString}" +
                          s" in flow: ${flow.displayName} at transformDownNodes"
                        )
                      }
                    }
                    // Set flow destination as resolved now.
                    resolvedFlowDestinationsMap.computeIfPresent(
                      flow.destinationIdentifier,
                      (_, _) => {
                        // If there are any other node failures dependent on this destination, retry
                        // them
                        failedDependentFlows.computeIfPresent(
                          flow.destinationIdentifier,
                          (_, toRetryFlows) => {
                            toRetryFlows.foreach(toBeResolvedFlows.addFirst(_))
                            null
                          }
                        )
                        true
                      }
                    )
                  }
                }
              } catch {
                case ex: TransformNodeFailedException => failedFlowsQueue.add(ex.failedNode)
              }
          )
        )
      }
    }

    // Mutate the fail analysis entities
    // A table is failed to analyze if:
    // - It does not exist in the resolvedFlowDestinationsMap
    failedTables = tables.filterNot { table =>
      resolvedFlowDestinationsMap.getOrDefault(table.identifier, false)
    }

    // We maintain the topological sort order of successful flows always
    val (resolvedFlowsWithResolvedDest, resolvedFlowsWithFailedDest) =
      resolvedFlows.asScala.toSeq.partition(flow => {
        resolvedFlowDestinationsMap.getOrDefault(flow.destinationIdentifier, false)
      })

    // A flow is failed to analyze if:
    // - It is non-retryable
    // - It is retryable but could not be retried, i.e. the dependent dataset is still unresolved
    // - It might be resolvable but it writes into a destination that is failed to analyze
    // To note: because we are transform down nodes, all downstream nodes of any pruned nodes
    // will also be pruned
    failedFlows =
      // All transformed flows that write to a destination that is failed to analyze.
      resolvedFlowsWithFailedDest ++
      // All failed flows thrown by TransformNodeFailedException
      failedFlowsQueue.asScala.toSeq ++
      // All flows that have not been transformed and resolved yet
      failedDependentFlows.values().asScala.flatten.toSeq

    // Mutate the resolved entities
    flows = resolvedFlowsWithResolvedDest
    flowsTo = computeFlowsTo()
    tables = resolvedTables.asScala.toSeq
    views = resolvedViews.asScala.toSeq
    tableMap = computeTableMap()
    viewMap = computeViewMap()
    this
  }

  def getDataflowGraph: DataflowGraph = {
    graph.copy(
      // Returns all flows (resolved and failed) in topological order.
      // The relative order between flows and failed flows doesn't matter here.
      // For failed flows that were resolved but were marked failed due to destination failure,
      // they will be front of the list in failedFlows and thus by definition topologically sorted
      // in the combined sequence too.
      flows = flows ++ failedFlows,
      tables = tables ++ failedTables
    )
  }

  override def close(): Unit = {
    if (fixedPoolExecutorInitialized) {
      fixedPoolExecutor.shutdown()
    }
  }
}

object DataflowGraphTransformer {

  /**
   * Exception thrown when transforming a node in the graph fails because at least one of its
   * dependencies weren't yet transformed.
   *
   * @param datasetIdentifier The identifier for an untransformed dependency table identifier in the
   *                          dataflow graph.
   */
  case class TransformNodeRetryableException(
      datasetIdentifier: TableIdentifier,
      failedNode: ResolutionFailedFlow)
      extends Exception
      with NoStackTrace

  /**
   * Exception thrown when transforming a node in the graph fails with a non-retryable error.
   *
   * @param failedNode The failed node that could not be transformed.
   */
  case class TransformNodeFailedException(failedNode: ResolutionFailedFlow)
      extends Exception
      with NoStackTrace

  /**
   * Autocloseable wrapper around DataflowGraphTransformer to ensure that the transformer is closed
   * without clients needing to remember to close it. It takes in the same arguments as
   * [[DataflowGraphTransformer]] constructor. It exposes the DataflowGraphTransformer instance
   * within the callable scope.
   */
  def withDataflowGraphTransformer[T](graph: DataflowGraph)(f: DataflowGraphTransformer => T): T = {
    val dataflowGraphTransformer = new DataflowGraphTransformer(graph)
    try {
      f(dataflowGraphTransformer)
    } finally {
      dataflowGraphTransformer.close()
    }
  }
}
