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

package org.apache.spark.sql.execution.ui

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.util.Try

/**
 * Provides functions that can be used to update nodes from SparkPlanGraphs.
 */
private class SparkPlanGraphNodeTransformers {

  /**
   * Identify nodes that should be hidden from the Spark SQL UI (descendants of InMemoryRelations
   * with no computed partitions i.e. cached InMemoryRelations).
   *
   * @param graph to identify hidden nodes for
   * @param metricValues map of metric ids to values for associated graph
   * @return predicate function that identifies node ids as corresponding to hidden nodes or not
   */
  def getIsHiddenNode(graph: SparkPlanGraph, metricValues: Map[Long, String]): Long => Boolean = {

    def getAllDescendantsOfCachedInMemoryRelations: Set[Long] = {

      @tailrec def getAllDescendantsOfNodes(nodeIds: Set[Long], outputAcc: Set[Long]): Set[Long] = {
        val descendants = nodeIds.flatMap(graph.childrenForEachNode)
          .filter(id => !outputAcc.contains(id) && !nodeIds.contains(id))
        if (descendants.isEmpty) {
          outputAcc
        } else {
          getAllDescendantsOfNodes(descendants, outputAcc ++ descendants)
        }
      }

      val cachedInMemoryRelations = graph.allNodes
        .filter(node => isCachedInMemoryRelation(node, metricValues)).map(_.id).toSet

      getAllDescendantsOfNodes(cachedInMemoryRelations, Set.empty)
    }

    def getAllClustersWithOnlyHiddenSubnodes(hiddenNodes: Set[Long]): Set[Long] =
      // Add formerly non-hidden clusters whose subnodes are all hidden
      graph.allNodes.filter {
        case cluster: SparkPlanGraphCluster => cluster.nodes.nonEmpty &&
          cluster.nodes.forall(node => hiddenNodes(node.id))
        case _ => false
      }.map(_.id).toSet

    val hiddenNodes = getAllDescendantsOfCachedInMemoryRelations
    val allHiddenNodes = hiddenNodes ++ getAllClustersWithOnlyHiddenSubnodes(hiddenNodes)
    allHiddenNodes.contains
  }

  /**
   * Identify metrics that should be hidden from the Spark SQL UI (InMemoryRelations with no
   * computed partitions i.e. cached InMemoryRelations).
   *
   * @param metricValues map of metric ids to values
   * @return transformer function that filters out hidden metrics from a node
   */
  def getFilterHiddenMetricsForNode(metricValues: Map[Long, String]
                                   ): SparkPlanGraphNode => SparkPlanGraphNode =
    node => node.name match {
      case "InMemoryRelation" =>
        val filteredMetrics = node.metrics.filter(metric => !isNumComputedPartitions(metric))
        node.copy(newMetrics = filteredMetrics)
      case _ => node
    }

  /**
   * Determine reassignment of node ids such that new ids are consecutively increasing from 0 to
   * nodes.size.
   *
   * @param graph to find reassignment for
   * @return transformer function that returns a new id given the old id for any node on the graph
   */
  def getNodeIdReassigner(graph: SparkPlanGraph): Long => Long = {
    val nodeIdGenerator = new AtomicLong(0)
    val allNodesSorted = graph.allNodes.sortBy(_.id)
    val oldToNewIds: Map[Long, Long] = allNodesSorted.map(node =>
      (node.id, nodeIdGenerator.getAndIncrement())).toMap

    oldId => oldToNewIds.getOrElse(oldId,
      throw new IllegalArgumentException("old id does not exist in reassignment mapping"))
  }

  private def isCachedInMemoryRelation(node: SparkPlanGraphNode,
                                       metricValues: Map[Long, String]): Boolean =
    node.name == "InMemoryRelation" && hasZeroComputedPartitions(node.metrics, metricValues)

  private def hasZeroComputedPartitions(nodeMetrics: Seq[SQLPlanMetric],
                                        metricValues: Map[Long, String]): Boolean =
    nodeMetrics.exists { metric =>
      isNumComputedPartitions(metric) &&
          metricValues.get(metric.accumulatorId).exists(_.toLongOption.contains(0L))
    }

  private def isNumComputedPartitions(metric: SQLPlanMetric): Boolean =
    metric.name == "numComputedPartitions" && metric.metricType == "sum"

  private implicit class StringToLongOption(s: String) {
    def toLongOption: Option[Long] = Try(s.toLong).toOption
  }
}

/**
 * Provides functions used to update SparkPlanGraphs before rendering. Uses transformer functions
 * defined by SparkPlanGraphNodeTransformers by default.
 *
 * @param metricValues map of metric ids to values for a graph
 * @param transformers node transformers class that can be overridden to provide different
 *                     functionality than the default SparkPlanGraphNodeTransformer
 */
private class SparkPlanGraphUpdater(private val metricValues: Map[Long, String],
                                    private val transformers: SparkPlanGraphNodeTransformers =
                                    new SparkPlanGraphNodeTransformers) {

  /**
   * Prunes nodes that should be hidden from the Spark SQL UI.
   *
   * @param graph to prune hidden nodes from
   * @return a new SparkPlanGraph with hidden nodes pruned
   */
  def pruneHiddenNodes(graph: SparkPlanGraph): SparkPlanGraph = {
    val isHiddenNode = transformers.getIsHiddenNode(graph, metricValues)
    val prunedNodes = filterNodes(graph.nodes, node => !isHiddenNode(node.id))
    val prunedEdges = filterEdges(graph, isHiddenNode)
    SparkPlanGraph(prunedNodes, prunedEdges)
  }

  /**
   * Filters out metrics that should be hidden from the Spark SQL UI.
   *
   * @param graph to prune hidden nodes from
   * @return a new SparkPlanGraph with hidden nodes pruned
   */
  def filterHiddenMetrics(graph: SparkPlanGraph): SparkPlanGraph = {
    val filterHiddenMetricsForNode = transformers.getFilterHiddenMetricsForNode(metricValues)
    val filteredNodes = mapNodes(graph.nodes, filterHiddenMetricsForNode)
    SparkPlanGraph(filteredNodes, graph.edges)
  }

  /**
   * Reassign node ids for a graph.
   *
   * @param graph to reassign node ids for
   * @return a new SparkPlanGraph with node ids reassigned
   */
  def reassignNodeIds(graph: SparkPlanGraph): SparkPlanGraph = {
    val reassigner = transformers.getNodeIdReassigner(graph)
    val updatedNodes = mapNodes(graph.nodes, node => node.copy(newId = reassigner(node.id)))
    val updatedEdges = graph.edges.map(
      edge => edge.copy(reassigner(edge.fromId), reassigner(edge.toId)))
    SparkPlanGraph(updatedNodes, updatedEdges)
  }

  private def mapNodes(nodes: Seq[SparkPlanGraphNode],
                       transformer: SparkPlanGraphNode => SparkPlanGraphNode
                      ): Seq[SparkPlanGraphNode] = {
    def mapNodesRecursively(nodes: Seq[SparkPlanGraphNode]): Seq[SparkPlanGraphNode] = {
      nodes.map {
        case cluster: SparkPlanGraphCluster =>
          transformer(cluster.updateNodes(mapNodesRecursively(cluster.nodes)))
        case node => transformer(node)
      }
    }
    mapNodesRecursively(nodes)
  }

  private def filterNodes(nodes: Seq[SparkPlanGraphNode],
                          predicate: SparkPlanGraphNode => Boolean
                         ): Seq[SparkPlanGraphNode] = {
    def filterNodesRecursively(nodes: Seq[SparkPlanGraphNode]): Seq[SparkPlanGraphNode] = {
      nodes collect {
        case cluster: SparkPlanGraphCluster if predicate(cluster) =>
          filterNodesRecursively(cluster.nodes) match {
            case cluster.nodes => cluster
            case subnodes => cluster.updateNodes(subnodes)
          }
        case node if predicate(node) => node
      }
    }
    filterNodesRecursively(nodes)
  }

  private def filterEdges(graph: SparkPlanGraph,
                          isHiddenNode: Long => Boolean): Seq[SparkPlanGraphEdge] = {
    val isNodeOfHiddenCluster = graph.allNodes.collect {
      case cluster: SparkPlanGraphCluster if isHiddenNode(cluster.id) => cluster.nodes
    }.flatten.map(_.id).toSet

    graph.edges.filter(edge => !isHiddenNode(edge.fromId) &&
      !isHiddenNode(edge.toId) &&
      !isNodeOfHiddenCluster(edge.fromId) &&
      !isNodeOfHiddenCluster(edge.toId))
  }
}
