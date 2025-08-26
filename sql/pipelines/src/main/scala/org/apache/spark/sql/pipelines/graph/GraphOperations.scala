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

import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * @param identifier The identifier of the flow.
 * @param inputs The identifiers of nodes used as inputs to this flow.
 * @param output The identifier of the output that this flow writes to.
 */
case class FlowNode(
    identifier: TableIdentifier,
    inputs: Set[TableIdentifier],
    output: TableIdentifier)

trait GraphOperations {
  this: DataflowGraph =>

  /** The set of all outputs in the graph. */
  private lazy val destinationSet: Set[TableIdentifier] =
    flows.map(_.destinationIdentifier).toSet

  /** A map from flow identifier to `FlowNode`, which contains the input/output nodes. */
  lazy val flowNodes: Map[TableIdentifier, FlowNode] = {
    flows.map { f =>
      val identifier = f.identifier
      val n =
        FlowNode(
          identifier = identifier,
          inputs = resolvedFlow(f.identifier).inputs,
          output = f.destinationIdentifier
        )
      identifier -> n
    }.toMap
  }

  /** Map from dataset identifier to all reachable upstream destinations, including itself. */
  private lazy val upstreamDestinations =
    mutable.HashMap
      .empty[TableIdentifier, Set[TableIdentifier]]
      .withDefault(key => dfsInternal(startDestination = key, downstream = false))

  /** Map from dataset identifier to all reachable downstream destinations, including itself. */
  private lazy val downstreamDestinations =
    mutable.HashMap
      .empty[TableIdentifier, Set[TableIdentifier]]
      .withDefault(key => dfsInternal(startDestination = key, downstream = true))

  /**
   * Performs a DFS starting from `startNode` and returns the set of nodes (datasets) reached.
   * @param startDestination The identifier of the node to start from.
   * @param downstream if true, traverse output edges (search downstream)
   *                        if false, traverse input edges (search upstream).
   * @param stopAtMaterializationPoints If true, stop when we reach a materialization point (table).
   *                                    If false, keep going until the end.
   */
  protected def dfsInternal(
      startDestination: TableIdentifier,
      downstream: Boolean,
      stopAtMaterializationPoints: Boolean = false): Set[TableIdentifier] = {
    assert(
      destinationSet.contains(startDestination),
      s"$startDestination is not a valid start node"
    )
    val visited = new mutable.HashSet[TableIdentifier]

    // Same semantics as a stack. Need to be able to push/pop items.
    var nextNodes = List[TableIdentifier]()
    nextNodes = startDestination :: nextNodes

    while (nextNodes.nonEmpty) {
      val currNode = nextNodes.head
      nextNodes = nextNodes.tail

      if (!visited.contains(currNode)
        // When we stop at materialization points, skip non-start nodes that are materialized.
        && !(stopAtMaterializationPoints && table.contains(currNode)
        && currNode != startDestination)) {
        visited.add(currNode)
        val neighbors = if (downstream) {
          flowNodes.values.filter(_.inputs.contains(currNode)).map(_.output)
        } else {
          flowNodes.values.filter(_.output == currNode).flatMap(_.inputs)
        }
        nextNodes = neighbors.toList ++ nextNodes
      }
    }
    visited.toSet
  }

  /**
   * An implementation of DFS that takes in a sequence of start nodes and returns the
   * "reachability set" of nodes from the start nodes.
   *
   * @param downstream Walks the graph via the input edges if true, otherwise via the output
   *                   edges.
   * @return A map from visited nodes to its origin[s] in `datasetIdentifiers`, e.g.
   *         Let graph = a -> b  c -> d (partitioned graph)
   *
   *         reachabilitySet(Seq("a", "c"), downstream = true)
   *         -> ["a" -> ["a"], "b" -> ["a"], "c" -> ["c"], "d" -> ["c"]]
   */
  private def reachabilitySet(
      datasetIdentifiers: Seq[TableIdentifier],
      downstream: Boolean): Map[TableIdentifier, Set[TableIdentifier]] = {
    // Seq of the form "node identifier" -> Set("dependency1, "dependency2")
    val deps = datasetIdentifiers.map(n => n -> reachabilitySet(n, downstream))

    // Invert deps so that we get a map from each dependency to node identifier
    val finalMap = mutable.HashMap
      .empty[TableIdentifier, Set[TableIdentifier]]
      .withDefaultValue(Set.empty[TableIdentifier])
    deps.foreach {
      case (start, reachableNodes) =>
        reachableNodes.foreach(n => finalMap.put(n, finalMap(n) + start))
    }
    finalMap.toMap
  }

  /**
   * Returns all datasets that can be reached from `destinationIdentifier`.
   */
  private def reachabilitySet(
      destinationIdentifier: TableIdentifier,
      downstream: Boolean): Set[TableIdentifier] = {
    if (downstream) downstreamDestinations(destinationIdentifier)
    else upstreamDestinations(destinationIdentifier)
  }

  /** Returns the set of flows reachable from `flowIdentifier` via output (child) edges. */
  def downstreamFlows(flowIdentifier: TableIdentifier): Set[TableIdentifier] = {
    assert(flowNodes.contains(flowIdentifier), s"$flowIdentifier is not a valid start flow")
    val downstreamDatasets = reachabilitySet(flowNodes(flowIdentifier).output, downstream = true)
    flowNodes.values.filter(_.inputs.exists(downstreamDatasets.contains)).map(_.identifier).toSet
  }

  /** Returns the set of flows reachable from `flowIdentifier` via input (parent) edges. */
  def upstreamFlows(flowIdentifier: TableIdentifier): Set[TableIdentifier] = {
    assert(flowNodes.contains(flowIdentifier), s"$flowIdentifier is not a valid start flow")
    val upstreamDatasets =
      flowNodes(flowIdentifier).inputs.flatMap(reachabilitySet(_, downstream = false))
    flowNodes.values.filter(e => upstreamDatasets.contains(e.output)).map(_.identifier).toSet
  }

  /** Returns the set of datasets reachable from `datasetIdentifier` via input (parent) edges. */
  def upstreamDatasets(datasetIdentifier: TableIdentifier): Set[TableIdentifier] =
    reachabilitySet(datasetIdentifier, downstream = false) - datasetIdentifier

  /**
   * Traverses the graph upstream starting from the specified `datasetIdentifiers` to return the
   * reachable nodes. The return map's keyset consists of all datasets reachable from
   * `datasetIdentifiers`. For each entry in the response map, the value of that element refers
   * to which of `datasetIdentifiers` was able to reach the key. If multiple of `datasetIdentifiers`
   * could reach that key, one is picked arbitrarily.
   */
  def upstreamDatasets(
      datasetIdentifiers: Seq[TableIdentifier]): Map[TableIdentifier, Set[TableIdentifier]] =
    reachabilitySet(datasetIdentifiers, downstream = false) -- datasetIdentifiers
}
