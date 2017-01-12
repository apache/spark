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

package org.apache.spark.ui.scope

import java.util.Objects

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, StringBuilder}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.storage.StorageLevel

/**
 * A representation of a generic cluster graph used for storing information on RDD operations.
 *
 * Each graph is defined with a set of edges and a root cluster, which may contain children
 * nodes and children clusters. Additionally, a graph may also have edges that enter or exit
 * the graph from nodes that belong to adjacent graphs.
 */
private[ui] case class RDDOperationGraph(
    edges: Seq[RDDOperationEdge],
    outgoingEdges: Seq[RDDOperationEdge],
    incomingEdges: Seq[RDDOperationEdge],
    rootCluster: RDDOperationCluster)

/** A node in an RDDOperationGraph. This represents an RDD. */
private[ui] case class RDDOperationNode(id: Int, name: String, cached: Boolean, callsite: String)

/**
 * A directed edge connecting two nodes in an RDDOperationGraph.
 * This represents an RDD dependency.
 */
private[ui] case class RDDOperationEdge(fromId: Int, toId: Int)

/**
 * A cluster that groups nodes together in an RDDOperationGraph.
 *
 * This represents any grouping of RDDs, including operation scopes (e.g. textFile, flatMap),
 * stages, jobs, or any higher level construct. A cluster may be nested inside of other clusters.
 */
private[ui] class RDDOperationCluster(val id: String, private var _name: String) {
  private val _childNodes = new ListBuffer[RDDOperationNode]
  private val _childClusters = new ListBuffer[RDDOperationCluster]

  def name: String = _name
  def setName(n: String): Unit = { _name = n }

  def childNodes: Seq[RDDOperationNode] = _childNodes.iterator.toSeq
  def childClusters: Seq[RDDOperationCluster] = _childClusters.iterator.toSeq
  def attachChildNode(childNode: RDDOperationNode): Unit = { _childNodes += childNode }
  def attachChildCluster(childCluster: RDDOperationCluster): Unit = {
    _childClusters += childCluster
  }

  /** Return all the nodes which are cached. */
  def getCachedNodes: Seq[RDDOperationNode] = {
    _childNodes.filter(_.cached) ++ _childClusters.flatMap(_.getCachedNodes)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[RDDOperationCluster]

  override def equals(other: Any): Boolean = other match {
    case that: RDDOperationCluster =>
      (that canEqual this) &&
          _childClusters == that._childClusters &&
          id == that.id &&
          _name == that._name
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(_childClusters, id, _name)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }
}

private[ui] object RDDOperationGraph extends Logging {

  val STAGE_CLUSTER_PREFIX = "stage_"

  /**
   * Construct a RDDOperationGraph for a given stage.
   *
   * The root cluster represents the stage, and all children clusters represent RDD operations.
   * Each node represents an RDD, and each edge represents a dependency between two RDDs pointing
   * from the parent to the child.
   *
   * This does not currently merge common operation scopes across stages. This may be worth
   * supporting in the future if we decide to group certain stages within the same job under
   * a common scope (e.g. part of a SQL query).
   */
  def makeOperationGraph(stage: StageInfo, retainedNodes: Int): RDDOperationGraph = {
    val edges = new ListBuffer[RDDOperationEdge]
    val nodes = new mutable.HashMap[Int, RDDOperationNode]
    val clusters = new mutable.HashMap[String, RDDOperationCluster] // indexed by cluster ID

    // Root cluster is the stage cluster
    // Use a special prefix here to differentiate this cluster from other operation clusters
    val stageClusterId = STAGE_CLUSTER_PREFIX + stage.stageId
    val stageClusterName = s"Stage ${stage.stageId}" +
      { if (stage.attemptId == 0) "" else s" (attempt ${stage.attemptId})" }
    val rootCluster = new RDDOperationCluster(stageClusterId, stageClusterName)

    var rootNodeCount = 0
    val addRDDIds = new mutable.HashSet[Int]()
    val dropRDDIds = new mutable.HashSet[Int]()

    // Find nodes, edges, and operation scopes that belong to this stage
    stage.rddInfos.sortBy(_.id).foreach { rdd =>
      val parentIds = rdd.parentIds
      val isAllowed =
        if (parentIds.isEmpty) {
          rootNodeCount += 1
          rootNodeCount <= retainedNodes
        } else {
          parentIds.exists(id => addRDDIds.contains(id) || !dropRDDIds.contains(id))
        }

      if (isAllowed) {
        addRDDIds += rdd.id
        edges ++= parentIds.filter(id => !dropRDDIds.contains(id)).map(RDDOperationEdge(_, rdd.id))
      } else {
        dropRDDIds += rdd.id
      }

      // TODO: differentiate between the intention to cache an RDD and whether it's actually cached
      val node = nodes.getOrElseUpdate(rdd.id, RDDOperationNode(
        rdd.id, rdd.name, rdd.storageLevel != StorageLevel.NONE, rdd.callSite))
      if (rdd.scope.isEmpty) {
        // This RDD has no encompassing scope, so we put it directly in the root cluster
        // This should happen only if an RDD is instantiated outside of a public RDD API
        if (isAllowed) {
          rootCluster.attachChildNode(node)
        }
      } else {
        // Otherwise, this RDD belongs to an inner cluster,
        // which may be nested inside of other clusters
        val rddScopes = rdd.scope.map { scope => scope.getAllScopes }.getOrElse(Seq.empty)
        val rddClusters = rddScopes.map { scope =>
          val clusterId = scope.id
          val clusterName = scope.name.replaceAll("\\n", "\\\\n")
          clusters.getOrElseUpdate(clusterId, new RDDOperationCluster(clusterId, clusterName))
        }
        // Build the cluster hierarchy for this RDD
        rddClusters.sliding(2).foreach { pc =>
          if (pc.size == 2) {
            val parentCluster = pc(0)
            val childCluster = pc(1)
            parentCluster.attachChildCluster(childCluster)
          }
        }
        // Attach the outermost cluster to the root cluster, and the RDD to the innermost cluster
        rddClusters.headOption.foreach { cluster =>
          if (!rootCluster.childClusters.contains(cluster)) {
            rootCluster.attachChildCluster(cluster)
          }
        }
        if (isAllowed) {
          rddClusters.lastOption.foreach { cluster => cluster.attachChildNode(node) }
        }
      }
    }

    // Classify each edge as internal, outgoing or incoming
    // This information is needed to reason about how stages relate to each other
    val internalEdges = new ListBuffer[RDDOperationEdge]
    val outgoingEdges = new ListBuffer[RDDOperationEdge]
    val incomingEdges = new ListBuffer[RDDOperationEdge]
    edges.foreach { case e: RDDOperationEdge =>
      val fromThisGraph = nodes.contains(e.fromId)
      val toThisGraph = nodes.contains(e.toId)
      (fromThisGraph, toThisGraph) match {
        case (true, true) => internalEdges += e
        case (true, false) => outgoingEdges += e
        case (false, true) => incomingEdges += e
        // should never happen
        case _ => logWarning(s"Found an orphan edge in stage ${stage.stageId}: $e")
      }
    }

    RDDOperationGraph(internalEdges, outgoingEdges, incomingEdges, rootCluster)
  }

  /**
   * Generate the content of a dot file that describes the specified graph.
   *
   * Note that this only uses a minimal subset of features available to the DOT specification.
   * Part of the styling must be done here because the rendering library must take certain
   * attributes into account when arranging the graph elements. More style is added in the
   * visualization later through post-processing in JavaScript.
   *
   * For the complete DOT specification, see http://www.graphviz.org/Documentation/dotguide.pdf.
   */
  def makeDotFile(graph: RDDOperationGraph): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    makeDotSubgraph(dotFile, graph.rootCluster, indent = "  ")
    graph.edges.foreach { edge => dotFile.append(s"""  ${edge.fromId}->${edge.toId};\n""") }
    dotFile.append("}")
    val result = dotFile.toString()
    logDebug(result)
    result
  }

  /** Return the dot representation of a node in an RDDOperationGraph. */
  private def makeDotNode(node: RDDOperationNode): String = {
    val label = s"${node.name} [${node.id}]\n${node.callsite}"
    s"""${node.id} [label="${StringEscapeUtils.escapeJava(label)}"]"""
  }

  /** Update the dot representation of the RDDOperationGraph in cluster to subgraph. */
  private def makeDotSubgraph(
      subgraph: StringBuilder,
      cluster: RDDOperationCluster,
      indent: String): Unit = {
    subgraph.append(indent).append(s"subgraph cluster${cluster.id} {\n")
      .append(indent).append(s"""  label="${StringEscapeUtils.escapeJava(cluster.name)}";\n""")
    cluster.childNodes.foreach { node =>
      subgraph.append(indent).append(s"  ${makeDotNode(node)};\n")
    }
    cluster.childClusters.foreach { cscope =>
      makeDotSubgraph(subgraph, cscope, indent + "  ")
    }
    subgraph.append(indent).append("}\n")
  }
}
