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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.Logging
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
private[ui] case class RDDOperationNode(id: Int, name: String, cached: Boolean)

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
private[ui] class RDDOperationCluster(val id: String, val name: String) {
  private val _childNodes = new ListBuffer[RDDOperationNode]
  private val _childClusters = new ListBuffer[RDDOperationCluster]

  def childNodes: Seq[RDDOperationNode] = _childNodes.iterator.toSeq
  def childClusters: Seq[RDDOperationCluster] = _childClusters.iterator.toSeq
  def attachChildNode(childNode: RDDOperationNode): Unit = { _childNodes += childNode }
  def attachChildCluster(childCluster: RDDOperationCluster): Unit = {
    _childClusters += childCluster
  }

  /** Return all the nodes container in this cluster, including ones nested in other clusters. */
  def getAllNodes: Seq[RDDOperationNode] = {
    _childNodes ++ _childClusters.flatMap(_.childNodes)
  }
}

private[ui] object RDDOperationGraph extends Logging {

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
  def makeOperationGraph(stage: StageInfo): RDDOperationGraph = {
    val edges = new ListBuffer[RDDOperationEdge]
    val nodes = new mutable.HashMap[Int, RDDOperationNode]
    val clusters = new mutable.HashMap[String, RDDOperationCluster] // indexed by cluster ID

    // Root cluster is the stage cluster
    val stageClusterId = s"stage_${stage.stageId}"
    val stageClusterName = s"Stage ${stage.stageId}" +
      { if (stage.attemptId == 0) "" else s" (attempt ${stage.attemptId})" }
    val rootCluster = new RDDOperationCluster(stageClusterId, stageClusterName)

    // Find nodes, edges, and operation scopes that belong to this stage
    stage.rddInfos.foreach { rdd =>
      edges ++= rdd.parentIds.map { parentId => RDDOperationEdge(parentId, rdd.id) }

      // TODO: differentiate between the intention to cache an RDD and whether it's actually cached
      val node = nodes.getOrElseUpdate(
        rdd.id, RDDOperationNode(rdd.id, rdd.name, rdd.storageLevel != StorageLevel.NONE))

      if (rdd.scope == null) {
        // This RDD has no encompassing scope, so we put it directly in the root cluster
        // This should happen only if an RDD is instantiated outside of a public RDD API
        rootCluster.attachChildNode(node)
      } else {
        // Otherwise, this RDD belongs to an inner cluster,
        // which may be nested inside of other clusters
        val rddScopes = rdd.scope.map { scope => scope.getAllScopes }.getOrElse(Seq.empty)
        val rddClusters = rddScopes.map { scope =>
          val clusterId = scope.name + "_" + scope.id
          val clusterName = scope.name
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
        rddClusters.headOption.foreach { cluster => rootCluster.attachChildCluster(cluster) }
        rddClusters.lastOption.foreach { cluster => cluster.attachChildNode(node) }
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
  def makeDotFile(graph: RDDOperationGraph, forJob: Boolean): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    dotFile.append(makeDotSubgraph(graph.rootCluster, forJob, indent = "  "))
    graph.edges.foreach { edge =>
      dotFile.append(s"""  ${edge.fromId}->${edge.toId} [lineInterpolate="basis"];\n""")
    }
    dotFile.append("}")
    val result = dotFile.toString()
    logDebug(result)
    result
  }

  /**
   * Return the dot representation of a node in an RDDOperationGraph.
   *
   * On the job page, is displayed as a small circle without labels.
   * On the stage page, it is displayed as a box with an embedded label.
   */
  private def makeDotNode(node: RDDOperationNode, forJob: Boolean): String = {
    if (forJob) {
      s"""${node.id} [label=" " shape="circle" padding="5" labelStyle="font-size: 0"]"""
    } else {
      s"""${node.id} [label="${node.name} (${node.id})"]"""
    }
  }

  /** Return the dot representation of a subgraph in an RDDOperationGraph. */
  private def makeDotSubgraph(
      scope: RDDOperationCluster,
      forJob: Boolean,
      indent: String): String = {
    val subgraph = new StringBuilder
    subgraph.append(indent + s"subgraph cluster${scope.id} {\n")
    subgraph.append(indent + s"""  label="${scope.name}";\n""")
    scope.childNodes.foreach { node =>
      subgraph.append(indent + s"  ${makeDotNode(node, forJob)};\n")
    }
    scope.childClusters.foreach { cscope =>
      subgraph.append(makeDotSubgraph(cscope, forJob, indent + "  "))
    }
    subgraph.append(indent + "}\n")
    subgraph.toString()
  }
}
