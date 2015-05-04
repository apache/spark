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

package org.apache.spark.ui.viz

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.Logging
import org.apache.spark.rdd.OperatorScope
import org.apache.spark.scheduler.StageInfo

/**
 * A representation of a generic cluster graph used for storing visualization information.
 *
 * Each graph is defined with a set of edges and a root cluster, which may contain children
 * nodes and children clusters. Additionally, a graph may also have edges that enter or exit
 * the graph from nodes that belong to adjacent graphs.
 */
private[ui] case class VizGraph(
    edges: Seq[VizEdge],
    outgoingEdges: Seq[VizEdge],
    incomingEdges: Seq[VizEdge],
    rootCluster: VizCluster)

/** A node in a VizGraph. This represents an RDD. */
private[ui] case class VizNode(id: Int, name: String)

/** A directed edge connecting two nodes in a VizGraph. This represents an RDD dependency. */
private[ui] case class VizEdge(fromId: Int, toId: Int)

/**
 * A cluster that groups nodes together in a VizGraph.
 *
 * This represents any grouping of RDDs, including operator scopes (e.g. textFile, flatMap),
 * stages, jobs, or any higher level construct. A cluster may be nested inside of other clusters.
 */
private[ui] class VizCluster(val id: String, val name: String) {
  private val _childrenNodes = new ListBuffer[VizNode]
  private val _childrenClusters = new ListBuffer[VizCluster]

  def childrenNodes: Seq[VizNode] = _childrenNodes.iterator.toSeq
  def childrenClusters: Seq[VizCluster] = _childrenClusters.iterator.toSeq
  def attachChildNode(childNode: VizNode): Unit = { _childrenNodes += childNode }
  def attachChildCluster(childCluster: VizCluster): Unit = { _childrenClusters += childCluster }
}

private[ui] object VizGraph extends Logging {

  /**
   * Construct a VizGraph for a given stage.
   *
   * The root cluster represents the stage, and all children clusters represent RDD operations.
   * Each node represents an RDD, and each edge represents a dependency between two RDDs pointing
   * from the parent to the child.
   *
   * This does not currently merge common scopes across stages. This may be worth supporting in
   * the future when we decide to group certain stages within the same job under a common scope
   * (e.g. part of a SQL query).
   */
  def makeVizGraph(stage: StageInfo): VizGraph = {
    val edges = new ListBuffer[VizEdge]
    val nodes = new mutable.HashMap[Int, VizNode]
    val clusters = new mutable.HashMap[String, VizCluster] // cluster ID -> VizCluster

    // Root cluster is the stage cluster
    val stageClusterId = s"stage_${stage.stageId}"
    val stageClusterName = s"Stage ${stage.stageId}" +
      { if (stage.attemptId == 0) "" else s" (attempt ${stage.attemptId})" }
    val rootCluster = new VizCluster(stageClusterId, stageClusterName)

    // Find nodes, edges, and operator scopes that belong to this stage
    stage.rddInfos.foreach { rdd =>
      edges ++= rdd.parentIds.map { parentId => VizEdge(parentId, rdd.id) }
      val node = nodes.getOrElseUpdate(rdd.id, VizNode(rdd.id, rdd.name))

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
          clusters.getOrElseUpdate(clusterId, new VizCluster(clusterId, clusterName))
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
    val internalEdges = new ListBuffer[VizEdge]
    val outgoingEdges = new ListBuffer[VizEdge]
    val incomingEdges = new ListBuffer[VizEdge]
    edges.foreach { case e: VizEdge =>
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

    VizGraph(internalEdges, outgoingEdges, incomingEdges, rootCluster)
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
  def makeDotFile(graph: VizGraph, forJob: Boolean): String = {
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
   * Return the dot representation of a node.
   *
   * On the job page, is displayed as a small circle without labels.
   * On the stage page, it is displayed as a box with an embedded label.
   */
  private def makeDotNode(node: VizNode, forJob: Boolean): String = {
    if (forJob) {
      s"""${node.id} [label=" " shape="circle" padding="5" labelStyle="font-size: 0"]"""
    } else {
      s"""${node.id} [label="${node.name} (${node.id})"]"""
    }
  }

  /** Return the dot representation of a subgraph. */
  private def makeDotSubgraph(scope: VizCluster, forJob: Boolean, indent: String): String = {
    val subgraph = new StringBuilder
    subgraph.append(indent + s"subgraph cluster${scope.id} {\n")
    subgraph.append(indent + s"""  label="${scope.name}";\n""")
    scope.childrenNodes.foreach { node =>
      subgraph.append(indent + s"  ${makeDotNode(node, forJob)};\n")
    }
    scope.childrenClusters.foreach { cscope =>
      subgraph.append(makeDotSubgraph(cscope, forJob, indent + "  "))
    }
    subgraph.append(indent + "}\n")
    subgraph.toString()
  }
}
