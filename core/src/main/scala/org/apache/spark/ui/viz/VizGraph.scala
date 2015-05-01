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
import org.apache.spark.rdd.RDDScope
import org.apache.spark.scheduler.StageInfo

/**
 * A representation of a generic scoped graph used for storing visualization information.
 *
 * Each graph is defined with a set of edges and a root scope, which may contain children
 * nodes and children scopes. Additionally, a graph may also have edges that enter or exit
 * the graph from nodes that belong to adjacent graphs.
 */
private[ui] case class VizGraph(
    edges: Seq[VizEdge],
    outgoingEdges: Seq[VizEdge],
    incomingEdges: Seq[VizEdge],
    rootScope: VizScope)

private[ui] case class VizNode(id: Int, name: String)
private[ui] case class VizEdge(fromId: Int, toId: Int)

/** A cluster in the graph that represents a level in the scope hierarchy. */
private[ui] class VizScope(val id: String, val name: String) {
  private val _childrenNodes = new ListBuffer[VizNode]
  private val _childrenScopes = new ListBuffer[VizScope]

  def this(id: String) { this(id, id.split(RDDScope.SCOPE_NAME_DELIMITER).head) }

  def childrenNodes: Seq[VizNode] = _childrenNodes.iterator.toSeq
  def childrenScopes: Seq[VizScope] = _childrenScopes.iterator.toSeq
  def attachChildNode(childNode: VizNode): Unit = { _childrenNodes += childNode }
  def attachChildScope(childScope: VizScope): Unit = { _childrenScopes += childScope }
}

private[ui] object VizGraph extends Logging {

  /**
   * Construct a VizGraph for a given stage.
   *
   * The root scope represents the stage, and all children scopes represent individual
   * levels of RDD scopes. Each node represents an RDD, and each edge represents a dependency
   * between two RDDs from the parent to the child.
   */
  def makeVizGraph(stage: StageInfo): VizGraph = {
    val edges = new ListBuffer[VizEdge]
    val nodes = new mutable.HashMap[Int, VizNode]
    val scopes = new mutable.HashMap[String, VizScope] // scope ID -> viz scope

    // Root scope is the stage scope
    val stageScopeId = s"stage${stage.stageId}"
    val stageScopeName = s"Stage ${stage.stageId}" +
      { if (stage.attemptId == 0) "" else s" (attempt ${stage.attemptId})" }
    val rootScope = new VizScope(stageScopeId, stageScopeName)

    // Find nodes, edges, and children scopes that belong to this stage. Each node is an RDD
    // that lives either directly in the root scope or in one of the children scopes. Each
    // children scope represents a level of the RDD scope and must contain at least one RDD.
    // Children scopes can be nested if one RDD operation calls another.
    stage.rddInfos.foreach { rdd =>
      edges ++= rdd.parentIds.map { parentId => VizEdge(parentId, rdd.id) }
      val node = nodes.getOrElseUpdate(rdd.id, VizNode(rdd.id, rdd.name))

      if (rdd.scope == null) {
        // This RDD has no encompassing scope, so we put it directly in the root scope
        // This should happen only if an RDD is instantiated outside of a public RDD API
        rootScope.attachChildNode(node)
      } else {
        // Otherwise, this RDD belongs to an inner scope
        val rddScopes = rdd.scope
          .split(RDDScope.SCOPE_NESTING_DELIMITER)
          .map { scopeId => scopes.getOrElseUpdate(scopeId, new VizScope(scopeId)) }
        // Build the scope hierarchy for this RDD
        rddScopes.sliding(2).foreach { pc =>
          if (pc.size == 2) {
            val parentScope = pc(0)
            val childScope = pc(1)
            parentScope.attachChildScope(childScope)
          }
        }
        // Attach the outermost scope to the root scope, and the RDD to the innermost scope
        rddScopes.headOption.foreach { scope => rootScope.attachChildScope(scope) }
        rddScopes.lastOption.foreach { scope => scope.attachChildNode(node) }
      }
    }

    // Classify each edge as internal, outgoing or incoming
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

    VizGraph(internalEdges, outgoingEdges, incomingEdges, rootScope)
  }

  /**
   * Generate the content of a dot file that describes the specified graph.
   *
   * Note that this only uses a minimal subset of features available to the DOT specification.
   * More style is added in the visualization later through post-processing in JavaScript.
   *
   * For the complete specification, see http://www.graphviz.org/Documentation/dotguide.pdf.
   */
  def makeDotFile(graph: VizGraph, forJob: Boolean): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    dotFile.append(makeDotSubgraph(graph.rootScope, forJob, indent = "  "))
    graph.edges.foreach { edge =>
      dotFile.append(s"""  ${edge.fromId}->${edge.toId} [lineInterpolate="basis"];\n""")
    }
    dotFile.append("}")
    val result = dotFile.toString()
    logDebug(result)
    result
  }

  /** Return the dot representation of a node. */
  private def makeDotNode(node: VizNode, forJob: Boolean): String = {
    if (forJob) {
      // On the job page, we display RDDs as dots without labels
      s"""${node.id} [label=" " shape="circle" padding="5" labelStyle="font-size: 0"]"""
    } else {
      s"""${node.id} [label="${node.name} (${node.id})"]"""
    }
  }

  /** Return the dot representation of a subgraph recursively. */
  private def makeDotSubgraph(scope: VizScope, forJob: Boolean, indent: String): String = {
    val subgraph = new StringBuilder
    subgraph.append(indent + s"subgraph cluster${scope.id} {\n")
    subgraph.append(indent + s"""  label="${scope.name}";\n""")
    scope.childrenNodes.foreach { node =>
      subgraph.append(indent + s"  ${makeDotNode(node, forJob)};\n")
    }
    scope.childrenScopes.foreach { cscope =>
      subgraph.append(makeDotSubgraph(cscope, forJob, indent + "  "))
    }
    subgraph.append(indent + "}\n")
    subgraph.toString()
  }
}
