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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.RDDInfo
import org.apache.spark.rdd.RDDScope

/**
 * A class that represents an RDD DAG for a stage.
 *
 * Each scope can have many children scopes and children nodes, and edges can span multiple scopes.
 * Thus, it is sufficient to only keep track of the root scopes and the root nodes in the graph
 * as all children scopes and nodes will be transitively included.
 */
private[ui] case class VizGraph(
    edges: Seq[VizEdge],
    rootNodes: Seq[VizNode],
    rootScopes: Seq[VizScope])

/** A node in the graph that represents an RDD. */
private[ui] case class VizNode(id: Int, name: String)

/** An edge in the graph that represents an RDD dependency. */
private[ui] case class VizEdge(fromId: Int, toId: Int)

/** A cluster in the graph that represents a level in the RDD scope hierarchy. */
private[ui] class VizScope(val id: String) {
  private val _childrenNodes = new ArrayBuffer[VizNode]
  private val _childrenScopes = new ArrayBuffer[VizScope]
  val name: String = id.split(RDDScope.SCOPE_NAME_DELIMITER).head
  def childrenNodes: Seq[VizNode] = _childrenNodes.iterator.toSeq
  def childrenScopes: Seq[VizScope] = _childrenScopes.iterator.toSeq
  def attachChildNode(childNode: VizNode): Unit = { _childrenNodes += childNode }
  def attachChildScope(childScope: VizScope): Unit = { _childrenScopes += childScope }
}

private[ui] object VizGraph {

  /**
   * Construct a VizGraph from a list of RDDInfo's.
   *
   * The information needed to construct this graph include the names,
   * IDs, and scopes of all RDDs, and the dependencies between these RDDs.
   */
  def makeVizGraph(rddInfos: Seq[RDDInfo]): VizGraph = {
    val edges = new mutable.HashSet[VizEdge]
    val nodes = new mutable.HashMap[Int, VizNode]
    val scopes = new mutable.HashMap[String, VizScope] // scope ID -> viz scope

    // Entities that are not part of any scopes
    val rootNodes = new ArrayBuffer[VizNode]
    val rootScopes = new mutable.HashSet[VizScope]

    // Populate nodes, edges, and scopes
    rddInfos.foreach { rdd =>
      val node = nodes.getOrElseUpdate(rdd.id, VizNode(rdd.id, rdd.name))
      edges ++= rdd.parentIds.map { parentId => VizEdge(parentId, rdd.id) }

      if (rdd.scope == null) {
        // There is no encompassing scope, so this is a root node
        rootNodes += node
      } else {
        // Attach children scopes and nodes to each scope in the hierarchy
        var previousScope: VizScope = null
        val scopeIt = rdd.scope.split(RDDScope.SCOPE_NESTING_DELIMITER).iterator
        while (scopeIt.hasNext) {
          val scopeId = scopeIt.next()
          val scope = scopes.getOrElseUpdate(scopeId, new VizScope(scopeId))
          // Only attach this node to the innermost scope so
          // the node is not duplicated across all levels
          if (!scopeIt.hasNext) {
            scope.attachChildNode(node)
          }
          // RDD scopes are hierarchical, with the outermost scopes ordered first
          // If there is not a previous scope, then this must be a root scope
          if (previousScope == null) {
            rootScopes += scope
          } else {
            // Otherwise, attach this scope to its parent
            previousScope.attachChildScope(scope)
          }
          previousScope = scope
        }
      }
    }

    // Remove any edges with nodes belonging to other stages so we do not have orphaned nodes
    edges.retain { case VizEdge(f, t) => nodes.contains(f) && nodes.contains(t) }

    new VizGraph(edges.toSeq, rootNodes, rootScopes.toSeq)
  }

  /**
   * Generate the content of a dot file that describes the specified graph.
   *
   * Note that this only uses a minimal subset of features available to the DOT specification.
   * The style is added in the UI later through post-processing in JavaScript.
   *
   * For the complete specification, see http://www.graphviz.org/Documentation/dotguide.pdf.
   */
  def makeDotFile(graph: VizGraph): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    graph.rootScopes.foreach { scope =>
      dotFile.append(makeDotSubgraph(scope, "  "))
    }
    graph.rootNodes.foreach { node =>
      dotFile.append(s"  ${makeDotNode(node)};\n")
    }
    graph.edges.foreach { edge =>
      dotFile.append(s"  ${edge.fromId}->${edge.toId};\n")
    }
    dotFile.append("}")
    dotFile.toString()
  }

  /** Return the dot representation of a node. */
  private def makeDotNode(node: VizNode): String = {
    s"""${node.id} [label="${node.name} (${node.id})"]"""
  }

  /** Return the dot representation of a subgraph recursively. */
  private def makeDotSubgraph(scope: VizScope, indent: String): String = {
    val subgraph = new StringBuilder
    subgraph.append(indent + s"subgraph cluster${scope.id} {\n")
    scope.childrenNodes.foreach { node =>
      subgraph.append(indent + s"  ${makeDotNode(node)};\n")
    }
    scope.childrenScopes.foreach { cscope =>
      subgraph.append(makeDotSubgraph(cscope, indent + "  "))
    }
    subgraph.append(indent + "}\n")
    subgraph.toString()
  }
}
