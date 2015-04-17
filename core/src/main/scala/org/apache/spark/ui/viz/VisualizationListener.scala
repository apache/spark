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

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.storage.RDDInfo

/**
 * A SparkListener that...
 */
private[spark] class VisualizationListener extends SparkListener {
  private val graphsByStageId = new mutable.HashMap[Int, VizGraph] // stage ID -> viz graph

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    val stageId = stageSubmitted.stageInfo.stageId
    val rddInfos = stageSubmitted.stageInfo.rddInfos

    println(s"******** STAGE $stageId **********")
    rddInfos.foreach(println)

    val vizGraph = makeVizGraph(rddInfos)
    graphsByStageId(stageId) = vizGraph

    println(
      s"""
        |=====================================================
        |${VisualizationListener.makeDotFile(vizGraph)}
        |=====================================================
      """.stripMargin)
  }

  /**
   *
   */
  private def makeVizGraph(rddInfos: Seq[RDDInfo]): VizGraph = {
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
        // Attach children scopes and nodes to each scope
        var previousScope: VizScope = null
        val scopeIt = rdd.scope.split(RDD.SCOPE_NESTING_DELIMITER).iterator
        while (scopeIt.hasNext) {
          val scopeId = scopeIt.next()
          val scope = scopes.getOrElseUpdate(scopeId, new VizScope(scopeId))
          scope.attachChildNode(node)
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
}

private object VisualizationListener {

  /**
   *
   */
  def makeDotFile(graph: VizGraph): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    //
    graph.rootScopes.foreach { scope =>
      dotFile.append(makeDotSubgraph(scope, "  "))
    }
    //
    graph.rootNodes.foreach { node =>
      dotFile.append("  " + makeDotNode(node) + "\n")
    }
    //
    graph.edges.foreach { edge =>
      dotFile.append("  " + edge.fromId + "->" + edge.toId + "\n")
    }
    dotFile.append("}")
    dotFile.toString()
  }

  /**
   *
   */
  private def makeDotNode(node: VizNode): String = {
    node.id + " [label = \"" + node.name + "\"]"
  }

  /**
   *
   */
  private def makeDotSubgraph(scope: VizScope, indent: String): String = {
    val subgraph = new StringBuilder
    subgraph.append(indent + "subgraph cluster" + scope.id + "{\n")
    subgraph.append(indent + "  label = \"" + scope.name + "\"\n")
    scope.childrenNodes.foreach { node =>
      subgraph.append(indent + "  " + makeDotNode(node) + "\n")
    }
    scope.childrenScopes.foreach { cscope =>
      subgraph.append(makeDotSubgraph(cscope, indent + "  "))
    }
    subgraph.append(indent + "}\n")
    subgraph.toString()
  }
}

private case class VizNode(id: Int, name: String)
private case class VizEdge(fromId: Int, toId: Int)

private class VizScope(val id: String) {
  private val _childrenNodes = new ArrayBuffer[VizNode]
  private val _childrenScopes = new ArrayBuffer[VizScope]
  val name: String = id.split(RDD.SCOPE_NAME_DELIMITER).head

  def childrenNodes: Seq[VizNode] = _childrenNodes.iterator.toSeq
  def childrenScopes: Seq[VizScope] = _childrenScopes.iterator.toSeq

  def attachChildNode(childNode: VizNode): Unit = { _childrenNodes += childNode }
  def attachChildScope(childScope: VizScope): Unit = { _childrenScopes += childScope }
}

private case class VizGraph(
    edges: Seq[VizEdge],
    rootNodes: Seq[VizNode],
    rootScopes: Seq[VizScope])
