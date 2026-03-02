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

import scala.collection.mutable

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.execution.{SparkPlanInfo, WholeStageCodegenExec}


/**
 * A graph used for storing information of an executionPlan of DataFrame.
 *
 * Each graph is defined with a set of nodes and a set of edges. Each node represents a node in the
 * SparkPlan tree, and each edge represents a parent-child relationship between two nodes.
 */
case class SparkPlanGraph(
    nodes: collection.Seq[SparkPlanGraphNode],
    edges: collection.Seq[SparkPlanGraphEdge]) {

  def makeDotFile(metrics: Map[Long, String]): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    nodes.foreach(node => dotFile.append(node.makeDotNode(metrics) + "\n"))
    edges.foreach(edge => dotFile.append(edge.makeDotEdge + "\n"))
    dotFile.append("}")
    dotFile.toString()
  }

  /**
   * Generate a JSON string containing node details (name, metrics, and optional children)
   * for the detail side panel. This keeps the DOT node labels compact (name only) while
   * providing full metrics on demand via JavaScript.
   */
  def makeNodeDetailsJson(metrics: Map[Long, String]): String = {
    val entries = allNodes.map { node =>
      val metricsJson = node.metrics.flatMap { m =>
        metrics.get(m.accumulatorId).map { v =>
          val n = StringEscapeUtils.escapeJson(m.name)
          val mv = StringEscapeUtils.escapeJson(v)
          val mt = StringEscapeUtils.escapeJson(m.metricType)
          s"""{"name":"$n","value":"$mv","type":"$mt"}"""
        }
      }.mkString("[", ",", "]")
      val (prefix, extra) = node match {
        case cluster: SparkPlanGraphCluster =>
          val childIds = cluster.nodes
            .map(n => s""""node${n.id}"""").mkString("[", ",", "]")
          ("cluster", s""","children":$childIds""")
        case _ => ("node", "")
      }
      s"""  "$prefix${node.id}":{"name":"${StringEscapeUtils.escapeJson(node.name)}",""" +
        s""""metrics":$metricsJson$extra}"""
    }
    entries.mkString("{\n", ",\n", "\n}")
  }

  /**
   * All the SparkPlanGraphNodes, including those inside of WholeStageCodegen.
   */
  val allNodes: collection.Seq[SparkPlanGraphNode] = {
    nodes.flatMap {
      case cluster: SparkPlanGraphCluster => cluster.nodes :+ cluster
      case node => Seq(node)
    }
  }
}

object SparkPlanGraph {

  /**
   * Build a SparkPlanGraph from the root of a SparkPlan tree.
   */
  def apply(planInfo: SparkPlanInfo): SparkPlanGraph = {
    val nodeIdGenerator = new AtomicLong(0)
    val nodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
    val edges = mutable.ArrayBuffer[SparkPlanGraphEdge]()
    val exchanges = mutable.HashMap[SparkPlanInfo, SparkPlanGraphNode]()
    buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, null, null, exchanges)
    new SparkPlanGraph(nodes.toSeq, edges.toSeq)
  }

  private def buildSparkPlanGraphNode(
      planInfo: SparkPlanInfo,
      nodeIdGenerator: AtomicLong,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      edges: mutable.ArrayBuffer[SparkPlanGraphEdge],
      parent: SparkPlanGraphNode,
      subgraph: SparkPlanGraphCluster,
      exchanges: mutable.HashMap[SparkPlanInfo, SparkPlanGraphNode]): Unit = {
    planInfo.nodeName match {
      case name if name.startsWith("WholeStageCodegen") =>
        val metrics = planInfo.metrics.map { metric =>
          SQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
        }

        val cluster = new SparkPlanGraphCluster(
          nodeIdGenerator.getAndIncrement(),
          planInfo.nodeName,
          planInfo.simpleString,
          mutable.ArrayBuffer[SparkPlanGraphNode](),
          metrics)
        nodes += cluster

        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, cluster, exchanges)
      case "InputAdapter" =>
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "BroadcastQueryStage" | "ShuffleQueryStage" =>
        if (exchanges.contains(planInfo.children.head)) {
          // Point to the re-used exchange
          val node = exchanges(planInfo.children.head)
          edges += SparkPlanGraphEdge(node.id, parent.id)
        } else {
          buildSparkPlanGraphNode(
            planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, exchanges)
        }
      case "TableCacheQueryStage" | "ResultQueryStage" =>
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "Subquery" if subgraph != null =>
        // Subquery should not be included in WholeStageCodegen
        buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "Subquery" if exchanges.contains(planInfo) =>
        val node = exchanges(planInfo)
        val newEdge = SparkPlanGraphEdge(node.id, parent.id)
        if (!edges.contains(newEdge)) {
          // Point to the re-used subquery
          edges += newEdge
        }
      case "ReusedSubquery" =>
        // Re-used subquery might appear before the original subquery, so skip this node and let
        // the previous `case` make sure the re-used and the original point to the same node.
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, subgraph, exchanges)
      case "ReusedExchange" if exchanges.contains(planInfo.children.head) =>
        // Point to the re-used exchange
        val node = exchanges(planInfo.children.head)
        edges += SparkPlanGraphEdge(node.id, parent.id)
      case name =>
        val metrics = planInfo.metrics.map { metric =>
          SQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
        }
        val node = new SparkPlanGraphNode(
          nodeIdGenerator.getAndIncrement(), planInfo.nodeName,
          planInfo.simpleString, metrics)
        if (subgraph == null) {
          nodes += node
        } else {
          subgraph.nodes += node
        }
        if (name.contains("Exchange") || name == "Subquery") {
          exchanges += planInfo -> node
        }

        if (parent != null) {
          edges += SparkPlanGraphEdge(node.id, parent.id)
        }
        planInfo.children.foreach(
          buildSparkPlanGraphNode(_, nodeIdGenerator, nodes, edges, node, subgraph, exchanges))
    }
  }
}

/**
 * Represent a node in the SparkPlan tree, along with its metrics.
 *
 * @param id generated by "SparkPlanGraph". There is no duplicate id in a graph
 * @param name the name of this SparkPlan node
 * @param metrics metrics that this SparkPlan node will track
 */
class SparkPlanGraphNode(
    val id: Long,
    val name: String,
    val desc: String,
    val metrics: collection.Seq[SQLPlanMetric]) {

  def makeDotNode(metricsValue: Map[Long, String]): String = {
    val nodeId = s"node$id"
    val tooltip = StringEscapeUtils.escapeJava(desc)
    // Compact label: show only operator name; metrics shown in side panel on click
    val labelStr = StringEscapeUtils.escapeJava(name)
    s"""  $id [id="$nodeId" label="$labelStr" tooltip="$tooltip"];"""

  }
}

/**
 * Represent a tree of SparkPlan for WholeStageCodegen.
 */
class SparkPlanGraphCluster(
    id: Long,
    name: String,
    desc: String,
    val nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    metrics: collection.Seq[SQLPlanMetric])
  extends SparkPlanGraphNode(id, name, desc, metrics) {

  override def makeDotNode(metricsValue: Map[Long, String]): String = {
    val duration = metrics.filter(_.name.startsWith(WholeStageCodegenExec.PIPELINE_DURATION_METRIC))
    // Extract short label: "(1)" from "WholeStageCodegen (1)"
    val shortName = "\\(\\d+\\)".r.findFirstIn(name).getOrElse(name)
    val labelStr = if (duration.nonEmpty) {
      require(duration.length == 1)
      val durationMetricId = duration(0).accumulatorId
      if (metricsValue.contains(durationMetricId)) {
        // For multi-line values like "total (min, med, max)\n10.0 s (...)",
        // extract just the total number from the data line
        val raw = metricsValue(durationMetricId)
        val lines = raw.split("\n")
        val dataLine = if (lines.length > 1) lines(1).trim else lines(0).trim
        // Extract just the total value (first token before any parenthesized breakdown)
        val total = dataLine.split("\\s*\\(")(0).trim
        shortName + " / " + total
      } else {
        shortName
      }
    } else {
      shortName
    }
    val clusterId = s"cluster$id"
    s"""
       |  subgraph $clusterId {
       |    isCluster="true";
       |    id="$clusterId";
       |    label="${StringEscapeUtils.escapeJava(labelStr)}";
       |    tooltip="${StringEscapeUtils.escapeJava(desc)}";
       |    ${nodes.map(_.makeDotNode(metricsValue)).mkString("    \n")}
       |  }
     """.stripMargin
  }
}


/**
 * Represent an edge in the SparkPlan tree. `fromId` is the child node id, and `toId` is the parent
 * node id.
 */
case class SparkPlanGraphEdge(fromId: Long, toId: Long) {

  def makeDotEdge: String = s"""  $fromId->$toId;\n"""
}
