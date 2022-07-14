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
    nodes: Seq[SparkPlanGraphNode], edges: Seq[SparkPlanGraphEdge]) {

  def makeDotFile(metrics: Map[Long, String]): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    nodes.foreach(node => dotFile.append(node.makeDotNode(metrics) + "\n"))
    edges.foreach(edge => dotFile.append(edge.makeDotEdge + "\n"))
    dotFile.append("}")
    dotFile.toString()
  }

  /**
   * All the SparkPlanGraphNodes, including those inside of WholeStageCodegen.
   */
  val allNodes: Seq[SparkPlanGraphNode] = {
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
      case "Subquery" if subgraph != null =>
        // Subquery should not be included in WholeStageCodegen
        buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "Subquery" if exchanges.contains(planInfo) =>
        // Point to the re-used subquery
        val node = exchanges(planInfo)
        edges += SparkPlanGraphEdge(node.id, parent.id)
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
    val metrics: Seq[SQLPlanMetric]) {

  def makeDotNode(metricsValue: Map[Long, String]): String = {
    val builder = new mutable.StringBuilder("<b>" + name + "</b>")

    val values = for {
      metric <- metrics
      value <- metricsValue.get(metric.accumulatorId)
    } yield {
      // The value may contain ":" to extend the name, like `total (min, med, max): ...`
      if (value.contains(":")) {
        metric.name + " " + value
      } else {
        metric.name + ": " + value
      }
    }

    if (values.nonEmpty) {
      // If there are metrics, display each entry in a separate line.
      // Note: whitespace between two "\n"s is to create an empty line between the name of
      // SparkPlan and metrics. If removing it, it won't display the empty line in UI.
      builder ++= "<br><br>"
      builder ++= values.mkString("<br>")
      val labelStr = StringEscapeUtils.escapeJava(builder.toString().replaceAll("\n", "<br>"))
      s"""  $id [labelType="html" label="${labelStr}"];"""
    } else {
      // SPARK-30684: when there is no metrics, add empty lines to increase the height of the node,
      // so that there won't be gaps between an edge and a small node.
      s"""  $id [labelType="html" label="<br><b>$name</b><br><br>"];"""
    }
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
    metrics: Seq[SQLPlanMetric])
  extends SparkPlanGraphNode(id, name, desc, metrics) {

  override def makeDotNode(metricsValue: Map[Long, String]): String = {
    val duration = metrics.filter(_.name.startsWith(WholeStageCodegenExec.PIPELINE_DURATION_METRIC))
    val labelStr = if (duration.nonEmpty) {
      require(duration.length == 1)
      val id = duration(0).accumulatorId
      if (metricsValue.contains(id)) {
        name + "\n \n" + duration(0).name + ": " + metricsValue(id)
      } else {
        name
      }
    } else {
      name
    }
    s"""
       |  subgraph cluster${id} {
       |    isCluster="true";
       |    label="${StringEscapeUtils.escapeJava(labelStr)}";
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
