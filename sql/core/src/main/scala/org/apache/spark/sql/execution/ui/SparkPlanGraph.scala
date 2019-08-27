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

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.{SparkPlanInfo, WholeStageCodegenExec}
import org.apache.spark.sql.internal.SQLConf


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

  /**
   * Returns a map that gives the children for each node, or the empty set if a node has no
   * children.
   */
  lazy val childrenForEachNode: Map[Long, Set[Long]] = {
    val defaultForEachNode = allNodes.map(_.id -> Set.empty[Long]).toMap
    defaultForEachNode ++ edges.groupBy(_.toId)
      .map { case (k, v) => k -> v.map(_.fromId).toSet }
  }

  /**
   * Whether the graph ids increase consecutively from 0 or not.
   */
  lazy val idsAreConsecutiveFromZero: Boolean = {
    val sortedIds = allNodes.map(_.id).sorted
    sortedIds == sortedIds.indices
  }
}

object SparkPlanGraph {

  /**
   * Build a SparkPlanGraph from the root of a SparkPlan tree.
   */
  def apply(planInfo: SparkPlanInfo, conf: SparkConf): SparkPlanGraph = {
    val nodeIdGenerator = new AtomicLong(0)
    val nodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
    val edges = mutable.ArrayBuffer[SparkPlanGraphEdge]()
    val reusedPlans = mutable.HashMap[SparkPlanNodeKey, SparkPlanGraphNode]()
    buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, null, null, reusedPlans)(conf)
    new SparkPlanGraph(nodes, edges)
  }

  private def buildSparkPlanGraphNode(
      planInfo: SparkPlanInfo,
      nodeIdGenerator: AtomicLong,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      edges: mutable.ArrayBuffer[SparkPlanGraphEdge],
      parent: SparkPlanGraphNode,
      subgraph: SparkPlanGraphCluster,
      reusedPlans: mutable.HashMap[SparkPlanNodeKey, SparkPlanGraphNode])
                                     (implicit conf: SparkConf): Unit = {
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
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, cluster, reusedPlans)
      case "InputAdapter" =>
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, reusedPlans)
      case "BroadcastQueryStage" | "ShuffleQueryStage" =>
        if (reusedPlans.contains(planInfo.children.head.getNodeKey)) {
          // Point to the re-used exchange
          val node = reusedPlans(planInfo.children.head.getNodeKey)
          edges += SparkPlanGraphEdge(node.id, parent.id)
        } else {
          buildSparkPlanGraphNode(
            planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, reusedPlans)
        }
      case "Subquery" | "InMemoryRelation" if subgraph != null =>
        // Subquery/relation should not be included in WholeStageCodegen
        buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, parent, null, reusedPlans)
      case "Subquery" | "InMemoryRelation" if reusedPlans.contains(planInfo.getNodeKey) =>
        // Point to the re-used subquery/relation
        val node = reusedPlans(planInfo.getNodeKey)
        edges += SparkPlanGraphEdge(node.id, parent.id)
      case "ReusedSubquery" =>
        // Re-used subquery might appear before the original subquery, so skip this node and let
        // the previous `case` make sure the re-used and the original point to the same node.
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, subgraph, reusedPlans)
      case "ReusedExchange" if reusedPlans.contains(planInfo.children.head.getNodeKey) =>
        // Point to the re-used exchange
        val node = reusedPlans(planInfo.children.head.getNodeKey)
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

        if (name.contains("Exchange") || name == "Subquery" || name == "InMemoryRelation") {
          reusedPlans += planInfo.getNodeKey -> node
        }

        if (parent != null) {
          edges += SparkPlanGraphEdge(node.id, parent.id)
        }

        def isNotBlacklistedChild(childInfo: SparkPlanInfo): Boolean = {
          conf.get(SQLConf.UI_EXTENDED_INFO) ||
            !(name == "InMemoryTableScan" && childInfo.nodeName == "InMemoryRelation")
        }

        planInfo.children.filter(isNotBlacklistedChild).foreach(
          buildSparkPlanGraphNode(_, nodeIdGenerator, nodes, edges, node, subgraph, reusedPlans))
    }
  }

  private case class SparkPlanNodeKey(nodeName: String,
                                      simpleString: String,
                                      children: Seq[SparkPlanNodeKey],
                                      metadata: Map[String, String])

  private implicit class SparkPlanInfoAdditions(info: SparkPlanInfo) {
    def getNodeKey: SparkPlanNodeKey = {
      // InMemoryRelation has its simpleString replaced to ensure that InMemoryRelations with
      // different simpleStrings are merged correctly
      val simpleString = info.nodeName match {
        case "InMemoryRelation" => "InMemoryRelation"
        case _ => info.simpleString
      }
      val children = info.children.map(_.getNodeKey)
      SparkPlanNodeKey(info.nodeName, simpleString, children, info.metadata)
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
private[ui] class SparkPlanGraphNode(
    val id: Long,
    val name: String,
    val desc: String,
    val metrics: Seq[SQLPlanMetric]) {

  def makeDotNode(metricsValue: Map[Long, String]): String = {
    val builder = new mutable.StringBuilder(name)

    val values = for {
      metric <- metrics
      value <- metricsValue.get(metric.accumulatorId)
    } yield {
      metric.name + ": " + value
    }

    if (values.nonEmpty) {
      // If there are metrics, display each entry in a separate line.
      // Note: whitespace between two "\n"s is to create an empty line between the name of
      // SparkPlan and metrics. If removing it, it won't display the empty line in UI.
      builder ++= "\n \n"
      builder ++= values.mkString("\n")
    }

    s"""  $id [label="${StringEscapeUtils.escapeJava(builder.toString())}"];"""
  }

  def copy(newId: Long = id,
           newName: String = name,
           newDesc: String = desc,
           newMetrics: Seq[SQLPlanMetric] = metrics): SparkPlanGraphNode =
    new SparkPlanGraphNode(newId, newName, newDesc, newMetrics)
}

/**
 * Represent a tree of SparkPlan for WholeStageCodegen.
 */
private[ui] class SparkPlanGraphCluster(
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
      if (metricsValue.contains(duration(0).accumulatorId)) {
        name + "\n\n" + metricsValue(id)
      } else {
        name
      }
    } else {
      name
    }
    s"""
       |  subgraph cluster${id} {
       |    label="${StringEscapeUtils.escapeJava(labelStr)}";
       |    ${nodes.map(_.makeDotNode(metricsValue)).mkString("    \n")}
       |  }
     """.stripMargin
  }

  override def copy(newId: Long = id,
                    newName: String = name,
                    newDesc: String = desc,
                    newMetrics: Seq[SQLPlanMetric] = metrics): SparkPlanGraphCluster =
    new SparkPlanGraphCluster(newId, newName, newDesc, nodes, newMetrics)

  def updateNodes(newNodes: Seq[SparkPlanGraphNode]): SparkPlanGraphCluster = {
    this.nodes.clear()
    this.nodes ++= newNodes
    this
  }
}


/**
 * Represent an edge in the SparkPlan tree. `fromId` is the child node id, and `toId` is the parent
 * node id.
 */
private[ui] case class SparkPlanGraphEdge(fromId: Long, toId: Long) {

  def makeDotEdge: String = s"""  $fromId->$toId;\n"""
}
