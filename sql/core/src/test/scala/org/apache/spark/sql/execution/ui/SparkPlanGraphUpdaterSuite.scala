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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.Matchers._

import org.apache.spark.SparkFunSuite

class SparkPlanGraphUpdaterSuite extends SparkFunSuite {

  test("pruneHiddenNodes returns empty graph when empty graph is given") {
    val graph = SparkPlanGraph(Seq.empty, Seq.empty)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getIsHiddenNode(graph: SparkPlanGraph,
                                   metricValues: Map[Long, String]): Long => Boolean =
        _ == 1
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val prunedGraph = updater.pruneHiddenNodes(graph)

    prunedGraph.nodes shouldBe empty
    prunedGraph.edges shouldBe empty
  }

  test("pruneHiddenNodes prunes hidden nodes identified by input function") {
    val nodes = createNodes(0, 1, 2, 3)
    val edges = Seq((3L, 2L), (2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getIsHiddenNode(graph: SparkPlanGraph,
                                   metricValues: Map[Long, String]): Long => Boolean =
        _ > 1
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val prunedGraph = updater.pruneHiddenNodes(graph)

    prunedGraph.nodes.map(_.id) should contain only (0, 1)
    prunedGraph.getEdgesAsTuples should contain theSameElementsAs Seq((1L, 0L))
  }

  test("pruneHiddenNodes prunes hidden nodes identified by input function that are" +
    " in separate branches on graph") {
    val nodes = createNodes(0, 1, 2, 3)
    val edges = Seq((3L, 1L), (2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getIsHiddenNode(graph: SparkPlanGraph,
                                   metricValues: Map[Long, String]): Long => Boolean =
        _ > 1
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val prunedGraph = updater.pruneHiddenNodes(graph)

    prunedGraph.nodes.map(_.id) should contain only (0, 1)
    prunedGraph.getEdgesAsTuples should contain theSameElementsAs Seq((1L, 0L))
  }

  test("pruneHiddenNodes prunes every node in graph if every node is hidden") {
    val nodes = createNodes(0, 1, 2, 3)
    val edges = Seq((3L, 2L), (2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getIsHiddenNode(graph: SparkPlanGraph,
                                   metricValues: Map[Long, String]): Long => Boolean =
        _ => true
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val prunedGraph = updater.pruneHiddenNodes(graph)

    prunedGraph.nodes.map(_.id) shouldBe empty
    prunedGraph.getEdgesAsTuples shouldBe empty
  }

  test("pruneHiddenNodes prunes no nodes in graph if no nodes are hidden") {
    val nodes = createNodes(0, 1, 2, 3)
    val edges = Seq((3L, 2L), (2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getIsHiddenNode(graph: SparkPlanGraph,
                                   metricValues: Map[Long, String]): Long => Boolean =
        _ => false
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val prunedGraph = updater.pruneHiddenNodes(graph)

    prunedGraph.nodes.map(_.id) should contain only (0, 1, 2, 3)
    prunedGraph.getEdgesAsTuples should contain theSameElementsAs Seq((3L, 2L), (2L, 1L), (1L, 0L))
  }

  test("pruneHiddenNodes prunes hidden cluster and its non-hidden subnodes identified by input " +
    "function") {
    val nodes = Seq(createNode(0), createCluster(1, createNodes(2, 3)))
    val edges = Seq((3L, 2L), (2L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getIsHiddenNode(graph: SparkPlanGraph,
                                   metricValues: Map[Long, String]): Long => Boolean =
        _ == 1
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val prunedGraph = updater.pruneHiddenNodes(graph)

    prunedGraph.nodes.map(_.id) should contain only 0
    prunedGraph.getEdgesAsTuples shouldBe empty
  }

  test("pruneHiddenNodes doesn't prune cluster if all its subnodes are pruned") {
    val nodes = Seq(createNode(0), createCluster(1, createNodes(2, 3)))
    val edges = Seq((3L, 2L), (2L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getIsHiddenNode(graph: SparkPlanGraph,
                                   metricValues: Map[Long, String]): Long => Boolean =
        _ > 1
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val prunedGraph = updater.pruneHiddenNodes(graph)

    prunedGraph.nodes.map(_.id) should contain only (0, 1)
    prunedGraph.getEdgesAsTuples shouldBe empty
  }

  test("filterHiddenMetrics returns empty graph when empty graph is given") {
    val graph = SparkPlanGraph(Seq.empty, Seq.empty)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getFilterHiddenMetricsForNode(metricValues: Map[Long, String]
                                                ): SparkPlanGraphNode => SparkPlanGraphNode =
        node => node.copy(newMetrics = node.metrics.filter(_.name != "a"))
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val filteredGraph = updater.filterHiddenMetrics(graph)

    filteredGraph.nodes shouldBe empty
    filteredGraph.edges shouldBe empty
  }

  test("filterHiddenMetrics filters out node metrics identified by input function") {
    val metricA = createMetric("a")
    val metricB = createMetric("b")
    val metricC = createMetric("c")

    val nodes = Seq(createNode(0, Seq(metricA)),
      createNode(1, Seq(metricB)),
      createNode(2, Seq(metricC, metricA)))
    val edges = Seq((2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getFilterHiddenMetricsForNode(metricValues: Map[Long, String]
                                                ): SparkPlanGraphNode => SparkPlanGraphNode =
        node => node.copy(newMetrics = node.metrics.filter(_.name != "a"))
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val filteredGraph = updater.filterHiddenMetrics(graph)

    filteredGraph.nodes.map(_.metrics) should contain theSameElementsInOrderAs
      Seq(Seq.empty, Seq(metricB), Seq(metricC))
  }

  test("filterHiddenMetrics filters out node and cluster metrics identified by input function") {
    val metricA = createMetric("a")
    val metricB = createMetric("b")
    val metricC = createMetric("c")

    val nodes = Seq(createNode(0, Seq(metricA)),
      createCluster(1, Seq(createNode(2, Seq(metricB))), Seq(metricC, metricA)))
    val edges = Seq((2L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getFilterHiddenMetricsForNode(metricValues: Map[Long, String]
                                                ): SparkPlanGraphNode => SparkPlanGraphNode =
        node => node.copy(newMetrics = node.metrics.filter(_.name != "a"))
    }
    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val filteredGraph = updater.filterHiddenMetrics(graph)

    filteredGraph.allNodes.map(_.metrics) should contain theSameElementsInOrderAs
      Seq(Seq.empty, Seq(metricB), Seq(metricC))
  }

  test("reassignNodeIds returns empty graph when empty graph is given") {
    val graph = SparkPlanGraph(Seq.empty, Seq.empty)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getNodeIdReassigner(graph: SparkPlanGraph): Long => Long = {
        case 0 => 1
      }
    }

    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val updatedGraph = updater.reassignNodeIds(graph)

    updatedGraph.nodes shouldBe empty
    updatedGraph.edges shouldBe empty
  }

  test("reassignNodeIds reassigns node ids according to input function") {
    val nodes = createNodes(20, 300, 1)
    val edges = Seq((20L, 300L), (300L, 1L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getNodeIdReassigner(graph: SparkPlanGraph): Long => Long = {
        case 20 => 0
        case 300 => 1
        case 1 => 2
      }
    }

    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val updatedGraph = updater.reassignNodeIds(graph)

    updatedGraph.nodes.map(_.id) should contain theSameElementsInOrderAs Seq(0, 1, 2)
    updatedGraph.getEdgesAsTuples should contain theSameElementsAs Seq((0, 1), (1, 2))
  }

  test("reassignNodeIds reassigns node and cluster ids according to input function") {
    val nodes = Seq(createNode(20), createCluster(300, createNodes(1)))
    val edges = Seq((20L, 1L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers {
      override def getNodeIdReassigner(graph: SparkPlanGraph): Long => Long = {
        case 20 => 0
        case 300 => 1
        case 1 => 2
      }
    }

    val updater = new SparkPlanGraphUpdater(Map.empty, transformers)
    val updatedGraph = updater.reassignNodeIds(graph)

    updatedGraph.allNodes.map(_.id) should contain theSameElementsAs Seq(0, 1, 2)
    updatedGraph.getEdgesAsTuples should contain theSameElementsAs Seq((0, 2))
  }

  private def createNodes(nodeIds: Long*): Seq[SparkPlanGraphNode] = {
    nodeIds.map(id => createNode(id))
  }

  private def createNode(id: Long,
                         metrics: Seq[SQLPlanMetric] = Seq.empty): SparkPlanGraphNode = {
    new SparkPlanGraphNode(id, "", "", metrics)
  }

  private def createCluster(id: Long,
                            subNodes: Seq[SparkPlanGraphNode],
                            metrics: Seq[SQLPlanMetric] = Seq.empty): SparkPlanGraphCluster = {
    val buffer = new ArrayBuffer[SparkPlanGraphNode]()
    buffer ++= subNodes
    new SparkPlanGraphCluster(id, "", "", buffer, metrics)
  }

  private def createEdge(edge: (Long, Long)): SparkPlanGraphEdge = edge match {
    case (from, to) => SparkPlanGraphEdge(from, to)
  }

  private def createMetric(name: String): SQLPlanMetric = SQLPlanMetric(name, 0L, "")

  private implicit class SparkPlanGraphAdditions(graph: SparkPlanGraph) {
    def getEdgesAsTuples: Seq[(Long, Long)] = graph.edges.map {
      case SparkPlanGraphEdge(from, to) => (from, to)
    }
  }
}
