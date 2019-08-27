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

class SparkPlanGraphNodeTransformersSuite extends SparkFunSuite {

  test("getIsHiddenNode output identifies children of cached InMemoryRelation as hidden") {
    val nodes = createNodesWithAscendingId("Parent", "InMemoryRelation", "child")
    val edges = Seq((2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val metrics = Map(0L -> "0")

    val transformers = new SparkPlanGraphNodeTransformers
    val isHiddenNode = transformers.getIsHiddenNode(graph, metrics)

    graph.nodes.map(node => isHiddenNode(node.id)) should contain theSameElementsInOrderAs
      Seq(false, false, true)
  }

  test("getIsHiddenNode output identifies children of non-cached InMemoryRelation as not hidden") {
    val nodes = createNodesWithAscendingId("Parent", "InMemoryRelation", "child")
    val edges = Seq((2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val metrics = Map(0L -> "1")

    val transformers = new SparkPlanGraphNodeTransformers
    val isHiddenNode = transformers.getIsHiddenNode(graph, metrics)

    graph.nodes.map(node => isHiddenNode(node.id)) should contain theSameElementsInOrderAs
      Seq(false, false, false)
  }

  test("getIsHiddenNode output identifies children of InMemoryRelation with no " +
    "numComputedPartitions metric as not hidden") {
    val nodes = Seq(createNode(0, "Parent"), createInMemoryRelationWithoutNumComputedPartitions(1),
      createNode(2, "child"))
    val edges = Seq((2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val metrics = Map.empty[Long, String]

    val transformers = new SparkPlanGraphNodeTransformers
    val isHiddenNode = transformers.getIsHiddenNode(graph, metrics)

    graph.nodes.map(node => isHiddenNode(node.id)) should contain theSameElementsInOrderAs
      Seq(false, false, false)
  }

  test("getIsHiddenNode output identifies children of InMemoryRelation with its " +
    "numComputedPartitions metric missing from metricValues as not hidden") {
    val nodes = createNodesWithAscendingId("Parent", "InMemoryRelation", "child")
    val edges = Seq((2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val metrics = Map.empty[Long, String]

    val transformers = new SparkPlanGraphNodeTransformers
    val isHiddenNode = transformers.getIsHiddenNode(graph, metrics)

    graph.nodes.map(node => isHiddenNode(node.id)) should contain theSameElementsInOrderAs
      Seq(false, false, false)
  }

  test("getIsHiddenNode output does not identify empty WholeStageCodegen as hidden") {
    val cluster = createWholeStageCodegen(0, Seq.empty)
    val graph = SparkPlanGraph(Seq(cluster), Seq.empty)

    val transformers = new SparkPlanGraphNodeTransformers
    val isHiddenNode = transformers.getIsHiddenNode(graph, Map.empty)

    isHiddenNode(cluster.id) shouldBe false
  }

  test("getIsHiddenNode output identifies child of cached InMemoryRelation nested inside " +
    "WholeStageCodegen cluster and the cluster itself as hidden") {
    val nodes = Seq(createNode(0, "InMemoryRelation"),
      createWholeStageCodegen(1, Seq(createNode(2, "child"))))
    val edges = Seq(createEdge((2L, 0L)))
    val graph = SparkPlanGraph(nodes, edges)

    val metrics = Map(0L -> "0")

    val transformers = new SparkPlanGraphNodeTransformers
    val isHiddenNode = transformers.getIsHiddenNode(graph, metrics)

    graph.nodes.map(node => isHiddenNode(node.id)) should contain theSameElementsInOrderAs
      Seq(false, true)

    val clusterSubNodes = graph.nodes.collect {
      case cluster: SparkPlanGraphCluster => cluster.nodes
    }.flatten
    clusterSubNodes.map(node => isHiddenNode(node.id)) should contain only true
  }

  test("GetFilterHiddenMetricsForNode output filters empty numComputedPartitions metric for" +
    " InMemoryRelation") {
    val node = createNode(0, "InMemoryRelation", Seq(createNumComputedPartitionsMetric(0)))
    val metrics = Map(0L -> "0")

    val transformers = new SparkPlanGraphNodeTransformers
    val filterHiddenMetricsForNode = transformers.getFilterHiddenMetricsForNode(metrics)

    filterHiddenMetricsForNode(node).metrics shouldBe empty
  }

  test("GetFilterHiddenMetricsForNode output filters out numComputedPartitions metric") {
    val numComputedPartitionsMetric = createNumComputedPartitionsMetric(0)
    val numComputedRowsMetric = SQLPlanMetric("numComputedRows", 1, "sum")
    val node = createNode(0, "InMemoryRelation",
      Seq(numComputedPartitionsMetric, numComputedRowsMetric))
    val metrics = Map(0L -> "1", 1L -> "0")

    val transformers = new SparkPlanGraphNodeTransformers
    val filterHiddenMetricsForNode = transformers.getFilterHiddenMetricsForNode(metrics)

    filterHiddenMetricsForNode(node).metrics should contain only numComputedRowsMetric
  }

  test("GetFilterHiddenMetrics output does not filter out empty numComputedPartitions not of " +
    "metricType \"sum\" and other metrics for non-InMemoryRelation") {
    val numComputedPartitionsMetric = SQLPlanMetric("numComputedPartitions", 0, "size")
    val numComputedRowsMetric = SQLPlanMetric("numComputedRows", 1, "sum")
    val node = createNode(0, "InMemoryRelation",
      Seq(numComputedPartitionsMetric, numComputedRowsMetric))
    val metrics = Map(0L -> "0", 1L -> "0")

    val transformers = new SparkPlanGraphNodeTransformers
    val filterHiddenMetricsForNode = transformers.getFilterHiddenMetricsForNode(metrics)

    filterHiddenMetricsForNode(node).metrics should contain only
      (numComputedPartitionsMetric, numComputedRowsMetric)
  }

  test("GetFilterHiddenMetrics output does not filter out empty numComputedPartitions and other " +
    "metrics for non-InMemoryRelation") {
    val numComputedPartitionsMetric = createNumComputedPartitionsMetric(0)
    val numComputedRowsMetric = SQLPlanMetric("numComputedRows", 1, "sum")
    val node = createNode(0, "NotInMemoryRelation",
      Seq(numComputedPartitionsMetric, numComputedRowsMetric))
    val metrics = Map(0L -> "0", 1L -> "0")

    val transformers = new SparkPlanGraphNodeTransformers
    val filterHiddenMetricsForNode = transformers.getFilterHiddenMetricsForNode(metrics)

    filterHiddenMetricsForNode(node).metrics should contain only
      (numComputedPartitionsMetric, numComputedRowsMetric)
  }

  test("getNodeIdReassigner creates 0 to n reassignment with graph with discontinuous ids") {
    val nodes = createNodes(("one", 0L), ("two", 2L), ("three", 3L))
    val edges = Seq((3L, 2L), (2L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers
    val reassigner = transformers.getNodeIdReassigner(graph)

    Seq(0L, 2L, 3L).map(reassigner) should contain theSameElementsInOrderAs Seq(0L, 1L, 2L)
  }

  test("getNodeIdReassigner creates 0 to n reassignment with graph where ids are continuous but" +
    " do not start at 0") {
    val nodes = createNodes(("one", 1L), ("two", 2L), ("three", 3L))
    val edges = Seq((3L, 2L), (2L, 1L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers
    val reassigner = transformers.getNodeIdReassigner(graph)

    Seq(1L, 2L, 3L).map(reassigner) should contain theSameElementsInOrderAs Seq(0L, 1L, 2L)
  }

  test("getNodeIdReassigner output throws exception when provided a node id not in the provided" +
    " graph") {
    val nodes = createNodes(("one", 0L), ("two", 2L), ("three", 3L))
    val edges = Seq((3L, 2L), (2L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers
    val reassigner = transformers.getNodeIdReassigner(graph)

    an [IllegalArgumentException] should be thrownBy reassigner(1L)
  }

  test("getNodeIdReassigner existing 0 to n assignment in preorder gives identity assignment") {
    val nodes = createNodes(("one", 0L), ("two", 1L), ("three", 2L))
    val edges = Seq((2L, 1L), (1L, 0L)).map(createEdge)
    val graph = SparkPlanGraph(nodes, edges)

    val transformers = new SparkPlanGraphNodeTransformers
    val reassigner = transformers.getNodeIdReassigner(graph)

    Seq(0L, 1L, 2L).map(reassigner) should contain theSameElementsInOrderAs Seq(0L, 1L, 2L)
  }

  private def createNodesWithAscendingId(nodeNames: String*): Seq[SparkPlanGraphNode] = {
    nodeNames.zipWithIndex.map {
      case (name, index) => createNode(index, name)
    }
  }

  private def createNodes(nodeNameAndIds: (String, Long)*): Seq[SparkPlanGraphNode] = {
    nodeNameAndIds.map{ case (name, id) => createNode(id, name) }
  }

  private def createNode(id: Long,
                         name: String,
                         metrics: Seq[SQLPlanMetric] = null): SparkPlanGraphNode = {
    val resultMetrics = metrics match {
      case null => name match {
        case "InMemoryRelation" => Seq(createNumComputedPartitionsMetric(0))
        case _ => Seq.empty
      }
      case metrics: Seq[SQLPlanMetric] => metrics
    }
    new SparkPlanGraphNode(id, name, "", resultMetrics)
  }

  private def createInMemoryRelationWithoutNumComputedPartitions(id: Long) =
    new SparkPlanGraphNode(id, "InMemoryRelation", "", Seq.empty)

  private def createWholeStageCodegen(id: Long,
                                      subNodes: Seq[SparkPlanGraphNode]): SparkPlanGraphCluster = {
    val buffer = new ArrayBuffer[SparkPlanGraphNode]()
    buffer ++= subNodes
    new SparkPlanGraphCluster(id, "WholeStageCodegen", "", buffer, Seq.empty)
  }

  private def createNumComputedPartitionsMetric(accumulatorId: Long): SQLPlanMetric =
    SQLPlanMetric("numComputedPartitions", accumulatorId, "sum")

  private def createEdge(edge: (Long, Long)): SparkPlanGraphEdge = edge match {
    case (from, to) => SparkPlanGraphEdge(from, to)
  }

}

