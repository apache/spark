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

import java.util.NoSuchElementException

import scala.collection.mutable.ArrayBuffer

import org.scalatest.Matchers._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.internal.SQLConf.UI_EXTENDED_INFO
import org.apache.spark.sql.test.SharedSparkSession

class SparkPlanGraphSuite extends SparkFunSuite with SharedSparkSession {

  private val confWithExtendedUIEnabled: SparkConf = new SparkConf()
    .set(UI_EXTENDED_INFO.key, "true")

  private val confWithExtendedUIDisabled: SparkConf = new SparkConf()
    .set(UI_EXTENDED_INFO.key, "false")

  test("SparkPlanInfo with nodename InMemoryTableScan has child InMemoryRelation processed with " +
    UI_EXTENDED_INFO.key + " enabled") {
      val inMemoryTableScanInfo =
        createStemInfo("InMemoryTableScan", createInMemoryRelationInfo())
      val graph = SparkPlanGraph(inMemoryTableScanInfo, confWithExtendedUIEnabled)

      graph.nodes.map(_.name) should contain only("InMemoryTableScan", "InMemoryRelation")
      graph.getEdgesAsTuples should contain theSameElementsAs Seq(
        ((1, "InMemoryRelation"), (0, "InMemoryTableScan")))
  }

  test("SparkPlanInfo with nodename InMemoryTableScan does not have child InMemoryRelation " +
    "processed with " + UI_EXTENDED_INFO.key + " disabled") {
      val inMemoryTableScanInfo = createStemInfo("InMemoryTableScan", createInMemoryRelationInfo())
      val graph = SparkPlanGraph(inMemoryTableScanInfo, confWithExtendedUIDisabled)

      graph.nodes.map(_.name) should contain only "InMemoryTableScan"
      graph.getEdgesAsTuples shouldBe empty
  }

  test("SparkPlanInfo with nodename InMemoryTableScan has child with nodename Subquery processed " +
    "with " + UI_EXTENDED_INFO.key + " enabled") {
    val inMemoryTableScanInfo = createStemInfo("InMemoryTableScan", createLeafInfo("Subquery"))
    val graph = SparkPlanGraph(inMemoryTableScanInfo, confWithExtendedUIEnabled)

    graph.nodes.map(_.name) should contain only ("InMemoryTableScan", "Subquery")
    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "Subquery"), (0, "InMemoryTableScan")))
  }

  test("SparkPlanInfo with nodename InMemoryTableScan has child with nodename Subquery " +
    "processed with " + UI_EXTENDED_INFO.key + " disabled") {
    val inMemoryTableScanInfo = createStemInfo("InMemoryTableScan", createLeafInfo("Subquery"))
    val graph = SparkPlanGraph(inMemoryTableScanInfo, confWithExtendedUIDisabled)

    graph.nodes.map(_.name) should contain only ("InMemoryTableScan", "Subquery")
    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "Subquery"), (0, "InMemoryTableScan")))
  }

  test("SparkPlanInfo with nodename InMemoryTableScan has child with nodename GenerateBloomFilter" +
    " processed with " + UI_EXTENDED_INFO.key + " enabled") {
    val generateBloomFilterInfo = createLeafInfo("GenerateBloomFilter")
    val inMemoryTableScanInfo = createStemInfo("InMemoryTableScan", generateBloomFilterInfo)
    val graph = SparkPlanGraph(inMemoryTableScanInfo, confWithExtendedUIEnabled)

    graph.nodes.map(_.name) should contain only ("InMemoryTableScan", "GenerateBloomFilter")
    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "GenerateBloomFilter"), (0, "InMemoryTableScan")))
  }

  test("SparkPlanInfo with nodename InMemoryTableScan has child with nodename GenerateBloomFilter" +
    " processed with " + UI_EXTENDED_INFO.key + " disabled") {
    val generateBloomFilterInfo = createLeafInfo("GenerateBloomFilter")
    val inMemoryTableScanInfo = createStemInfo("InMemoryTableScan", generateBloomFilterInfo)
    val graph = SparkPlanGraph(inMemoryTableScanInfo, confWithExtendedUIDisabled)

    graph.nodes.map(_.name) should contain only ("InMemoryTableScan", "GenerateBloomFilter")
    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "GenerateBloomFilter"), (0, "InMemoryTableScan")))
  }

  test("InMemoryTableScan inside WholeStageCodegen does not include the child InMemoryRelation" +
    " inside the WholeStageCodegen") {
      val children = Seq(createLeafInfo("child"))
      val inMemoryRelationInfo = createInMemoryRelationInfo(children = children)
      val inMemoryTableScanInfo = createStemInfo("InMemoryTableScan", inMemoryRelationInfo)
      val wholeStageCodegenInfo = createStemInfo("WholeStageCodegen", inMemoryTableScanInfo)
      val graph = SparkPlanGraph(wholeStageCodegenInfo, confWithExtendedUIEnabled)

      val clusters = graph.nodes collect { case cluster: SparkPlanGraphCluster => cluster }
      clusters.length shouldBe 1

      val codegenNodes = clusters.head.nodes
      codegenNodes.map(_.name) should contain only "InMemoryTableScan"

      val nonCodegenNodes = graph.nodes diff clusters
      nonCodegenNodes.map(_.name) should contain only("InMemoryRelation", "child")

      graph.getEdgesAsTuples should contain theSameElementsAs Seq(
        ((3, "child"), (2, "InMemoryRelation")),
        ((2, "InMemoryRelation"), (1, "InMemoryTableScan"))
      )
  }

  test("InMemoryRelation keeps simpleString after being processed into SparkPlanGraph") {
    val info = createInMemoryRelationInfo(simpleStringSuffix = "_a")
    val graph = SparkPlanGraph(info, confWithExtendedUIDisabled)

    graph.nodes.map(_.name) should contain only "InMemoryRelation"
    graph.nodes.map(_.desc) should contain only "InMemoryRelation_a"
  }

  test("SparkPlanInfo with 2 InMemoryRelation children with same cacheBuilderId and simpleString" +
    " are merged") {
    val inMemoryRelationInfoOne = createInMemoryRelationInfo(1)
    val inMemoryRelationInfoTwo = createInMemoryRelationInfo(1)
    val parentInfo = createStemInfo("Parent", inMemoryRelationInfoOne, inMemoryRelationInfoTwo)
    val graph = SparkPlanGraph(parentInfo, confWithExtendedUIDisabled)

    graph.nodes.map(_.name) should contain only ("Parent", "InMemoryRelation")

    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "InMemoryRelation"), (0, "Parent")),
      ((1, "InMemoryRelation"), (0, "Parent"))
    )
  }

  test("SparkPlanInfo with 2 InMemoryRelation children with different cacheBuilderId are not" +
    " merged") {
    val inMemoryRelationInfoOne = createInMemoryRelationInfo(1)
    val inMemoryRelationInfoTwo = createInMemoryRelationInfo(2)
    val parentInfo = createStemInfo("Parent", inMemoryRelationInfoOne, inMemoryRelationInfoTwo)
    val graph = SparkPlanGraph(parentInfo, confWithExtendedUIDisabled)

    graph.nodes.map(_.name) should contain theSameElementsAs
      Seq("Parent", "InMemoryRelation", "InMemoryRelation")

    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "InMemoryRelation"), (0, "Parent")),
      ((2, "InMemoryRelation"), (0, "Parent"))
    )
  }

  test("SparkPlanInfo with 2 InMemoryRelation children with same cacheBuilderId and simpleString" +
    " but different children are not merged") {
    val childrenOne = Seq(createLeafInfo("child1"))
    val childrenTwo = Seq(createLeafInfo("child2"))
    val inMemoryRelationInfoOne = createInMemoryRelationInfo(1, children = childrenOne)
    val inMemoryRelationInfoTwo = createInMemoryRelationInfo(1, children = childrenTwo)
    val parentInfo = createStemInfo("Parent", inMemoryRelationInfoOne, inMemoryRelationInfoTwo)
    val graph = SparkPlanGraph(parentInfo, confWithExtendedUIDisabled)

    graph.nodes.map(_.name) should contain theSameElementsAs
      Seq("Parent", "InMemoryRelation", "InMemoryRelation", "child1", "child2")

    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "InMemoryRelation"), (0, "Parent")),
      ((3, "InMemoryRelation"), (0, "Parent")),
      ((2, "child1"), (1, "InMemoryRelation")),
      ((4, "child2"), (3, "InMemoryRelation"))
    )
  }

  test("SparkPlanInfo with 2 InMemoryRelation children with same cacheBuilderId and children " +
    "but different simpleStrings are merged and desc of InMemoryRelation node reflects the " +
    "simpleString of first InMemoryRelation info in preorder") {
    val childrenOne = Seq(createLeafInfo("child"))
    val childrenTwo = Seq(createLeafInfo("child"))
    val inMemoryRelationInfoOne = createInMemoryRelationInfo(1, "_a", childrenOne)
    val inMemoryRelationInfoTwo = createInMemoryRelationInfo(1, "_b", childrenTwo)
    val parentInfo = createStemInfo("Parent", inMemoryRelationInfoOne, inMemoryRelationInfoTwo)
    val graph = SparkPlanGraph(parentInfo, confWithExtendedUIDisabled)

    graph.nodes.map(_.name) should contain theSameElementsAs
      Seq("Parent", "InMemoryRelation", "child")

    graph.nodes.filter(_.name == "InMemoryRelation").map(_.desc) should contain only
      inMemoryRelationInfoOne.simpleString

    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "InMemoryRelation"), (0, "Parent")),
      ((1, "InMemoryRelation"), (0, "Parent")),
      ((2, "child"), (1, "InMemoryRelation"))
    )
  }

  test("SparkPlanInfo with 2 InMemoryRelation children with different cacheBuilderId and" +
    " simpleStrings but same children are not merged") {
    val childrenOne = Seq(createLeafInfo("child"))
    val childrenTwo = Seq(createLeafInfo("child"))
    val inMemoryRelationInfoOne = createInMemoryRelationInfo(1, "_a", childrenOne)
    val inMemoryRelationInfoTwo = createInMemoryRelationInfo(2, "_b", childrenTwo)
    val parentInfo = createStemInfo("Parent", inMemoryRelationInfoOne, inMemoryRelationInfoTwo)
    val graph = SparkPlanGraph(parentInfo, confWithExtendedUIDisabled)

    graph.nodes.map(_.name) should contain theSameElementsAs Seq("Parent", "InMemoryRelation",
      "InMemoryRelation", "child", "child")

    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "InMemoryRelation"), (0, "Parent")),
      ((3, "InMemoryRelation"), (0, "Parent")),
      ((2, "child"), (1, "InMemoryRelation")),
      ((4, "child"), (3, "InMemoryRelation"))
    )
  }

  test("SparkPlanInfo with 2 InMemoryRelation children with same cacheBuilderId but different" +
    " simpleStrings and different children are not merged") {
    val childrenOne = Seq(createLeafInfo("child1"))
    val childrenTwo = Seq(createLeafInfo("child2"))
    val inMemoryRelationInfoOne = createInMemoryRelationInfo(1, "_a", childrenOne)
    val inMemoryRelationInfoTwo = createInMemoryRelationInfo(1, "_b", childrenTwo)
    val parentInfo = createStemInfo("Parent", inMemoryRelationInfoOne, inMemoryRelationInfoTwo)
    val graph = SparkPlanGraph(parentInfo, confWithExtendedUIDisabled)

    graph.nodes.map(_.name) should contain theSameElementsAs Seq("Parent", "InMemoryRelation",
      "InMemoryRelation", "child1", "child2")

    graph.getEdgesAsTuples should contain theSameElementsAs Seq(
      ((1, "InMemoryRelation"), (0, "Parent")),
      ((3, "InMemoryRelation"), (0, "Parent")),
      ((2, "child1"), (1, "InMemoryRelation")),
      ((4, "child2"), (3, "InMemoryRelation"))
    )
  }

  test("Graph with no nodes or edges has childrenForEachNode return empty Map") {
    val graph = SparkPlanGraph(Seq.empty, Seq.empty)

    graph.childrenForEachNode shouldBe Map.empty
  }

  test("Graph with one node and no edges has childrenForEachNode return empty Map with a default" +
    " value of empty set for the node") {
    val node = createNode(0)
    val graph = SparkPlanGraph(Seq(node), Seq.empty)

    graph.childrenForEachNode should have size 1
    graph.childrenForEachNode(node.id) shouldBe Set.empty
  }

  test("Graph with no nodes or edges has childrenForEachNode throw exception for node id not" +
    " in graph node set") {
    val node = createNode(0)
    val graph = SparkPlanGraph(Seq.empty, Seq.empty)

    graph.childrenForEachNode shouldBe Map.empty
    a [NoSuchElementException] should be thrownBy graph.childrenForEachNode(node.id)
  }

  test("Graph with two nodes and one edge has childrenForEachNode return Map with one entry") {
    val nodes = Seq(0L, 1L).map(createNode)
    val edges = Seq(SparkPlanGraphEdge(1L, 0L))
    val graph = SparkPlanGraph(nodes, edges)

    graph.childrenForEachNode should have size 2
    graph.childrenForEachNode(0L) should contain only 1L
    graph.childrenForEachNode(1L) shouldBe Set.empty
  }

  test("Graph with multiple nodes and edges has childrenForEachNode give expected result") {
    val nodes = Seq(0L, 1L, 2L, 3L).map(createNode)
    val edges = Seq((1L, 0L), (2L, 0L), (3L, 2L), (0L, 3L))
      .map{ case (x, y) => SparkPlanGraphEdge(x, y) }
    val graph = SparkPlanGraph(nodes, edges)

    graph.childrenForEachNode should have size 4
    graph.childrenForEachNode(0L) should contain only (1L, 2L)
    graph.childrenForEachNode(1L) shouldBe Set.empty
    graph.childrenForEachNode(2L) should contain only 3L
    graph.childrenForEachNode(3L) should contain only 0L
  }

  test("Graph with sub-node inside a SparkPlanGraphCluster has childrenForEachNode give" +
    " entry for subnode") {
    val subnode = createNode(1L)
    val cluster = createCluster(0L, Seq(subnode))
    val graph = SparkPlanGraph(Seq(cluster), Seq.empty)

    graph.childrenForEachNode should have size 2
    graph.childrenForEachNode(0L) shouldBe Set.empty
    graph.childrenForEachNode(1L) shouldBe Set.empty
  }

  test("Graph with subnode inside a SparkPlanGraphCluster connected to a non-subnode" +
    " has childrenForEachNode give expected result") {
    val subnode = createNode(1L)
    val cluster = createCluster(0L, Seq(subnode))
    val node = createNode(2L)
    val edges = Seq(SparkPlanGraphEdge(2L, 1L))
    val graph = SparkPlanGraph(Seq(cluster, node), edges)

    graph.childrenForEachNode should have size 3
    graph.childrenForEachNode(0L) shouldBe Set.empty
    graph.childrenForEachNode(1L) should contain only 2L
    graph.childrenForEachNode(2L) shouldBe Set.empty
  }

  test("Graph with 0 nodes has idsAreConsecutiveFromZero return true") {
    val graph = SparkPlanGraph(Seq.empty, Seq.empty)

    graph.idsAreConsecutiveFromZero shouldBe true
  }

  test("Graph with one node with id = 0 has idsAreConsecutiveFromZero return true") {
    val nodes = Seq(0L).map(createNode)
    val graph = SparkPlanGraph(nodes, Seq.empty)

    graph.idsAreConsecutiveFromZero shouldBe true
  }

  test("Graph with one node with id != 0 has idsAreConsecutiveFromZero return false") {
    val nodes = Seq(1L).map(createNode)
    val graph = SparkPlanGraph(nodes, Seq.empty)

    graph.idsAreConsecutiveFromZero shouldBe false
  }

  test("Graph with consecutive ids has idsAreConsecutiveFromZero return true") {
    val nodes = Seq(0L, 1L, 2L, 3L).map(createNode)
    val graph = SparkPlanGraph(nodes, Seq.empty)

    graph.idsAreConsecutiveFromZero shouldBe true
  }

  test("Graph with consecutive ids (but not given in consecutive order) has " +
    "idsAreConsecutiveFromZero return true") {
    val nodes = Seq(0L, 2L, 1L, 3L).map(createNode)
    val graph = SparkPlanGraph(nodes, Seq.empty)

    graph.idsAreConsecutiveFromZero shouldBe true
  }

  test("Graph with non-consecutive ids has idsAreConsecutiveFromZero return false") {
    val nodes = Seq(0L, 2L, 3L).map(createNode)
    val graph = SparkPlanGraph(nodes, Seq.empty)

    graph.idsAreConsecutiveFromZero shouldBe false
  }

  test("Graph with ids not starting at 0 has idsAreConsecutiveFromZero return false") {
    val nodes = Seq(1L, 2L, 3L).map(createNode)
    val graph = SparkPlanGraph(nodes, Seq.empty)

    graph.idsAreConsecutiveFromZero shouldBe false
  }

  private implicit class SparkPlanGraphAdditions(graph: SparkPlanGraph) {
    def getEdgesAsTuples: Seq[((Long, String), (Long, String))] = {
      val nodeIdsToNames = graph.allNodes.map(node => node.id -> node.name).toMap
      graph.edges collect {
        case SparkPlanGraphEdge(from, to) =>
          ((from, nodeIdsToNames(from)), (to, nodeIdsToNames(to)))
      }
    }
  }

  private def createInMemoryRelationInfo(id: Long = 0,
                                         simpleStringSuffix: String = "",
                                         children: Seq[SparkPlanInfo] = Nil): SparkPlanInfo = {
    new SparkPlanInfo("InMemoryRelation", "InMemoryRelation" + simpleStringSuffix,
      children, Map("cacheBuilderId" -> id.toString), Nil)
    }

  private def createStemInfo(name: String, children: SparkPlanInfo*): SparkPlanInfo =
    new SparkPlanInfo(name, name, children, Map.empty, Nil)

  private def createLeafInfo(name: String): SparkPlanInfo =
    new SparkPlanInfo(name, name, Nil, Map.empty, Nil)

  private def createNode(id: Long): SparkPlanGraphNode =
    new SparkPlanGraphNode(id, "", "", Nil)

  private def createCluster(id: Long, subNodes: Seq[SparkPlanGraphNode]): SparkPlanGraphCluster = {
    val subNodeBuffer = new ArrayBuffer[SparkPlanGraphNode]
    subNodeBuffer ++= subNodes
    new SparkPlanGraphCluster(id, "", "", subNodeBuffer, Nil)
  }
}

