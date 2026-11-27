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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.util.JsonProtocol

class SparkPlanGraphSuite extends SparkFunSuite {
  test("SPARK-47503: name of a node should be escaped even if there is no metrics") {
    val planGraphNode = new SparkPlanGraphNode(
      id = 24,
      name = "Scan JDBCRelation(\"test-schema\".tickets) [numPartitions=1]",
      desc = "Scan JDBCRelation(\"test-schema\".tickets) [numPartitions=1] " +
        "[ticket_no#0] PushedFilters: [], ReadSchema: struct<ticket_no:string>",
      metrics = List(
        SQLPlanMetric(
          name = "number of output rows",
          accumulatorId = 75,
          metricType = "sum"
        ),
        SQLPlanMetric(
          name = "JDBC query execution time",
          accumulatorId = 35,
          metricType = "nsTiming")))
    val dotNode = planGraphNode.makeDotNode(Map.empty[Long, String])
    val expectedDotNode = "  24 [id=\"node24\" label=\"" +
      "Scan JDBCRelation(\\\"test-schema\\\".tickets) [numPartitions=1]\" " +
      "tooltip=\"Scan JDBCRelation(\\\"test-schema\\\".tickets) [numPartitions=1] [ticket_no#0] " +
      "PushedFilters: [], ReadSchema: struct<ticket_no:string>\"];"

    assertResult(expectedDotNode)(dotNode)
  }

  test("SPARK-55786: makeNodeDetailsJson uses cluster prefix and includes children") {
    import scala.collection.mutable
    val childNode = new SparkPlanGraphNode(
      id = 1, name = "HashAggregate", desc = "HashAggregate(keys=[])",
      metrics = Seq(SQLPlanMetric("peak memory", 10, "size")))
    val cluster = new SparkPlanGraphCluster(
      id = 2, name = "WholeStageCodegen (1)", desc = "WholeStageCodegen (1)",
      nodes = mutable.ArrayBuffer(childNode),
      metrics = Seq(SQLPlanMetric("duration", 20, "timing")))
    val graph = SparkPlanGraph(Seq(cluster), Seq.empty)
    val json = graph.makeNodeDetailsJson(Map(10L -> "256.0 KiB", 20L -> "5 ms"))
    assert(json.contains("\"cluster2\""), "cluster should use cluster prefix")
    assert(json.contains("\"node1\""), "child should use node prefix")
    assert(json.contains("\"children\":[\"node1\"]"),
      "cluster should list children")
    assert(!json.contains("\"node2\""),
      "cluster should not use node prefix")
    assert(json.contains("\"desc\":\"HashAggregate(keys=[])\""),
      "node should include desc field")
    assert(json.contains("\"desc\":\"WholeStageCodegen (1)\""),
      "cluster should include desc field")
  }

  test("SPARK-56331: truncateLabel returns short labels unchanged") {
    assert(SparkPlanGraph.truncateLabel("Sort") === "Sort")
    assert(SparkPlanGraph.truncateLabel("Filter") === "Filter")
    assert(SparkPlanGraph.truncateLabel("HashAggregate") === "HashAggregate")
    assert(SparkPlanGraph.truncateLabel("SortMergeJoin Inner") === "SortMergeJoin Inner")
    assert(SparkPlanGraph.truncateLabel(null) === null)
    assert(SparkPlanGraph.truncateLabel("") === "")
  }

  test("SPARK-56331: truncateLabel does not truncate long operator names") {
    // Long operator names without qualified paths should NOT be truncated
    val insertCmd = "Execute InsertIntoHadoopFsRelationCommand"
    assert(SparkPlanGraph.truncateLabel(insertCmd) === insertCmd)
    val longOp = "BroadcastHashJoin Inner BuildRight with very long hint"
    assert(SparkPlanGraph.truncateLabel(longOp) === longOp)
  }

  test("SPARK-56331: truncateLabel truncates qualified catalog paths") {
    val exactly36 = "Scan spark_catalog.default.store_sal"
    assert(SparkPlanGraph.truncateLabel(exactly36) === exactly36)

    val longScan =
      "Scan parquet spark_catalog.very_long_schema_name.catalog_sales"
    val truncated = SparkPlanGraph.truncateLabel(longScan)
    assert(truncated.length <= 36,
      s"Truncated label should be <= 36 chars but was ${truncated.length}")
    assert(truncated.startsWith("Scan parquet spa"),
      "Should preserve operator prefix")
    assert(truncated.endsWith("catalog_sales"),
      "Should preserve table name at the end")
    assert(truncated.contains("..."),
      "Should contain ellipsis in the middle")
  }

  test("SPARK-56331: DOT label truncated, tooltip keeps full name") {
    val longName = "ScanTransformer parquet " +
      "spark_catalog.abcdefghijklmnopqrstuvwxyz.store_sales"
    val node = new SparkPlanGraphNode(
      id = 1, name = longName, desc = longName,
      metrics = Seq.empty)
    val dot = node.makeDotNode(Map.empty)
    assert(dot.contains("..."), "DOT label should contain ellipsis")
    assert(dot.contains("tooltip=\"" + longName + "\""),
      "Tooltip should contain the full name")
  }

  test("SPARK-56331: JSON details keep full name") {
    val longName = "ScanTransformer parquet " +
      "spark_catalog.abcdefghijklmnopqrstuvwxyz.store_sales"
    val node = new SparkPlanGraphNode(
      id = 1, name = longName, desc = longName,
      metrics = Seq.empty)
    val graph = SparkPlanGraph(Seq(node), Seq.empty)
    val json = graph.makeNodeDetailsJson(Map.empty)
    assert(json.contains(longName),
      "JSON details should contain the full untruncated name")
  }

  private def graphOf(info: SparkPlanInfo): SparkPlanGraph = {
    val event = SparkListenerSQLExecutionStart(17L, Some(17L), "DPP graph", "", "", info, 1L)
    val json = JsonProtocol.sparkEventToJsonString(event)
    val decoded = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[SparkListenerSQLExecutionStart].sparkPlanInfo
    SparkPlanGraph(decoded)
  }

  private val versionKey = "spark.sql.dpp.planInfo.version"
  private val idKey = "spark.sql.dpp.planInfo.id"
  private val refKey = "spark.sql.dpp.planInfo.ref"

  test("SPARK-48486: table cache and result query stages remain transparent") {
    Seq("TableCacheQueryStage", "ResultQueryStage").foreach { stage =>
      val graph = graphOf(plan("Root", Seq(plan(stage, Seq(plan("Source"))))))
      val nodes = visible(graph).map(node => node.name -> node.id).toMap
      assert(nodes.keySet == Set("Root", "Source"))
      assert(graph.edges.toSet == Set(SparkPlanGraphEdge(nodes("Source"), nodes("Root"))))
    }
  }

  private def plan(
      name: String,
      children: Seq[SparkPlanInfo] = Nil,
      id: Option[String] = None,
      metrics: Seq[SQLMetricInfo] = Nil): SparkPlanInfo = {
    new SparkPlanInfo(name, name, children,
      id.map(value => Map(versionKey -> "1", idKey -> value)).getOrElse(Map.empty), metrics)
  }

  private def ref(id: String): SparkPlanInfo = {
    new SparkPlanInfo("DPPSharedPlanReference", s"DPP reference $id", Nil,
      Map(versionKey -> "1", refKey -> id), Nil)
  }

  private def visible(graph: SparkPlanGraph): Seq[SparkPlanGraphNode] = {
    graph.allNodes.filterNot(_.isInstanceOf[SparkPlanGraphCluster]).toSeq
  }

  private def assertRealEndpoints(graph: SparkPlanGraph): Unit = {
    val ids = visible(graph).map(_.id).toSet
    graph.edges.foreach { edge =>
      assert(ids.contains(edge.fromId))
      assert(ids.contains(edge.toId))
    }
    val dot = graph.makeDotFile(Map.empty)
    val nodeMap = graph.allNodes.map(n => n.id -> n).toMap
    graph.edges.foreach(edge => assert(dot.contains(edge.makeDotEdge(nodeMap, Map.empty))))
    assert(!dot.contains("DPPSharedPlanReference"))
  }

  test("SPARK-48486: DPP forward references preserve every consumer edge and producer metrics") {
    val metric = new SQLMetricInfo("rows", 71L, "sum")
    val root = plan("Root", Seq(
      plan("Consumer1", Seq(ref("p"))),
      plan("Consumer2", Seq(ref("p"))),
      plan("Producer", id = Some("p"), metrics = Seq(metric))))
    val graph = graphOf(root)
    val byName = visible(graph).map(node => node.name -> node).toMap
    assert(visible(graph).size == 4)
    assert(graph.edges.toSet == Set(
      SparkPlanGraphEdge(byName("Consumer1").id, byName("Root").id),
      SparkPlanGraphEdge(byName("Consumer2").id, byName("Root").id),
      SparkPlanGraphEdge(byName("Producer").id, byName("Root").id),
      SparkPlanGraphEdge(byName("Producer").id, byName("Consumer1").id),
      SparkPlanGraphEdge(byName("Producer").id, byName("Consumer2").id)))
    assert(visible(graph).flatMap(_.metrics).map(_.accumulatorId) == Seq(71L))
    assertRealEndpoints(graph)
  }

  test("SPARK-48486: DPP wrapper aliases resolve to codegen body and retain metrics") {
    val duration = new SQLMetricInfo("duration", 80L, "timing")
    val rows = new SQLMetricInfo("rows", 81L, "sum")
    val body = plan("Body", id = Some("body"), metrics = Seq(rows))
    val stage = plan("WholeStageCodegen (1)", Seq(body), Some("cluster"), Seq(duration))
    val queryStage = plan("BroadcastQueryStage", Seq(stage), Some("stage"))
    val adapter = plan("InputAdapter", Seq(queryStage), Some("adapter"))
    val root = plan("Root", Seq(
      plan("Consumer", Seq(ref("adapter"), ref("stage"), ref("cluster"), ref("body"))),
      adapter))
    val graph = graphOf(root)
    val bodyNode = visible(graph).find(_.name == "Body").get
    val consumer = visible(graph).find(_.name == "Consumer").get
    assert(graph.edges.count(_ == SparkPlanGraphEdge(bodyNode.id, consumer.id)) == 4)
    assert(graph.nodes.collect { case c: SparkPlanGraphCluster => c }.size == 1)
    assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId).sorted == Seq(80L, 81L))
    assertRealEndpoints(graph)
  }

  Seq("Subquery", "Exchange").foreach { name =>
    test(s"SPARK-48486: DPP $name reuse binds deep skipped aliases") {
      val metric = new SQLMetricInfo("rows", 91L, "sum")
      val firstLeaf = plan("Leaf", id = Some("leaf1"), metrics = Seq(metric))
      val first = plan(name,
        Seq(plan("Middle", Seq(firstLeaf), Some("middle1"))), Some("first"))
      val second = plan(name,
        Seq(plan("Middle", Seq(ref("leaf1")), Some("middle2"))), Some("second"))
      assert(!(first eq second))
      assert(first != second) // Old raw SparkPlanInfo equality cannot resolve the ref leaf.
      val reused = if (name == "Exchange") plan("ReusedExchange", Seq(second)) else second
      val root = plan("Root", Seq(first, reused,
        plan("Consumer", Seq(ref("second"), ref("middle2"), ref("leaf1")))))
      val graph = graphOf(root)
      val byName = visible(graph).map(node => node.name -> node).toMap
      assert(visible(graph).count(_.name == name) == 1)
      assert(visible(graph).count(_.name == "Middle") == 1)
      assert(visible(graph).count(_.name == "Leaf") == 1)
      assert(graph.edges.count(_ ==
        SparkPlanGraphEdge(byName(name).id, byName("Root").id)) == 2)
      Seq(name, "Middle", "Leaf").foreach { producer =>
        assert(graph.edges.contains(
          SparkPlanGraphEdge(byName(producer).id, byName("Consumer").id)))
      }
      assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId) == Seq(91L))
      assertRealEndpoints(graph)
    }

    test(s"SPARK-48486: DPP $name skipped details bind to existing ref descendants") {
      val producer = plan("Leaf", id = Some("leaf1"))
      val first = plan(name, Seq(plan("Middle", Seq(ref("leaf1")), Some("middle1"))))
      val second = plan(name, Seq(plan("Middle",
        Seq(plan("Leaf", id = Some("leaf2"))), Some("middle2"))), Some("second"))
      val reused = if (name == "Exchange") plan("ReusedExchange", Seq(second)) else second
      val graph = graphOf(plan("Root", Seq(producer, first, reused,
        plan("Consumer", Seq(ref("leaf2"), ref("middle2"), ref("second"))))))
      val byName = visible(graph).map(node => node.name -> node).toMap
      assert(visible(graph).count(_.name == "Leaf") == 1)
      assert(visible(graph).count(_.name == name) == 1)
      Seq("Leaf", "Middle", name).foreach { producerName =>
        assert(graph.edges.contains(
          SparkPlanGraphEdge(byName(producerName).id, byName("Consumer").id)))
      }
      assertRealEndpoints(graph)
    }
  }

  test("SPARK-48486: DPP distinct detail IDs do not replace legacy semantic comparison") {
    val first = plan("Subquery", Seq(plan("Left")), Some("a"))
    val second = plan("Subquery", Seq(plan("Right")), Some("b"))
    val graph = graphOf(plan("Root", Seq(first, second, ref("a"), ref("b"))))
    assert(visible(graph).count(_.name == "Subquery") == 2)
    assertRealEndpoints(graph)
  }

  test("SPARK-48486: DPP root detail does not produce a consumer edge") {
    val graph = graphOf(plan("InputAdapter",
      Seq(plan("Leaf", id = Some("leaf"))), Some("root")))
    assert(visible(graph).map(_.name) == Seq("Leaf"))
    assert(graph.edges.isEmpty)
    assertRealEndpoints(graph)
  }

  test("SPARK-48486: DPP malformed, missing, conflicting and cyclic references fail explicitly") {
    val bad = Seq(
      ref("missing"),
      plan("Root", Seq(plan("Consumer", Seq(ref("missing"))))),
      new SparkPlanInfo("DPPSharedPlanReference", "bad", Nil, Map.empty, Nil),
      new SparkPlanInfo("Leaf", "Leaf", Nil, Map(refKey -> "missing"), Nil),
      new SparkPlanInfo("Leaf", "Leaf", Nil, Map(versionKey -> "1"), Nil),
      plan("Root", Seq(plan("A", id = Some("same")), plan("B", id = Some("same")))),
      plan("Root", Seq(ref("self")), Some("self")),
      plan("Root", Seq(plan("A", Seq(ref("b")), Some("a")),
        plan("B", Seq(ref("a")), Some("b")))),
      new SparkPlanInfo("DPPSharedPlanReference", "bad", Seq(plan("Child")),
        Map(versionKey -> "1", refKey -> "id"), Nil),
      new SparkPlanInfo("DPPSharedPlanReference", "bad", Nil,
        Map(versionKey -> "1", refKey -> "id"), Seq(new SQLMetricInfo("rows", 1L, "sum"))),
      new SparkPlanInfo("Leaf", "Leaf", Nil, Map(versionKey -> "2", idKey -> "id"), Nil),
      new SparkPlanInfo("Leaf", "Leaf", Nil, Map(idKey -> "id"), Nil),
      new SparkPlanInfo("Leaf", "Leaf", Nil,
        Map(versionKey -> "1", idKey -> "id", refKey -> "id"), Nil))
    bad.foreach { root =>
      val error = intercept[IllegalStateException](graphOf(root))
      assert(error.getMessage.startsWith("Invalid DPP SparkPlanInfo:"))
    }
  }

  test("SPARK-48486: legacy graphs preserve reuse, codegen layout and metrics") {
    val leaf1 = plan("Leaf")
    val leaf2 = plan("Leaf")
    val original = plan("Exchange", Seq(leaf1))
    val equivalent = plan("Exchange", Seq(leaf2))
    val subquery1 = plan("Subquery", Seq(plan("SubqueryLeaf")))
    val subquery2 = plan("Subquery", Seq(plan("SubqueryLeaf")))
    val graph = graphOf(plan("Root", Seq(
      plan("WholeStageCodegen (1)", Seq(plan("InputAdapter", Seq(original)))),
      plan("ShuffleQueryStage", Seq(equivalent)),
      plan("ReusedExchange", Seq(equivalent)),
      plan("ReusedSubquery", Seq(subquery1)), subquery2)))
    assert(visible(graph).map(_.name).sorted ==
      Seq("Root", "Exchange", "Leaf", "Subquery", "SubqueryLeaf").sorted)
    assert(graph.edges.size == 6)
    assertRealEndpoints(graph)
  }


  private def assertNamedEdges(
      graph: SparkPlanGraph,
      expected: Seq[(String, String)]): Unit = {
    val names = visible(graph).map(node => node.id -> node.name).toMap
    val actual = graph.edges.map(edge => names(edge.fromId) -> names(edge.toId))
    def counts(edges: collection.Seq[(String, String)]): Map[(String, String), Int] =
      edges.groupBy(identity).map { case (edge, copies) => edge -> copies.size }.toMap
    assert(counts(actual) == counts(expected))
    assertRealEndpoints(graph)
  }

  Seq("Subquery", "ReusedExchange", "ShuffleQueryStage").foreach { reuseKind =>
    val producerName = if (reuseKind == "Subquery") "Subquery" else "Exchange"
    def reuse(child: SparkPlanInfo): SparkPlanInfo = {
      if (reuseKind == "Subquery") child else plan(reuseKind, Seq(child))
    }

    test(s"SPARK-48486: $reuseKind materializes the sole skipped deep detail") {
      val rows = new SQLMetricInfo("anchor rows", 101L, "sum")
      val tailRows = new SQLMetricInfo("tail rows", 102L, "sum")
      val tail = plan("Tail", id = Some("tail"), metrics = Seq(tailRows))
      val anchor = plan("Anchor", Seq(plan("Middle", Seq(plan("Deep", Seq(ref("tail")))))),
        Some("anchor"), Seq(rows))
      val first = plan(producerName, Seq(ref("anchor"), ref("tail")))
      val second = plan(producerName, Seq(anchor, tail))
      val graph = graphOf(plan("Root", Seq(first, reuse(second),
        plan("Consumer", Seq(ref("anchor"), ref("anchor"), ref("tail"))))))
      assert(visible(graph).map(_.name).sorted ==
        Seq("Root", producerName, "Anchor", "Middle", "Deep", "Tail", "Consumer").sorted)
      assertNamedEdges(graph, Seq(
        producerName -> "Root", producerName -> "Root", "Consumer" -> "Root",
        "Anchor" -> producerName, "Tail" -> producerName, "Middle" -> "Anchor",
        "Deep" -> "Middle", "Tail" -> "Deep", "Anchor" -> "Consumer",
        "Anchor" -> "Consumer", "Tail" -> "Consumer"))
      assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId).sorted == Seq(101L, 102L))
    }

    test(s"SPARK-48486: $reuseKind early materialization preserves later detail consumers") {
      val anchor = plan("Anchor", Seq(plan("Middle", Seq(plan("Deep", Seq(ref("tail")))))),
        Some("anchor"), Seq(new SQLMetricInfo("rows", 111L, "sum")))
      val tail = plan("Tail", id = Some("tail"),
        metrics = Seq(new SQLMetricInfo("rows", 112L, "sum")))
      val first = plan(producerName, Seq(ref("anchor")))
      val second = plan(producerName, Seq(ref("anchor")))
      val graph = graphOf(plan("Root", Seq(first, reuse(second),
        plan("LaterAnchor", Seq(anchor)), plan("LaterTail", Seq(tail)))))
      assertNamedEdges(graph, Seq(producerName -> "Root", producerName -> "Root",
        "LaterAnchor" -> "Root", "LaterTail" -> "Root", "Anchor" -> "LaterAnchor",
        "Tail" -> "LaterTail", "Anchor" -> producerName, "Middle" -> "Anchor",
        "Deep" -> "Middle", "Tail" -> "Deep"))
      assert(visible(graph).count(_.name == "Anchor") == 1)
      assert(visible(graph).count(_.name == "Tail") == 1)
      assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId).sorted == Seq(111L, 112L))
    }
  }

  test("SPARK-48486: missing anchor uses first reference codegen owner") {
    val anchor = plan("Anchor", Seq(plan("Middle", Seq(plan("Deep")))), Some("anchor"),
      Seq(new SQLMetricInfo("rows", 121L, "sum")))
    val cluster = plan("WholeStageCodegen (1)", Seq(plan("ClusterConsumer", Seq(ref("anchor")))),
      metrics = Seq(new SQLMetricInfo("duration", 122L, "timing")))
    val first = plan("Subquery", Seq(ref("anchor")))
    val second = plan("Subquery", Seq(anchor))
    val graph = graphOf(plan("Root", Seq(cluster, first, second)))
    val codegen = graph.nodes.collect { case c: SparkPlanGraphCluster => c }.head
    assert(codegen.nodes.map(_.name).toSet == Set("ClusterConsumer", "Anchor", "Middle", "Deep"))
    assertNamedEdges(graph, Seq("ClusterConsumer" -> "Root", "Subquery" -> "Root",
      "Subquery" -> "Root", "Anchor" -> "ClusterConsumer", "Anchor" -> "Subquery",
      "Middle" -> "Anchor", "Deep" -> "Middle"))
    assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId).sorted == Seq(121L, 122L))
  }

  test("SPARK-48486: identified Subquery leaves codegen before entering detail state") {
    val detail = plan("Subquery", Seq(plan("Leaf")), Some("subquery"),
      Seq(new SQLMetricInfo("rows", 131L, "sum")))
    val graph = graphOf(plan("Root", Seq(
      plan("WholeStageCodegen (1)", Seq(detail)), plan("Consumer", Seq(ref("subquery"))))))
    val cluster = graph.nodes.collect { case c: SparkPlanGraphCluster => c }.head
    assert(cluster.nodes.isEmpty)
    assert(graph.nodes.exists(_.name == "Subquery"))
    assertNamedEdges(graph, Seq("Subquery" -> "Root", "Leaf" -> "Subquery",
      "Consumer" -> "Root", "Subquery" -> "Consumer"))
    assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId) == Seq(131L))
  }

  test("SPARK-48486: missing WholeStage detail keeps one cluster and body representative") {
    val body = plan("Body", Seq(plan("Middle", Seq(plan("Deep")))),
      metrics = Seq(new SQLMetricInfo("rows", 141L, "sum")))
    val stage = plan("WholeStageCodegen (2)", Seq(body), Some("stage"),
      Seq(new SQLMetricInfo("duration", 142L, "timing")))
    val first = plan("Subquery", Seq(ref("stage")))
    val second = plan("Subquery", Seq(ref("stage")))
    val graph = graphOf(plan("Root", Seq(first, second,
      plan("LaterStage", Seq(stage)), plan("Consumer", Seq(ref("stage"), ref("stage"))))))
    val clusters = graph.nodes.collect { case c: SparkPlanGraphCluster => c }
    assert(clusters.size == 1)
    assert(clusters.head.nodes.map(_.name).toSet == Set("Body", "Middle", "Deep"))
    assertNamedEdges(graph, Seq("Subquery" -> "Root", "Subquery" -> "Root",
      "Body" -> "Subquery", "LaterStage" -> "Root", "Body" -> "LaterStage",
      "Middle" -> "Body", "Deep" -> "Middle", "Consumer" -> "Root",
      "Body" -> "Consumer", "Body" -> "Consumer"))
    assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId).sorted == Seq(141L, 142L))
  }

}
