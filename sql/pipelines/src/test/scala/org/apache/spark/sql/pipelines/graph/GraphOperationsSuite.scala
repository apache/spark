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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the flow/dataset reachability queries in [[GraphOperations]]. These exercise
 * `upstreamFlows` / `downstreamFlows` / `upstreamDatasets` against graphs whose expected
 * reachability is known by inspection, and verify that repeated queries avoid re-traversal.
 */
class GraphOperationsSuite extends PipelineTest with SharedSparkSession {

  private def id(name: String): TableIdentifier = fullyQualifiedIdentifier(name)

  test("reachability over a diamond graph") {
    val session = spark
    import session.implicits._

    // a -> b, a -> c, {b, c} -> d, d -> e
    val graph = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerMaterializedView("b", query = readFlowFunc("a"))
      registerMaterializedView("c", query = readFlowFunc("a"))
      registerMaterializedView("d", query = sqlFlowFunc(spark, "SELECT * FROM b JOIN c USING (x)"))
      registerMaterializedView("e", query = readFlowFunc("d"))
    }.resolveToDataflowGraph()

    assert(graph.upstreamFlows(id("a")) == Set.empty)
    assert(graph.upstreamFlows(id("b")) == Set(id("a")))
    assert(graph.upstreamFlows(id("c")) == Set(id("a")))
    assert(graph.upstreamFlows(id("d")) == Set(id("a"), id("b"), id("c")))
    assert(graph.upstreamFlows(id("e")) == Set(id("a"), id("b"), id("c"), id("d")))

    assert(graph.downstreamFlows(id("a")) == Set(id("b"), id("c"), id("d"), id("e")))
    assert(graph.downstreamFlows(id("b")) == Set(id("d"), id("e")))
    assert(graph.downstreamFlows(id("c")) == Set(id("d"), id("e")))
    assert(graph.downstreamFlows(id("d")) == Set(id("e")))
    assert(graph.downstreamFlows(id("e")) == Set.empty)

    assert(graph.upstreamDatasets(id("a")) == Set.empty)
    assert(graph.upstreamDatasets(id("d")) == Set(id("a"), id("b"), id("c")))
    assert(graph.upstreamDatasets(id("e")) == Set(id("a"), id("b"), id("c"), id("d")))
  }

  test("reachability does not cross disconnected components") {
    val session = spark
    import session.implicits._

    // Two independent chains: a -> b and c -> d.
    val graph = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerMaterializedView("b", query = readFlowFunc("a"))
      registerMaterializedView("c", query = dfFlowFunc(Seq(4, 5, 6).toDF("x")))
      registerMaterializedView("d", query = readFlowFunc("c"))
    }.resolveToDataflowGraph()

    assert(graph.upstreamFlows(id("b")) == Set(id("a")))
    assert(graph.upstreamFlows(id("d")) == Set(id("c")))
    assert(graph.downstreamFlows(id("a")) == Set(id("b")))
    assert(graph.downstreamFlows(id("c")) == Set(id("d")))
  }

  test("repeated reachability queries do not re-traverse the graph") {
    val session = spark
    import session.implicits._

    val resolved = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerMaterializedView("b", query = readFlowFunc("a"))
      registerMaterializedView("c", query = readFlowFunc("b"))
    }.resolveToDataflowGraph()
    val graph = new CountingGraph(resolved)

    assert(graph.upstreamFlows(id("c")) == Set(id("a"), id("b")))
    assert(graph.downstreamFlows(id("a")) == Set(id("b"), id("c")))
    val traversalsAfterFirstPass = graph.dfsCalls
    assert(traversalsAfterFirstPass > 0, "expected the first queries to traverse the graph")

    // The same queries are now memoized, so they return equal results without re-traversing.
    assert(graph.upstreamFlows(id("c")) == Set(id("a"), id("b")))
    assert(graph.downstreamFlows(id("a")) == Set(id("b"), id("c")))
    assert(graph.dfsCalls == traversalsAfterFirstPass, "repeated queries should not re-traverse")
  }

  /** A graph that counts `dfsInternal` traversals, to assert reachability queries are memoized. */
  private class CountingGraph(graph: DataflowGraph)
      extends DataflowGraph(graph.flows, graph.tables, graph.sinks, graph.views) {
    var dfsCalls: Int = 0

    override def dfsInternal(
        startDestination: TableIdentifier,
        downstream: Boolean,
        stopAtMaterializationPoints: Boolean): Set[TableIdentifier] = {
      dfsCalls += 1
      super.dfsInternal(startDestination, downstream, stopAtMaterializationPoints)
    }
  }
}
