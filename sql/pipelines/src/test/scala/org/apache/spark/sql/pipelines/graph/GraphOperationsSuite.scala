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
 * Tests for the flow/dataset reachability queries in [[GraphOperations]]. These exercise the
 * memoized `upstreamFlows` / `downstreamFlows` / `upstreamDatasets` against graphs whose expected
 * reachability is known by inspection, and verify that repeated calls are served from the cache.
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

  test("repeated queries are served from the memoization cache") {
    val session = spark
    import session.implicits._

    val graph = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerMaterializedView("b", query = readFlowFunc("a"))
      registerMaterializedView("c", query = readFlowFunc("b"))
    }.resolveToDataflowGraph()

    // Equal across calls, and the second call returns the identical cached instance.
    val up1 = graph.upstreamFlows(id("c"))
    val up2 = graph.upstreamFlows(id("c"))
    assert(up1 == Set(id("a"), id("b")))
    assert(up1 eq up2)

    val down1 = graph.downstreamFlows(id("a"))
    val down2 = graph.downstreamFlows(id("a"))
    assert(down1 == Set(id("b"), id("c")))
    assert(down1 eq down2)
  }
}
