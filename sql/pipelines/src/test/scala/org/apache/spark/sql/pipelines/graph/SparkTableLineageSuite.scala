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

import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for SPARK-57352: pipeline lineage detection for resolved table references.
 */
class SparkTableLineageSuite extends ExecutionTest with SharedSparkSession {

  test("SPARK-57352: requestedInputs flows into inputs for DAG ordering") {
    val session = spark
    import session.implicits._

    val pipelineDef = new TestGraphRegistrationContext(spark) {
      // Bronze: defined in the pipeline
      registerMaterializedView("bronze_real", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))

      // Silver: uses readFlowFunc which goes through the normal UnresolvedRelation path
      // This confirms requestedInputs is populated and now flows into inputs
      registerMaterializedView("silver_normal", query = readFlowFunc("bronze_real"))
    }

    val graph = pipelineDef.resolveToDataflowGraph()
    val silverFlow = graph.flows.find(
      f => f.identifier.table.contains("silver_normal")).get
        .asInstanceOf[ResolutionCompletedFlow]

    // requestedInputs is populated by readGraphInput and now included in inputs
    val silverInputs = silverFlow.funcResult.inputs
    val requestedInputs = silverFlow.funcResult.requestedInputs

    assert(requestedInputs.nonEmpty,
      s"requestedInputs should contain bronze_real but got: $requestedInputs")
    assert(silverInputs.nonEmpty,
      s"inputs should now include requestedInputs but got: $silverInputs")
    // Verify bronze_real is in inputs (used by DAG scheduler)
    assert(silverInputs.exists(_.table == "bronze_real"),
      s"inputs should contain bronze_real for DAG ordering but got: $silverInputs")
  }

  test("readFlowFunc correctly captures lineage (control test)") {
    val session = spark
    import session.implicits._

    val pipelineDef = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("bronze2", query = dfFlowFunc(Seq(1, 2).toDF("x")))
      registerMaterializedView("silver2", query = readFlowFunc("bronze2"))
    }

    val graph = pipelineDef.resolveToDataflowGraph()
    val silverFlow = graph.flows.find(
      f => f.identifier.table.contains("silver2")).get
        .asInstanceOf[ResolutionCompletedFlow]

    val silverInputs = silverFlow.funcResult.inputs
    assert(silverInputs.nonEmpty,
      s"Control test: readFlowFunc should capture bronze2 as input but got: $silverInputs")
  }
}
