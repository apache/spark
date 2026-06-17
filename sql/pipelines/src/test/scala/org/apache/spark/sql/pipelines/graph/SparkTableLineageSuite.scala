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
 * Reproduces SPARK-57352: When a flow references a pipeline dataset via a resolved
 * table relation (e.g., from spark.table() in classic mode), the pipeline engine
 * should still infer the dependency.
 */
class SparkTableLineageSuite extends ExecutionTest with SharedSparkSession {

  test("SPARK-57352: resolved table reference in plan captures lineage after fix") {
    val session = spark
    import session.implicits._

    // Create a real table in the catalog so spark.table() doesn't throw
    Seq(1, 2, 3).toDF("x").write.mode("overwrite").saveAsTable("bronze_real")

    try {
      val pipelineDef = new TestGraphRegistrationContext(spark) {
        // Bronze: defined in the pipeline
        registerMaterializedView("bronze_real", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))

        // Silver: reads bronze_real via a RESOLVED plan (simulating spark.table())
        // The plan has no UnresolvedRelation -- it's already resolved against the catalog
        val resolvedPlan = session.table("bronze_real").queryExecution.analyzed
        registerMaterializedView("silver_resolved", query =
          FlowAnalysis.createFlowFunctionFromLogicalPlan(resolvedPlan))
      }

      val graph = pipelineDef.resolveToDataflowGraph()
      val silverFlow = graph.flows.find(
        f => f.identifier.table.contains("silver_resolved")).get
          .asInstanceOf[ResolutionCompletedFlow]

      // After fix: the post-resolution scan should detect bronze_real as an input
      val silverInputs = silverFlow.funcResult.inputs
      val externalInputs = silverFlow.funcResult.usedExternalInputs

      // The fix records it as an external input (since it was resolved outside the pipeline)
      assert(externalInputs.nonEmpty || silverInputs.nonEmpty,
        s"SPARK-57352 NOT FIXED: resolved table 'bronze_real' not captured. " +
          s"inputs=$silverInputs, externalInputs=$externalInputs")
    } finally {
      spark.sql("DROP TABLE IF EXISTS bronze_real")
    }
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
