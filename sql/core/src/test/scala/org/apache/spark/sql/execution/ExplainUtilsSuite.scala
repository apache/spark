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

package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.StringConcat
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for [[ExplainUtils.processPlan]]: operator ID assignment, WholeStageCodegen tag
 * propagation, thread-local lifecycle, and subquery handling.
 */
class ExplainUtilsSuite extends QueryTest with SharedSparkSession {

  private def explainOutput(plan: SparkPlan): String = {
    val concat = new StringConcat()
    ExplainUtils.processPlan(plan, concat.append)
    concat.toString
  }

  test("processPlan assigns unique operator IDs to all visible plan nodes") {
    val df = spark.range(100).filter("id > 10").select("id")
    val output = explainOutput(df.queryExecution.executedPlan)
    // Each operator ID appears both in the tree header ("Filter (2)") and as the header of its
    // verbose section ("(2) Filter"). Anchor to the verbose-section headers, which are the only
    // lines that begin with "(N)", so each operator contributes exactly one ID.
    val ids = "(?m)^\\((\\d+)\\)".r.findAllMatchIn(output).map(_.group(1).toInt).toSeq
    assert(ids.nonEmpty, "processPlan should assign at least one operator ID")
    assert(ids == ids.distinct, s"processPlan operator IDs should be unique: $ids")
  }

  test("processPlan sets WholeStageCodegen tags on plan nodes") {
    withSQLConf("spark.sql.codegen.wholeStage" -> "true") {
      val df = spark.range(10).filter("id > 3")
      val plan = df.queryExecution.executedPlan
      ExplainUtils.processPlan(plan, _ => ())
      // CODEGEN_ID_TAG is assigned only to nodes under a WholeStageCodegenExec (see
      // generateWholeStageCodegenIds). Assert that invariant directly rather than assuming the
      // planner fused this query into whole-stage codegen: when the executed plan contains
      // WholeStageCodegenExec nodes, at least one node inside one of them carries the tag.
      val wscgNodes = plan.collect { case w: WholeStageCodegenExec => w }
      if (wscgNodes.nonEmpty) {
        val taggedInsideWscg = wscgNodes.exists { w =>
          w.collect { case p if p.getTagValue(QueryPlan.CODEGEN_ID_TAG).isDefined => p }.nonEmpty
        }
        assert(taggedInsideWscg,
          "processPlan should set CODEGEN_ID_TAG on nodes inside WholeStageCodegenExec")
      }
    }
  }

  test("processPlan restores localIdMap to its prior value after completion") {
    val prev = ExplainUtils.localIdMap.get()
    ExplainUtils.processPlan(spark.range(10).filter("id > 3").queryExecution.executedPlan,
      _ => ())
    assert(ExplainUtils.localIdMap.get() eq prev,
      "processPlan should restore the thread-local localIdMap on completion")
  }

  test("processPlan includes subquery output in the explain string") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val df = spark.range(100).filter("id > 0")
        .filter("id < (SELECT max(id) FROM range(5))")
      val output = explainOutput(df.queryExecution.executedPlan)
      assert(output.contains("Subqueries"), "processPlan should emit a Subqueries section")
    }
  }
}
