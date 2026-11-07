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

package org.apache.spark.sql.pipelines

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.pipelines.PipelineExecutionMetadata._
import org.apache.spark.sql.test.SharedSparkSession

class PipelineExecutionMetadataSuite extends QueryTest with SharedSparkSession {

  test("flow execution metadata names and tag format") {
    assert(FLOW_IDENTIFIER_PROPERTY == "spark.sql.pipelines.flow.identifier")
    assert(FLOW_EXECUTION_ID_PROPERTY == "spark.sql.pipelines.flow.executionId")
    assert(
      flowExecutionIdTag("execution-id") ==
        "spark.sql.pipelines.flow.executionId:execution-id")
  }

  test("flow execution metadata is scoped and restores previous state") {
    val sc = spark.sparkContext
    val previousIdentifier = "previous-flow"
    val previousExecutionId = "previous-execution"
    val unrelatedTag = "unrelated-tag"
    val executionId = "execution-id"
    val executionTag = flowExecutionIdTag(executionId)

    sc.setLocalProperty(FLOW_IDENTIFIER_PROPERTY, previousIdentifier)
    sc.setLocalProperty(FLOW_EXECUTION_ID_PROPERTY, previousExecutionId)
    sc.addJobTag(unrelatedTag)
    sc.addJobTag(executionTag)

    try {
      withFlowExecutionMetadata(sc, "`catalog`.`schema`.`target`", executionId) {
        assert(sc.getLocalProperty(FLOW_IDENTIFIER_PROPERTY) ==
          "`catalog`.`schema`.`target`")
        assert(sc.getLocalProperty(FLOW_EXECUTION_ID_PROPERTY) == executionId)
        assert(sc.getJobTags().contains(executionTag))
        assert(sc.getJobTags().contains(unrelatedTag))
      }

      assert(sc.getLocalProperty(FLOW_IDENTIFIER_PROPERTY) == previousIdentifier)
      assert(sc.getLocalProperty(FLOW_EXECUTION_ID_PROPERTY) == previousExecutionId)
      assert(sc.getJobTags().contains(executionTag))
      assert(sc.getJobTags().contains(unrelatedTag))
    } finally {
      sc.setLocalProperty(FLOW_IDENTIFIER_PROPERTY, null)
      sc.setLocalProperty(FLOW_EXECUTION_ID_PROPERTY, null)
      sc.removeJobTag(executionTag)
      sc.removeJobTag(unrelatedTag)
    }
  }

  test("flow execution metadata restores previous state after failure") {
    val sc = spark.sparkContext
    val unrelatedTag = "unrelated-tag"
    val executionId = "execution-id"

    sc.addJobTag(unrelatedTag)
    try {
      val error = intercept[SparkException] {
        withFlowExecutionMetadata(sc, "`target`", executionId) {
          throw new SparkException("expected failure")
        }
      }
      assert(error.getMessage == "expected failure")
      assert(sc.getLocalProperty(FLOW_IDENTIFIER_PROPERTY) == null)
      assert(sc.getLocalProperty(FLOW_EXECUTION_ID_PROPERTY) == null)
      assert(!sc.getJobTags().contains(flowExecutionIdTag(executionId)))
      assert(sc.getJobTags().contains(unrelatedTag))
    } finally {
      sc.removeJobTag(unrelatedTag)
    }
  }
}
