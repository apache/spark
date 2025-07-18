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

package org.apache.spark.sql.connect.pipelines

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.PipelineEvent
import org.apache.spark.sql.AnalysisException

class PipelineEventStreamSuite extends SparkDeclarativePipelinesServerTest {
  test("expected events are streamed back to the client") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "a",
          datasetType = proto.DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)"))
        createTable(
          name = "b",
          datasetType = proto.DatasetType.TABLE,
          sql = Some("SELECT * FROM STREAM a"))
      }
      registerPipelineDatasets(pipeline)

      val capturedEvents = new ArrayBuffer[PipelineEvent]()
      withClient { client =>
        val startRunRequest = buildStartRunPlan(
          proto.PipelineCommand.StartRun.newBuilder().setDataflowGraphId(graphId).build())
        val responseIterator = client.execute(startRunRequest)
        while (responseIterator.hasNext) {
          val response = responseIterator.next()
          if (response.hasPipelineEventResult) {
            capturedEvents.append(response.getPipelineEventResult.getEvent)
          }
        }
        val expectedEventMessages = Set(
          "Flow spark_catalog.default.a is QUEUED",
          "Flow spark_catalog.default.b is QUEUED",
          "Flow spark_catalog.default.a is PLANNING",
          "Flow spark_catalog.default.a is STARTING",
          "Flow spark_catalog.default.a is RUNNING",
          "Flow spark_catalog.default.a has COMPLETED",
          "Flow spark_catalog.default.b is STARTING",
          "Flow spark_catalog.default.b is RUNNING",
          "Flow spark_catalog.default.b has COMPLETED",
          "Run is COMPLETED")
        expectedEventMessages.foreach { eventMessage =>
          assert(
            capturedEvents.exists(e => e.getMessage.contains(eventMessage)),
            s"Did not receive expected event: $eventMessage")
        }
      }
    }
  }

  test("flow resolution failure") {
    val dryOptions = Seq(true, false)

    dryOptions.foreach { dry =>
      withRawBlockingStub { implicit stub =>
        val graphId = createDataflowGraph
        val pipeline = new TestPipelineDefinition(graphId) {
          createTable(
            name = "a",
            datasetType = proto.DatasetType.MATERIALIZED_VIEW,
            sql = Some("SELECT * FROM unknown_table"))
          createTable(
            name = "b",
            datasetType = proto.DatasetType.TABLE,
            sql = Some("SELECT * FROM STREAM a"))
        }
        registerPipelineDatasets(pipeline)

        val capturedEvents = new ArrayBuffer[PipelineEvent]()
        withClient { client =>
          val startRunRequest = buildStartRunPlan(
            proto.PipelineCommand.StartRun
              .newBuilder()
              .setDataflowGraphId(graphId)
              .setDry(dry)
              .build())
          val ex = intercept[AnalysisException] {
            val responseIterator = client.execute(startRunRequest)
            while (responseIterator.hasNext) {
              val response = responseIterator.next()
              if (response.hasPipelineEventResult) {
                capturedEvents.append(response.getPipelineEventResult.getEvent)
              }
            }
          }
          // (?s) enables wildcard matching on newline characters
          val runFailureErrorMsg = "(?s).*Failed to resolve flows in the pipeline.*".r
          assert(runFailureErrorMsg.matches(ex.getMessage))
          val expectedLogPatterns = Set(
            "(?s).*Failed to resolve flow.*Failed to read dataset 'spark_catalog.default.a'.*".r,
            "(?s).*Failed to resolve flow.*[TABLE_OR_VIEW_NOT_FOUND].*".r)
          expectedLogPatterns.foreach { logPattern =>
            assert(
              capturedEvents.exists(e => logPattern.matches(e.getMessage)),
              s"Did not receive expected event matching pattern: $logPattern")
          }
          // Ensure that the error causing the run failure is not surfaced to the user twice
          assert(capturedEvents.forall(e => !runFailureErrorMsg.matches(e.getMessage)))
        }
      }
    }
  }

  test("successful dry run") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "a",
          datasetType = proto.DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)"))
        createTable(
          name = "b",
          datasetType = proto.DatasetType.TABLE,
          sql = Some("SELECT * FROM STREAM a"))
      }
      registerPipelineDatasets(pipeline)

      val capturedEvents = new ArrayBuffer[PipelineEvent]()
      withClient { client =>
        val startRunRequest = buildStartRunPlan(
          proto.PipelineCommand.StartRun
            .newBuilder()
            .setDataflowGraphId(graphId)
            .setDry(true)
            .build())
        val responseIterator = client.execute(startRunRequest)
        while (responseIterator.hasNext) {
          val response = responseIterator.next()
          if (response.hasPipelineEventResult) {
            capturedEvents.append(response.getPipelineEventResult.getEvent)
          }
        }
        val expectedEventMessages = Set("Run is COMPLETED")
        expectedEventMessages.foreach { eventMessage =>
          assert(
            capturedEvents.exists(e => e.getMessage.contains(eventMessage)),
            s"Did not receive expected event: $eventMessage")
        }
      }

      // No flows should be started in dry run mode
      capturedEvents.foreach { event =>
        assert(!event.getMessage.contains("is QUEUED"))
      }
    }
  }
}
