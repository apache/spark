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

package org.apache.spark.sql.pipelines.logging

import java.sql.Timestamp

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.pipelines.common.FlowStatus
import org.apache.spark.sql.pipelines.graph.QueryOrigin

class ConstructPipelineEventSuite extends SparkFunSuite with BeforeAndAfterEach {
  test("Basic event construction") {
    val ts = new Timestamp(1747338049615L)
    val event = ConstructPipelineEvent(
      origin = PipelineEventOrigin(
        datasetName = Some("dataset"),
        flowName = Some("flow"),
        sourceCodeLocation = Some(
          QueryOrigin(
            filePath = Some("path"),
            line = None,
            startPosition = None
          )
        )
      ),
      level = EventLevel.INFO,
      message = "Flow 'b' has failed",
      details = FlowProgress(FlowStatus.FAILED),
      eventTimestamp = Some(ts)
    )
    assert(event.origin.datasetName.contains("dataset"))
    assert(event.origin.flowName.contains("flow"))
    assert(event.origin.sourceCodeLocation.get.filePath.contains("path"))
    assert(event.level == EventLevel.INFO)
    assert(event.message == "Flow 'b' has failed")
    assert(event.details.asInstanceOf[FlowProgress].status == FlowStatus.FAILED)
    assert(event.timestamp == ts)
  }

  test("basic flow progress event has expected fields set") {
    val event = ConstructPipelineEvent(
      origin =
        PipelineEventOrigin(flowName = Option("a"), datasetName = None, sourceCodeLocation = None),
      message = "Flow 'a' has completed",
      details = FlowProgress(FlowStatus.COMPLETED),
      level = EventLevel.INFO
    )
    assert(event.message == "Flow 'a' has completed")
    assert(event.details.isInstanceOf[FlowProgress])
    assert(event.origin.flowName == Option("a"))
    assert(event.level == EventLevel.INFO)
  }
}
