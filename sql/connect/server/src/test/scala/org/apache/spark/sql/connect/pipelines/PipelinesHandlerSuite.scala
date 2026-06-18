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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.common.RunState
import org.apache.spark.sql.pipelines.graph.{FailureStoppingFlow, UnexpectedRunFailure}
import org.apache.spark.sql.pipelines.logging.{
  ConstructPipelineEvent, EventLevel, PipelineEventOrigin, RunProgress}

class PipelinesHandlerSuite extends SparkFunSuite {

  private def runFailedEvent(message: String, error: Option[Throwable]) =
    ConstructPipelineEvent(
      origin = PipelineEventOrigin(datasetName = None, flowName = None, sourceCodeLocation = None),
      // throwRunFailure only reads message and exception; the remaining fields are filled with
      // valid placeholder values to construct the event.
      level = EventLevel.INFO,
      message = message,
      details = RunProgress(RunState.FAILED),
      exception = error)

  // Use the real no-cause termination-reason messages so the tests break if their wording drifts.
  private val unexpectedRunFailureMessage = UnexpectedRunFailure().message

  private val failureStoppingFlowMessage =
    FailureStoppingFlow(
      Seq(TableIdentifier("t1", Some("db")), TableIdentifier("t2", Some("db")))).message

  // Regression guard rather than a fix-validation test: the old buggy code (throw error.get) also
  // rethrew the cause unchanged, so this case passes against both implementations. The no-cause
  // test below is the one that genuinely exercises this PR's fix.
  test("throwRunFailure rethrows the underlying cause when present") {
    val cause = new RuntimeException("boom")
    val thrown = intercept[RuntimeException] {
      PipelinesHandler.throwRunFailure(runFailedEvent("Run failed.", Some(cause)))
    }
    assert(thrown eq cause)
  }

  test("throwRunFailure surfaces the message when the failure has no cause") {
    // No-cause reasons must fall back to a PIPELINE_RUN_FAILED error built from the event message
    // rather than raising NoSuchElementException; the message is forwarded verbatim.
    Seq(unexpectedRunFailureMessage, failureStoppingFlowMessage).foreach { message =>
      val thrown = intercept[SparkException] {
        PipelinesHandler.throwRunFailure(runFailedEvent(message, None))
      }
      checkError(
        thrown,
        condition = "PIPELINE_RUN_FAILED",
        parameters = Map("message" -> message))
    }
  }
}
