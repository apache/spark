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

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for the flow-execution decision logic: `PipelinesErrors.streamingSourcesChanged` and
 * `GraphExecution.determineFlowExecutionActionFromError`, the single source of truth for whether a
 * failed flow is retried.
 *
 * These tests are intentionally session-less - both entry points are pure functions of a
 * `Throwable` and two counters. End-to-end behavior (event levels, retry counts, and the terminal
 * run state a source change produces) is covered by `TriggeredGraphExecutionSuite`.
 */
class GraphExecutionSuite extends SparkFunSuite {

  /** The message Structured Streaming asserts with when a stream's source set changes. */
  private def sourceSetChangeMessage: String =
    "There are [2] sources in the checkpoint offsets and now there are [3] sources " +
    "requested by the query. Cannot continue."

  test("streamingSourcesChanged matches only a streaming source-set change error") {
    // Structured Streaming raises a bare AssertionError; it is usually wrapped in another
    // exception by the time the pipeline sees it, so the whole cause chain has to be checked.
    val sourceChange = new AssertionError(sourceSetChangeMessage)
    assert(PipelinesErrors.streamingSourcesChanged(sourceChange))
    assert(PipelinesErrors.streamingSourcesChanged(new RuntimeException("wrapper", sourceChange)))
    // Unrelated errors - including an AssertionError with a different message - must not match.
    assert(!PipelinesErrors.streamingSourcesChanged(new RuntimeException("boom")))
    assert(!PipelinesErrors.streamingSourcesChanged(new AssertionError("a different assertion")))
  }

  test("determineFlowExecutionActionFromError stops on a source change before checking retries") {
    val sourceChange =
      new RuntimeException("stream failed", new AssertionError(sourceSetChangeMessage))

    // The source-change check comes before the retry budget, so the flow stops even with retries
    // left, and the run-level reason names the source change rather than an exhausted budget.
    GraphExecution.determineFlowExecutionActionFromError(
      ex = sourceChange,
      flowDisplayName = "flow_a",
      currentNumTries = 1,
      maxAllowedRetries = 3) match {
      case GraphExecution.StopFlowExecution(reason) =>
        assert(reason.failureMessage.contains("streaming sources added or removed"))
        assert(
          reason.runTerminationReason ==
            StreamingSourcesChangedFailure("flow_a", Some(sourceChange)))
      case other => fail(s"expected StopFlowExecution, got $other")
    }
  }

  test("determineFlowExecutionActionFromError retries other errors until budget is exhausted") {
    val transient = new RuntimeException("transient")
    // Retries remaining -> retry.
    assert(
      GraphExecution.determineFlowExecutionActionFromError(
        ex = transient, flowDisplayName = "flow_a", currentNumTries = 1, maxAllowedRetries = 3) ==
        GraphExecution.RetryFlowExecution)
    // Budget exhausted -> stop as max-retries-exceeded, not as a source change.
    GraphExecution.determineFlowExecutionActionFromError(
      ex = transient,
      flowDisplayName = "flow_a",
      currentNumTries = 4,
      maxAllowedRetries = 3) match {
      case GraphExecution.StopFlowExecution(reason) =>
        assert(!reason.failureMessage.contains("streaming sources added or removed"))
        assert(
          reason.runTerminationReason ==
            QueryExecutionFailure("flow_a", maxRetries = 3, Some(transient)))
      case other => fail(s"expected StopFlowExecution, got $other")
    }
  }
}
