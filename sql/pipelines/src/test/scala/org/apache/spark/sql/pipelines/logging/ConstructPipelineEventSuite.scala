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

import java.io.{PrintWriter, StringWriter}
import java.sql.Timestamp

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.pipelines.common.FlowStatus

class ConstructPipelineEventSuite extends SparkFunSuite with BeforeAndAfterEach {

  protected def getStackTracePrint(ex: Throwable): String = {
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    ex.printStackTrace(printWriter)
    stringWriter.toString
  }

  test("serializing basic exception with cause") {
    val ex1 = new RuntimeException("exception 1")
    val ex2 = new IllegalStateException("exception 2", ex1)

    val serializedEx = ConstructPipelineEvent.serializeException(ex2)
    assert(serializedEx.map(_.message) == Seq("exception 2", "exception 1"))

    assert(serializedEx.head.className == "java.lang.IllegalStateException")
    assert(serializedEx.head.stack.nonEmpty)
  }

  test("multiple levels of causes") {
    val ex1 = new RuntimeException("exception 1")
    val ex2 = new RuntimeException("exception 2", ex1)
    val ex3 = new IllegalStateException("exception 3", ex2)

    val serializedEx = ConstructPipelineEvent.serializeException(ex3)
    assert(serializedEx.map(_.message) == Seq("exception 3", "exception 2", "exception 1"))

    assert(serializedEx.head.className == "java.lang.IllegalStateException")
  }

  test("serializing exception with stack traces") {
    def generateExceptionWithStackTrace(message: String): RuntimeException = {
      try {
        throw new RuntimeException(message)
      } catch {
        case e: RuntimeException => e
      }
    }

    // Generate exceptions with actual stack traces
    val ex1 = generateExceptionWithStackTrace("exception 1")
    val ex2 = new IllegalStateException("exception 2", ex1)

    val serializedEx = ConstructPipelineEvent.serializeException(ex2)
    assert(serializedEx.map(_.message) == Seq("exception 2", "exception 1"))
    assert(serializedEx.head.className == "java.lang.IllegalStateException")

    assert(
      serializedEx.head.stack.nonEmpty, "Stack trace of main exception should not be empty")
    assert(serializedEx(1).stack.nonEmpty, "Stack trace of cause should not be empty")

    // Get the original and serialized stack traces for comparison
    val originalStackTrace1 = ex1.getStackTrace.map(_.toString)
    val originalStackTrace2 = ex2.getStackTrace.map(_.toString)
    val serializedStackTrace1 = serializedEx(1).stack.toList
    val serializedStackTrace2 = serializedEx.head.stack.toList

    assert(serializedStackTrace1.size == originalStackTrace1.length)
    assert(serializedStackTrace2.size == originalStackTrace2.length)

    // Verify method and class names in the stack trace. Note that stacktraces may contain slightly
    // different method names because of how Scala is compiled to bytecode
    // (e.g. generateExceptionWithStackTrace$1)
    val firstFramePattern = """.*generateExceptionWithStackTrace.*""".r
    assert(firstFramePattern.pattern.matcher(serializedStackTrace1.head.methodName).matches())
    val topFrame = serializedStackTrace2.head
    assert(topFrame.declaringClass.contains(this.getClass.getName))
  }

  test("Basic event construction") {
    val event = ConstructPipelineEvent(
      origin = PipelineEventOrigin(
        datasetName = Some("dataset"),
        flowName = Some("flow"),
        sourceCodeLocation = Some(
          SourceCodeLocation(
            path = Some("path"),
            lineNumber = None,
            columnNumber = None,
            endingLineNumber = None,
            endingColumnNumber = None
          )
        )
      ),
      message = "Flow 'b' has failed",
      details = FlowProgress(FlowStatus.FAILED),
      eventTimestamp = Some(new Timestamp(1747338049615L))
    )
    assert(event.origin.datasetName.contains("dataset"))
    assert(event.origin.flowName.contains("flow"))
    assert(event.origin.sourceCodeLocation.get.path.contains("path"))
    assert(event.message == "Flow 'b' has failed")
    assert(event.details.asInstanceOf[FlowProgress].status == FlowStatus.FAILED)
    assert(event.timestamp == "2025-05-15T19:40:49.615Z")
  }
}
