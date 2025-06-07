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

import org.apache.spark.sql.pipelines.common.{FlowStatus, RunState}

/**
 * An internal event that is emitted during the run of a pipeline.
 * @param id A globally unique id
 * @param timestamp The time of the event
 * @param origin Where the event originated from
 * @param level Security level of the event
 * @param message A user friendly description of the event
 * @param details The details of the event
 * @param error An error that occurred during the event
 */
case class PipelineEvent(
    id: String,
    timestamp: String,
    origin: PipelineEventOrigin,
    level: EventLevel,
    message: String,
    details: EventDetails,
    error: Option[ErrorDetail]
)

/**
 * Describes where the event originated from
 * @param datasetName The name of the dataset
 * @param flowName The name of the flow
 * @param sourceCodeLocation The location of the source code
 */
case class PipelineEventOrigin(
    datasetName: Option[String],
    flowName: Option[String],
    sourceCodeLocation: Option[SourceCodeLocation]
)

/**
 * Describes the location of the source code
 * @param path The path to the source code
 * @param lineNumber The line number of the source code
 * @param columnNumber The column number of the source code
 * @param endingLineNumber The ending line number of the source code
 * @param endingColumnNumber The ending column number of the source code
 */
case class SourceCodeLocation(
    path: Option[String],
    lineNumber: Option[Int],
    columnNumber: Option[Int],
    endingLineNumber: Option[Int],
    endingColumnNumber: Option[Int]
)

// Additional details about the PipelineEvent
sealed trait EventDetails

// An event indicating that a flow has made progress and transitioned to a different state
case class FlowProgress(status: FlowStatus) extends EventDetails

// An event indicating that a run has made progress and transitioned to a different state
case class RunProgress(state: RunState) extends EventDetails

// Additional details about the error that occurred during the event
case class ErrorDetail(exceptions: Seq[SerializedException])

// An exception that was thrown during a pipeline run
case class SerializedException(className: String, message: String, stack: Seq[StackFrame])

// A stack frame of an exception
case class StackFrame(declaringClass: String, methodName: String)

// The severity level of the event.
sealed trait EventLevel
object EventLevel {
  case object INFO extends EventLevel
  case object WARN extends EventLevel
  case object ERROR extends EventLevel
}
