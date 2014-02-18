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

package org.apache.spark

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.scheduler.JsonSerializable
import org.apache.spark.util.Utils

import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import net.liftweb.json.DefaultFormats

/**
 * Various possible reasons why a task ended. The low-level TaskScheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
 */
private[spark] sealed trait TaskEndReason extends JsonSerializable {
  override def toJson = "Reason" -> Utils.getFormattedClassName(this)
}

private[spark] case object TaskEndReason {
  def fromJson(json: JValue): TaskEndReason = {
    implicit val format = DefaultFormats
    val success = Utils.getFormattedClassName(Success)
    val resubmitted = Utils.getFormattedClassName(Resubmitted)
    val fetchFailed = Utils.getFormattedClassName(FetchFailed)
    val exceptionFailure = Utils.getFormattedClassName(ExceptionFailure)
    val taskResultLost = Utils.getFormattedClassName(TaskResultLost)
    val taskKilled = Utils.getFormattedClassName(TaskKilled)
    val executorLostFailure = Utils.getFormattedClassName(ExecutorLostFailure)
    val unknownReason = Utils.getFormattedClassName(UnknownReason)

    (json \ "Reason").extract[String] match {
      case `success` => Success
      case `resubmitted` => Resubmitted
      case `fetchFailed` => fetchFailedFromJson(json)
      case `exceptionFailure` => exceptionFailureFromJson(json)
      case `taskResultLost` => TaskResultLost
      case `taskKilled` => TaskKilled
      case `executorLostFailure` => ExecutorLostFailure
      case `unknownReason` => UnknownReason
    }
  }

  private def fetchFailedFromJson(json: JValue): TaskEndReason = {
    implicit val format = DefaultFormats
    new FetchFailed(
      BlockManagerId.fromJson(json \ "Block Manager Address"),
      (json \ "Shuffle ID").extract[Int],
      (json \ "Map ID").extract[Int],
      (json \ "Reduce ID").extract[Int])
  }

  private def exceptionFailureFromJson(json: JValue): TaskEndReason = {
    implicit val format = DefaultFormats
    val metrics = (json \ "Metrics") match {
      case JNothing => None
      case value: JValue => Some(TaskMetrics.fromJson(value))
    }
    val stackTrace = Utils.stackTraceFromJson(json \ "Stack Trace")
    new ExceptionFailure(
      (json \ "Class Name").extract[String],
      (json \ "Description").extract[String],
      stackTrace,
      metrics)
  }
}

private[spark] case object Success extends TaskEndReason

// Task was finished earlier but we've now lost it
private[spark] case object Resubmitted extends TaskEndReason

private[spark] case class FetchFailed(
    bmAddress: BlockManagerId,
    shuffleId: Int,
    mapId: Int,
    reduceId: Int)
  extends TaskEndReason {
  override def toJson = {
    super.toJson ~
    ("Block Manager Address" -> bmAddress.toJson) ~
    ("Shuffle ID" -> shuffleId) ~
    ("Map ID" -> mapId) ~
    ("Reduce ID" -> reduceId)
  }
}

private[spark] case class ExceptionFailure(
    className: String,
    description: String,
    stackTrace: Array[StackTraceElement],
    metrics: Option[TaskMetrics])
  extends TaskEndReason {
  override def toJson = {
    val stackTraceJson = Utils.stackTraceToJson(stackTrace)
    val metricsJson = metrics.map(_.toJson).getOrElse(JNothing)
    super.toJson ~
    ("Class Name" -> className) ~
    ("Description" -> description) ~
    ("Stack Trace" -> stackTraceJson) ~
    ("Metrics" -> metricsJson)
  }
}

/**
 * The task finished successfully, but the result was lost from the executor's block manager before
 * it was fetched.
 */
private[spark] case object TaskResultLost extends TaskEndReason

private[spark] case object TaskKilled extends TaskEndReason

/**
 * The task failed because the executor that it was running on was lost. This may happen because
 * the task crashed the JVM.
 */
private[spark] case object ExecutorLostFailure extends TaskEndReason

/**
 * We don't know why the task ended -- for example, because of a ClassNotFound exception when
 * deserializing the task result.
 */
private[spark] case object UnknownReason extends TaskEndReason
