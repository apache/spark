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

package org.apache.spark.nomad

import scala.collection.JavaConverters._

import com.hashicorp.nomad.apimodel.Evaluation
import com.hashicorp.nomad.javasdk.{ServerQueryResponse, WaitStrategy}
import com.hashicorp.nomad.scalasdk.{NomadScalaApi, ScalaQueryOptions}

import org.apache.spark.internal.Logging

private[spark] class EvaluationUtils(api: NomadScalaApi) extends Logging {

  /**
   * Traces the execution of an evaluation until it completes.
   *
   * @param evaluationId the ID of the evaluation to trace
   * @return a future that is completed when the evaluation completes
   */
  def traceEvaluation(evaluationId: String, waitStrategy: WaitStrategy): Evaluation = {
    var lastStatus: Option[String] = None
    api.evaluations.info(evaluationId, Some(ScalaQueryOptions(
      waitStrategy = Some(waitStrategy),
      repeatedPollPredicate = Some({ response: ServerQueryResponse[Evaluation] =>
        val status = response.getValue.getStatus
        if (!lastStatus.contains(status)) {
          log.info("Evaluation {} is {}", evaluationId: Any, status)
          lastStatus = Some(status)
        }
        status == "complete"
      })
    ))).getValue
  }

  def traceUntilFullyScheduled(
      evaluation: Evaluation,
      expectImmediateScheduling: Boolean,
      waitStrategy: WaitStrategy): Evaluation = {

    evaluation.getBlockedEval match {
      case null | "" => evaluation
      case blockedEvalId =>
        val unallocatedTaskGroups =
          Option(evaluation.getFailedTgAllocs).map(_.asScala).getOrElse(Map.empty)

        val message = unallocatedTaskGroups.mkString(
          s"Blocked evaluation $blockedEvalId created for failed allocations:\n    ",
          "\n    ",
          "")

        if (expectImmediateScheduling) {
          logError(message)
          sys.error(message)
        } else {
          logInfo(message)
          traceUntilFullyScheduled(blockedEvalId, expectImmediateScheduling, waitStrategy)
        }
    }
  }

  def traceUntilFullyScheduled(
      evaluationId: String,
      expectImmediateScheduling: Boolean,
      waitStrategy: WaitStrategy): Evaluation = {

    traceUntilFullyScheduled(
      traceEvaluation(evaluationId, waitStrategy),
      expectImmediateScheduling,
      waitStrategy)
  }

}
