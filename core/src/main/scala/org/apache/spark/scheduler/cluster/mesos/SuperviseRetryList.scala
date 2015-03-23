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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.mesos.Protos.TaskStatus
import java.util.Date
import scala.collection.mutable.ArrayBuffer

private[spark] case class RetryState(
    submission: DriverSubmission,
    lastFailureStatus: TaskStatus,
    retries: Int,
    nextRetry: Date,
    waitTime: Int) extends Serializable {
  def copy() = new RetryState(submission, lastFailureStatus, retries, nextRetry, waitTime)
}

/**
 * Tracks all the drivers that were submitted under supervise which failed to run
 * and waiting to be scheduled again.
 * @param state Persistence engine to store state.
 */
private[mesos] class SuperviseRetryList(state: ClusterPersistenceEngine) {
  val drivers = new ArrayBuffer[RetryState]

  initialize()

  def initialize() {
    state.fetchAll[RetryState]().foreach {
      s => drivers += s
    }
  }

  def getNextRetry(currentTime: Date): (Option[DriverSubmission], Option[RetryState]) = {
    val retry = drivers.find(d => d.nextRetry.before(currentTime))
    if (retry.isDefined) {
      (Some(retry.get.submission), retry)
    } else {
      (None, None)
    }
  }

  def retries: Iterable[RetryState] = {
    drivers.collect { case d => d.copy}.toList
  }

  def remove(submissionId: String): Boolean = {
    val index =
      drivers.indexWhere(
        s => s.submission.submissionId.equals(submissionId))

    if (index != -1) {
      drivers.remove(index)
      state.expunge(submissionId)
      true
    }

    index != -1
  }

  def add(retryState: RetryState) {
    drivers += retryState
    state.persist(retryState.submission.submissionId, retryState)
  }
}
