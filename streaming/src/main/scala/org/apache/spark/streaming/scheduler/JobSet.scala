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

package org.apache.spark.streaming.scheduler

import scala.collection.mutable.HashSet
import org.apache.spark.streaming.Time

private[streaming]
case class JobSet(time: Time, jobs: Seq[Job]) {

  private val incompleteJobs = new HashSet[Job]()
  var submissionTime = System.currentTimeMillis()
  var processingStartTime = -1L
  var processingEndTime = -1L

  jobs.zipWithIndex.foreach { case (job, i) => job.setId(i) }
  incompleteJobs ++= jobs

  def beforeJobStart(job: Job) {
    if (processingStartTime < 0) processingStartTime = System.currentTimeMillis()
  }

  def afterJobStop(job: Job) {
    incompleteJobs -= job
    if (hasCompleted) processingEndTime = System.currentTimeMillis()
  }

  def hasStarted() = (processingStartTime > 0)

  def hasCompleted() = incompleteJobs.isEmpty

  def processingDelay = processingEndTime - processingStartTime

  def totalDelay = {
    processingEndTime - time.milliseconds
  }

  def toBatchInfo(): BatchInfo = {
    new BatchInfo(
      time,
      submissionTime,
      if (processingStartTime >= 0 ) Some(processingStartTime) else None,
      if (processingEndTime >= 0 ) Some(processingEndTime) else None
    )
  }
}
