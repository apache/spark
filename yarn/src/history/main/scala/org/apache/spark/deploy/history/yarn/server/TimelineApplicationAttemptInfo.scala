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

package org.apache.spark.deploy.history.yarn.server

import org.apache.spark.deploy.history.{ApplicationHistoryInfo, ApplicationAttemptInfo}
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._

/**
 * Extend [[ApplicationAttemptInfo]] with information about the entityID; this allows
 * the attemptId to be set to something in the web UI for people, rather than
 * display the YARN attempt ID used  to retrieve it from the timeline server.
 *
 * @param attemptId attemptID for GUI
 * @param startTime start time in millis
 * @param endTime end time in millis (or 0)
 * @param lastUpdated updated time in millis
 * @param sparkUser user
 * @param completed flag true if completed
 * @param entityId ID of the YARN timeline server entity containing the data
 */
private[spark] class TimelineApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String,
    completed: Boolean,
    val entityId: String,
    val sparkAttemptId: Option[String],
    val version: Long = 0)
    extends ApplicationAttemptInfo(attemptId,
  startTime,
  endTime,
  lastUpdated,
  sparkUser,
  completed) {

  /**
   * Describe the application history, including timestamps and completed flag.
   *
   * @return a string description
   */
  override def toString: String = {
    val never = "-"
    s"""attemptId $attemptId,
       | completed = $completed,
       | sparkAttemptId $sparkAttemptId,
       | started ${timeShort(startTime, never)},
       | ended ${timeShort(endTime, never)},
       | updated ${timeShort(lastUpdated, never)},
       | sparkUser = $sparkUser,
       | version = $version",
     """.stripMargin
  }

}

private[spark] class TimelineApplicationHistoryInfo(
  override val id: String,
  override val name: String,
  override val attempts: List[TimelineApplicationAttemptInfo])
  extends ApplicationHistoryInfo(id, name, attempts)
