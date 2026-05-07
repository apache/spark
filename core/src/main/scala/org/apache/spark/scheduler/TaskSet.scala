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

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.internal.LogKeys.{STAGE_ATTEMPT_ID, STAGE_ID}
import org.apache.spark.internal.MessageWithContext

/**
 * A set of tasks submitted together to the low-level TaskScheduler, usually representing
 * missing partitions of a particular stage.
 */
private[spark] class TaskSet(
    val tasks: Array[Task[_]],
    val stageId: Int,
    val stageAttemptId: Int,
    val priority: Int,
    val properties: Properties,
    val resourceProfileId: Int,
    val shuffleId: Option[Int]) {
  val id: String = s"$stageId.$stageAttemptId"

  override def toString: String = "TaskSet " + id

  // Identifier used in the structured logging framework.
  lazy val logId: MessageWithContext = {
    val hashMap = new java.util.HashMap[String, String]()
    hashMap.put(STAGE_ID.name, stageId.toString)
    hashMap.put(STAGE_ATTEMPT_ID.name, stageAttemptId.toString)
    MessageWithContext(id, hashMap)
  }
}
