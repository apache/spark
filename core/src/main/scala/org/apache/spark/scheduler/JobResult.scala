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

import net.liftweb.json.JsonDSL._

/**
 * A result of a job in the DAGScheduler.
 */
private[spark] sealed trait JobResult extends JsonSerializable

private[spark] case object JobSucceeded extends JobResult {
  override def toJson = ("Status" -> "Success")
}

private[spark] case class JobFailed(exception: Exception, failedStageId: Int)
  extends JobResult {
  override def toJson = {
    ("Status" -> "Failed") ~
    ("Exception" -> exception.getMessage) ~
    ("Failed Stage ID" -> failedStageId)
  }
}
