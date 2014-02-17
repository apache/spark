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
import org.apache.spark.util.Utils
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.DefaultFormats

/**
 * A result of a job in the DAGScheduler.
 */
private[spark] sealed trait JobResult extends JsonSerializable {
  override def toJson = "Result" -> Utils.getFormattedClassName(this)
}

private[spark] object JobResult {
  def fromJson(json: JValue): JobResult = {
    implicit val format = DefaultFormats
    val jobSucceededString = Utils.getFormattedClassName(JobSucceeded)
    val jobFailedString = Utils.getFormattedClassName(JobFailed)

    (json \ "Result").extract[String] match {
      case `jobSucceededString` => JobSucceeded
      case `jobFailedString` => jobFailedFromJson(json)
    }
  }

  private def jobFailedFromJson(json: JValue): JobResult = {
    implicit val format = DefaultFormats
    new JobFailed(
      Utils.exceptionFromJson(json \ "Exception"),
      (json \ "Failed Stage ID").extract[Int])
  }
}

private[spark] case object JobSucceeded extends JobResult

private[spark] case class JobFailed(exception: Exception, failedStageId: Int) extends JobResult {
  override def toJson = {
    val exceptionJson = Utils.exceptionToJson(exception)

    super.toJson ~
    ("Exception" -> exceptionJson) ~
    ("Failed Stage ID" -> failedStageId)
  }
}
