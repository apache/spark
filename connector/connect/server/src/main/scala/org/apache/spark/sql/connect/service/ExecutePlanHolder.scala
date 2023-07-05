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

package org.apache.spark.sql.connect.service

import org.apache.spark.connect.proto

/**
 * Object used to hold the Spark Connect execution state.
 */
case class ExecutePlanHolder(
    operationId: String,
    sessionHolder: SessionHolder,
    request: proto.ExecutePlanRequest) {

  val jobTag =
    "SparkConnect_" +
      s"User_${sessionHolder.userId}_Session_${sessionHolder.sessionId}_Request_${operationId}"

  def interrupt(): Unit = {
    // TODO/WIP: This only interrupts active Spark jobs that are actively running.
    // This would then throw the error from ExecutePlan and terminate it.
    // But if the query is not running a Spark job, but executing code on Spark driver, this
    // would be a noop and the execution will keep running.
    sessionHolder.session.sparkContext.cancelJobsWithTag(jobTag)
  }

}
