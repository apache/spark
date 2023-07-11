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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.execution.{ExecutePlanResponseObserver, ExecutePlanResponseSender, ExecuteRunner}

/**
 * Object used to hold the Spark Connect execution state, and perform
 */
case class ExecuteHolder(
    request: proto.ExecutePlanRequest,
    operationId: String,
    sessionHolder: SessionHolder) extends Logging {

  val jobTag =
    s"User_${sessionHolder.userId}_Session_${sessionHolder.sessionId}_Request_${operationId}"

  val session = sessionHolder.session

  var responseObserver: ExecutePlanResponseObserver = new ExecutePlanResponseObserver()

  var runner: ExecuteRunner = new ExecuteRunner(this)

  def start(): Unit = {
    runner.start()
  }

  def attachRpc(responseSender: ExecutePlanResponseSender, lastSeenIndex: Long): Boolean = {
    responseSender.run(responseObserver, lastSeenIndex)
  }
}
