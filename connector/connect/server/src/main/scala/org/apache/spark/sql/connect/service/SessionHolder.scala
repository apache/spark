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

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import collection.JavaConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession

/**
 * Object used to hold the Spark Connect session state.
 */
case class SessionHolder(
  userId: String,
  sessionId: String,
  session: SparkSession) {

  val executePlanOperations: ConcurrentMap[String, ExecutePlanHolder] =
    new ConcurrentHashMap[String, ExecutePlanHolder]()

  private[connect] def createExecutePlanOperation(
      request: proto.ExecutePlanRequest): ExecutePlanHolder = {

    val operationId = UUID.randomUUID().toString

    executePlanOperations.put(
      operationId,
      ExecutePlanHolder(operationId, this, request))
  }

  private[connect] def removeExecutePlanHolder(operationId: String): Unit = {
    executePlanOperations.remove(operationId)
  }

  private[connect] def interruptAll(): Unit = {
    executePlanOperations.asScala.values.foreach(_.interrupt())
  }
}
