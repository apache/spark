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

package org.apache.spark.scheduler.cluster.nomad

import scala.collection.mutable

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart, SparkListenerExecutorAdded}
import org.apache.spark.scheduler.cluster.ExecutorInfo

private[spark] class SaveExecutorInfo extends SparkListener {
  val addedExecutorInfos = mutable.Map[String, ExecutorInfo]()
  var driverLogs: Option[collection.Map[String, String]] = None

  override def onExecutorAdded(executor: SparkListenerExecutorAdded) {
    addedExecutorInfos(executor.executorId) = executor.executorInfo
  }

  override def onApplicationStart(appStart: SparkListenerApplicationStart): Unit = {
    driverLogs = appStart.driverLogs
  }
}
