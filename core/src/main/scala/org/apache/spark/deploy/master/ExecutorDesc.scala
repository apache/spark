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

package org.apache.spark.deploy.master

import org.apache.spark.deploy.{ExecutorDescription, ExecutorState}
import org.apache.spark.resource.ResourceInformation

private[master] class ExecutorDesc(
    val id: Int,
    val application: ApplicationInfo,
    val worker: WorkerInfo,
    val cores: Int,
    val memory: Int,
    // resources(e.f. gpu/fpga) allocated to this executor
    // map from resource name to ResourceInformation
    val resources: Map[String, ResourceInformation],
    val rpId: Int) {

  var state = ExecutorState.LAUNCHING

  /** Copy all state (non-val) variables from the given on-the-wire ExecutorDescription. */
  def copyState(execDesc: ExecutorDescription): Unit = {
    state = execDesc.state
  }

  def fullId: String = application.id + "/" + id

  override def equals(other: Any): Boolean = {
    other match {
      case info: ExecutorDesc =>
        fullId == info.fullId &&
        worker.id == info.worker.id &&
        cores == info.cores &&
        memory == info.memory
      case _ => false
    }
  }

  override def toString: String = fullId

  override def hashCode: Int = toString.hashCode()
}
