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

private[spark] sealed trait ExecutorDecommissionReason {
  val reason: String = "decommissioned"
  override def toString: String = reason
}

case class DynamicAllocationDecommission() extends ExecutorDecommissionReason {
  override val reason: String = "decommissioned by dynamic allocation"
}

class ExecutorTriggeredDecommission extends ExecutorDecommissionReason

case class K8SDecommission() extends ExecutorTriggeredDecommission

/**
 *
 * @param workerHost When workerHost is defined, it means the worker has been decommissioned too.
 *                   Used to infer if the shuffle data might be lost even if the external shuffle
 *                   service is enabled.
 */
case class StandaloneDecommission(workerHost: Option[String] = None)
  extends ExecutorDecommissionReason {
  override val reason: String = if (workerHost.isDefined) {
    s"Worker ${workerHost.get} decommissioned"
  } else {
    "decommissioned"
  }
}

case class TestExecutorDecommission(host: Option[String] = None)
  extends ExecutorDecommissionReason {
  override val reason: String = if (host.isDefined) {
    s"Host ${host.get} decommissioned(test)"
  } else {
    "decommissioned(test)"
  }
}

/**
 * State related to decommissioning that is kept by the TaskSchedulerImpl. This state is derived
 * from the ExecutorDecommissionReason above but it is kept distinct to allow the state to evolve
 * independently from the message.
 */
case class ExecutorDecommissionState(
    // Timestamp the decommissioning commenced as per the Driver's clock,
    // to estimate when the executor might eventually be lost if EXECUTOR_DECOMMISSION_KILL_INTERVAL
    // is configured.
    startTime: Long,
    reason: ExecutorDecommissionReason) {

  def isHostDecommissioned: Boolean = reason match {
    case StandaloneDecommission(workerHost) => workerHost.isDefined
    case _ => false
  }

  def host: Option[String] = reason match {
    case StandaloneDecommission(workerHost) => workerHost
    case _ => None
  }
}
