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

/**
 * Message providing more detail when an executor is being decommissioned.
 * @param message Human readable reason for why the decommissioning is happening.
 * @param workerHost When workerHost is defined, it means the host (aka the `node` or `worker`
 *                in other places) has been decommissioned too. Used to infer if the
 *                shuffle data might be lost even if the external shuffle service is enabled.
 */
private[spark]
case class ExecutorDecommissionInfo(message: String, workerHost: Option[String] = None)

/**
 * State related to decommissioning that is kept by the TaskSchedulerImpl. This state is derived
 * from the info message above but it is kept distinct to allow the state to evolve independently
 * from the message.
 */
private[scheduler] case class ExecutorDecommissionState(
    // Timestamp the decommissioning commenced as per the Driver's clock,
    // to estimate when the executor might eventually be lost if EXECUTOR_DECOMMISSION_KILL_INTERVAL
    // is configured.
    startTime: Long,
    workerHost: Option[String] = None)
