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

package org.apache.spark.scheduler.cluster

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.ExecutorResourceInfo

/**
 * Grouping of data for an executor used by CoarseGrainedSchedulerBackend.
 *
 * @param executorEndpoint The RpcEndpointRef representing this executor
 * @param executorAddress The network address of this executor
 * @param executorHost The hostname that this executor is running on
 * @param freeCores  The current number of cores available for work on the executor
 * @param totalCores The total number of cores available to the executor
 * @param resourcesInfo The information of the currently available resources on the executor
 * @param resourceProfileId The id of the ResourceProfile being used by this executor
 * @param registrationTs The registration timestamp of this executor
 * @param requestTs What time this executor was most likely requested at
 */
private[cluster] class ExecutorData(
    val executorEndpoint: RpcEndpointRef,
    val executorAddress: RpcAddress,
    override val executorHost: String,
    var freeCores: Int,
    override val totalCores: Int,
    override val logUrlMap: Map[String, String],
    override val attributes: Map[String, String],
    override val resourcesInfo: Map[String, ExecutorResourceInfo],
    override val resourceProfileId: Int,
    val registrationTs: Long,
    val requestTs: Option[Long]
) extends ExecutorInfo(executorHost, totalCores, logUrlMap, attributes,
  resourcesInfo, resourceProfileId, Some(registrationTs), requestTs)
