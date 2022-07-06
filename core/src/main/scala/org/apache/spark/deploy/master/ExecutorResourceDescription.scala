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

import org.apache.spark.resource.ResourceRequirement

/**
 * Describe resource requirements for different resource profiles. Used for executor schedule.
 *
 * @param coresPerExecutor cores for each executor.
 * @param memoryMbPerExecutor memory for each executor.
 * @param customResourcesPerExecutor custom resource requests for each executor.
 */
private[spark] case class ExecutorResourceDescription(
    coresPerExecutor: Option[Int],
    memoryMbPerExecutor: Int,
    customResourcesPerExecutor: Seq[ResourceRequirement] = Seq.empty)
