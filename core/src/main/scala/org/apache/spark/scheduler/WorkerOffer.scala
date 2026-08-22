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

import org.apache.spark.resource.ResourceProfile

/**
 * Represents free resources available on an executor.
 */
private[spark]
case class WorkerOffer(
    executorId: String,
    host: String,
    cores: BigDecimal,
    // `address` is an optional hostPort string, it provide more useful information than `host`
    // when multiple executors are launched on the same host.
    address: Option[String] = None,
    resources: ExecutorResourcesAmounts = ExecutorResourcesAmounts.empty,
    resourceProfileId: Int = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID,
    // The executor's actual registered total cores (the `--cores` it registered with), which can
    // be smaller than the cores declared by the ResourceProfile or spark.executor.cores. For
    // example, Kubernetes OOM recovery deliberately registers a recovered executor with only
    // spark.task.cpus cores so it runs a single task at a time, and local[N] may use a different N
    // than spark.executor.cores. Used to cap the OOM retry cpus at what the executor can actually
    // run, so a retry can never request more cores than exist and become permanently unschedulable
    // (see spark.task.oomRetryCpusIncrement). Defaults to -1 (unset), which the scheduler treats
    // as "no known total" (no cap from this field).
    totalCores: Int = -1)
