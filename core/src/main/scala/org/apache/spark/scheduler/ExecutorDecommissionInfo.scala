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
 * Provides more detail when an executor is being decommissioned.
 * @param message Human readable reason for why the decommissioning is happening.
 * @param isHostDecommissioned Whether the host (aka the `node` or `worker` in other places) is
 *                             being decommissioned too. Used to infer if the shuffle data might
 *                             be lost even if the external shuffle service is enabled.
 */
private[spark]
case class ExecutorDecommissionInfo(message: String, isHostDecommissioned: Boolean)
