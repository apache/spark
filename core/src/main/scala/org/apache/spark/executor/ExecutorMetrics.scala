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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Executor level metrics.
 *
 * This is sent to the driver periodically (on executor heartbeat), to provide
 * information about each executor's metrics.
 *
 * @param timestamp the time the metrics were collected, or -1 for Spark history
 *                  log events which are logged when a stage has completed
 * @param jvmUsedHeapMemory the amount of JVM used heap memory for the executor
 * @param jvmUsedNonHeapMemory the amount of JVM used non-heap memory for the executor
 * @param onHeapExecutionMemory the amount of on heap execution memory used
 * @param offHeapExecutionMemory the amount of off heap execution memory used
 * @param onHeapStorageMemory the amount of on heap storage memory used
 * @param offHeapStorageMemory the amount of off heap storage memory used
 * @param onHeapUnifiedMemory the amount of on heap unified region memory used
 * @param offHeapUnifiedMemory the amount of off heap unified region memory used
 * @param directMemory the amount of direct memory used
 * @param mappedMemory the amount of mapped memory used
 */
@DeveloperApi
class ExecutorMetrics private[spark] (
    val timestamp: Long,
    val jvmUsedHeapMemory: Long,
    val jvmUsedNonHeapMemory: Long,
    val onHeapExecutionMemory: Long,
    val offHeapExecutionMemory: Long,
    val onHeapStorageMemory: Long,
    val offHeapStorageMemory: Long,
    val onHeapUnifiedMemory: Long,
    val offHeapUnifiedMemory: Long,
    val directMemory: Long,
    val mappedMemory: Long) extends Serializable
