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

package org.apache.spark

private class SparkJobInfoImpl (
    val jobId: Int,
    val stageIds: Array[Int],
    val status: JobExecutionStatus)
  extends SparkJobInfo

private class SparkStageInfoImpl(
    val stageId: Int,
    val currentAttemptId: Int,
    val submissionTime: Long,
    val name: String,
    val numTasks: Int,
    val numActiveTasks: Int,
    val numCompletedTasks: Int,
    val numFailedTasks: Int)
  extends SparkStageInfo

private class SparkExecutorInfoImpl(
    val host: String,
    val port: Int,
    val cacheSize: Long,
    val numRunningTasks: Int,
    val usedOnHeapStorageMemory: Long,
    val usedOffHeapStorageMemory: Long,
    val totalOnHeapStorageMemory: Long,
    val totalOffHeapStorageMemory: Long)
  extends SparkExecutorInfo
