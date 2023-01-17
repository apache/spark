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
package org.apache.spark.status.api.v1.sql

import java.util.Date

import org.apache.spark.sql.execution.ui.SparkPlanGraphEdge

class ExecutionData private[spark] (
    val id: Long,
    val status: String,
    val description: String,
    val planDescription: String,
    val submissionTime: Date,
    val duration: Long,
    val runningJobIds: Seq[Int],
    val successJobIds: Seq[Int],
    val failedJobIds: Seq[Int],
    val nodes: collection.Seq[Node],
    val edges: collection.Seq[SparkPlanGraphEdge])

case class Node private[spark](
    nodeId: Long,
    nodeName: String,
    wholeStageCodegenId: Option[Long] = None,
    metrics: collection.Seq[Metric])

case class Metric private[spark] (name: String, value: String)
