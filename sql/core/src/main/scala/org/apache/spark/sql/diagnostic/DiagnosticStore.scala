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

package org.apache.spark.sql.diagnostic

import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.status.KVUtils
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.{KVIndex, KVStore}

/**
 * Provides a view of a KVStore with methods that make it easy to query diagnostic-specific
 * information. There's no state kept in this class, so it's ok to have multiple instances
 * of it in an application.
 */
private[spark] class DiagnosticStore(store: KVStore) {

  def diagnosticsList(offset: Int, length: Int): Seq[ExecutionDiagnosticData] = {
    KVUtils.viewToSeq(store.view(classOf[ExecutionDiagnosticData]).skip(offset).max(length))
  }

  def diagnostic(executionId: Long): Option[ExecutionDiagnosticData] = {
    try {
      Some(store.read(classOf[ExecutionDiagnosticData], executionId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def adaptiveExecutionUpdates(executionId: Long): Seq[AdaptiveExecutionUpdate] = {
    KVUtils.viewToSeq(
    store.view(classOf[AdaptiveExecutionUpdate])
      .index("updateTime")
      .parent(executionId))
  }
}

/* Represents the diagnostic data of a SQL execution */
private[spark] class ExecutionDiagnosticData(
    @KVIndexParam val executionId: Long,
    val physicalPlan: String,
    val submissionTime: Long,
    val completionTime: Option[Long],
    val errorMessage: Option[String])

/* Represents the plan change of an adaptive execution */
private[spark] class AdaptiveExecutionUpdate(
    @KVIndexParam("id")
    val executionId: Long,
    @KVIndexParam(value = "updateTime", parent = "id")
    val updateTime: Long,
    val physicalPlan: String) {

  @JsonIgnore @KVIndex
  private def naturalIndex: Array[Long] = Array(executionId, updateTime)
}
