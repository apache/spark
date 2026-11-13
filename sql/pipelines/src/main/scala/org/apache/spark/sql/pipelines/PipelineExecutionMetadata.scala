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

package org.apache.spark.sql.pipelines

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{DeveloperApi, Since}

/**
 * Metadata used to correlate Spark Declarative Pipelines flow executions with Spark jobs.
 *
 * @since 4.4.0
 */
@DeveloperApi
@Since("4.4.0")
object PipelineExecutionMetadata {

  /** Spark local property containing the canonical identifier of the executing flow. */
  val FLOW_IDENTIFIER_PROPERTY: String = "spark.sql.pipelines.flow.identifier"

  /** Spark local property containing the unique identifier of the flow execution attempt. */
  val FLOW_EXECUTION_ID_PROPERTY: String = "spark.sql.pipelines.flow.executionId"

  /** Returns the Spark job tag associated with a flow execution attempt. */
  def flowExecutionIdTag(executionId: String): String =
    s"$FLOW_EXECUTION_ID_PROPERTY:$executionId"

  private[pipelines] def withFlowExecutionMetadata[T](
      sc: SparkContext,
      flowIdentifier: String,
      executionId: String)(body: => T): T = {
    val previousFlowIdentifier = sc.getLocalProperty(FLOW_IDENTIFIER_PROPERTY)
    val previousExecutionId = sc.getLocalProperty(FLOW_EXECUTION_ID_PROPERTY)
    val executionTag = flowExecutionIdTag(executionId)
    val executionTagAlreadySet = sc.getJobTags().contains(executionTag)

    sc.setLocalProperty(FLOW_IDENTIFIER_PROPERTY, flowIdentifier)
    sc.setLocalProperty(FLOW_EXECUTION_ID_PROPERTY, executionId)
    if (!executionTagAlreadySet) {
      sc.addJobTag(executionTag)
    }
    try {
      body
    } finally {
      if (!executionTagAlreadySet) {
        sc.removeJobTag(executionTag)
      }
      sc.setLocalProperty(FLOW_IDENTIFIER_PROPERTY, previousFlowIdentifier)
      sc.setLocalProperty(FLOW_EXECUTION_ID_PROPERTY, previousExecutionId)
    }
  }
}
