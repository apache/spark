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

package org.apache.spark.status.protobuf.sql

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.ui.SQLExecutionUIData
import org.apache.spark.status.api.v1.sql.SqlResourceSuite
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer

class KVStoreProtobufSerializerSuite extends SparkFunSuite {

  private val serializer = new KVStoreProtobufSerializer()

  test("SQLExecutionUIData") {
    val input = SqlResourceSuite.sqlExecutionUIData
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[SQLExecutionUIData])
    assert(result.executionId == input.executionId)
    assert(result.description == input.description)
    assert(result.details == input.details)
    assert(result.physicalPlanDescription == input.physicalPlanDescription)
    assert(result.modifiedConfigs == input.modifiedConfigs)
    assert(result.metrics == input.metrics)
    assert(result.submissionTime == input.submissionTime)
    assert(result.completionTime == input.completionTime)
    assert(result.errorMessage == input.errorMessage)
    assert(result.jobs == input.jobs)
    assert(result.stages == input.stages)
    assert(result.metricValues == input.metricValues)
  }

  test("SQLExecutionUIData with metricValues is empty map and null") {
    val templateData = SqlResourceSuite.sqlExecutionUIData

    val input1 = new SQLExecutionUIData(
      executionId = templateData.executionId,
      description = templateData.description,
      details = templateData.details,
      physicalPlanDescription = templateData.physicalPlanDescription,
      modifiedConfigs = templateData.modifiedConfigs,
      metrics = templateData.metrics,
      submissionTime = templateData.submissionTime,
      completionTime = templateData.completionTime,
      errorMessage = templateData.errorMessage,
      jobs = templateData.jobs,
      stages = templateData.stages,
      metricValues = Map.empty
    )
    val bytes1 = serializer.serialize(input1)
    val result1 = serializer.deserialize(bytes1, classOf[SQLExecutionUIData])
    // input.metricValues is empty map, result.metricValues is empty map.
    assert(result1.metricValues.isEmpty)

    val input2 = new SQLExecutionUIData(
      executionId = templateData.executionId,
      description = templateData.description,
      details = templateData.details,
      physicalPlanDescription = templateData.physicalPlanDescription,
      modifiedConfigs = templateData.modifiedConfigs,
      metrics = templateData.metrics,
      submissionTime = templateData.submissionTime,
      completionTime = templateData.completionTime,
      errorMessage = templateData.errorMessage,
      jobs = templateData.jobs,
      stages = templateData.stages,
      metricValues = null
    )
    val bytes2 = serializer.serialize(input2)
    val result2 = serializer.deserialize(bytes2, classOf[SQLExecutionUIData])
    // input.metricValues is null, result.metricValues is also empty map.
    assert(result2.metricValues.isEmpty)
  }
}
