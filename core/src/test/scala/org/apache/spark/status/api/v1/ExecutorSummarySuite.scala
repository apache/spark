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
package org.apache.spark.status.api.v1

import java.util.Date

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.SparkFunSuite

class ExecutorSummarySuite extends SparkFunSuite {

  test("Check ExecutorSummary serialize and deserialize with empty peakMemoryMetrics") {
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val executorSummary = new ExecutorSummary("id", "host:port", true, 1,
      10, 10, 1, 1, 1,
      0, 0, 1, 100,
      1, 100, 100,
      10, false, 20, new Date(1600984336352L),
      Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
      false, Set())
    val expectedJson = "{\"id\":\"id\",\"hostPort\":\"host:port\",\"isActive\":true," +
      "\"rddBlocks\":1,\"memoryUsed\":10,\"diskUsed\":10,\"totalCores\":1,\"maxTasks\":1," +
      "\"activeTasks\":1,\"failedTasks\":0,\"completedTasks\":0,\"totalTasks\":1," +
      "\"totalDuration\":100,\"totalGCTime\":1,\"totalInputBytes\":100," +
      "\"totalShuffleRead\":100,\"totalShuffleWrite\":10,\"isBlacklisted\":false," +
      "\"maxMemory\":20,\"addTime\":1600984336352,\"removeTime\":null,\"removeReason\":null," +
      "\"executorLogs\":{},\"memoryMetrics\":null,\"blacklistedInStages\":[]," +
      "\"peakMemoryMetrics\":null,\"attributes\":{},\"resources\":{},\"resourceProfileId\":1," +
      "\"isExcluded\":false,\"excludedInStages\":[]}"
    val json = mapper.writeValueAsString(executorSummary)
    assert(expectedJson.equals(json))
    val deserializeExecutorSummary = mapper.readValue(json, new TypeReference[ExecutorSummary] {})
    assert(deserializeExecutorSummary.peakMemoryMetrics == None)
  }

}
