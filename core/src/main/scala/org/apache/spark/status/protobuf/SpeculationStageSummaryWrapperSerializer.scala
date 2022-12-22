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

package org.apache.spark.status.protobuf

import org.apache.spark.status.SpeculationStageSummaryWrapper
import org.apache.spark.status.api.v1.SpeculationStageSummary

class SpeculationStageSummaryWrapperSerializer extends ProtobufSerDe {

  override val supportClass: Class[_] = classOf[SpeculationStageSummaryWrapper]

  override def serialize(input: Any): Array[Byte] =
    serialize(input.asInstanceOf[SpeculationStageSummaryWrapper])

  private def serialize(s: SpeculationStageSummaryWrapper): Array[Byte] = {
    val summary = serializeSpeculationStageSummary(s.info)
    val builder = StoreTypes.SpeculationStageSummaryWrapper.newBuilder()
    builder.setStageId(s.stageId.toLong)
    builder.setStageAttemptId(s.stageAttemptId)
    builder.setInfo(summary)
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): SpeculationStageSummaryWrapper = {
    val wrapper = StoreTypes.SpeculationStageSummaryWrapper.parseFrom(bytes)
    new SpeculationStageSummaryWrapper(
      stageId = wrapper.getStageId.toInt,
      stageAttemptId = wrapper.getStageAttemptId,
      info = deserializeSpeculationStageSummary(wrapper.getInfo)
    )
  }

  private def serializeSpeculationStageSummary(summary: SpeculationStageSummary):
    StoreTypes.SpeculationStageSummary = {
    val summaryBuilder = StoreTypes.SpeculationStageSummary.newBuilder()
    summaryBuilder.setNumTasks(summary.numTasks)
    summaryBuilder.setNumActiveTasks(summary.numActiveTasks)
    summaryBuilder.setNumCompletedTasks(summary.numCompletedTasks)
    summaryBuilder.setNumFailedTasks(summary.numFailedTasks)
    summaryBuilder.setNumKilledTasks(summary.numKilledTasks)
    summaryBuilder.build()
  }

  private def deserializeSpeculationStageSummary(info: StoreTypes.SpeculationStageSummary):
    SpeculationStageSummary = {
    new SpeculationStageSummary(
      numTasks = info.getNumTasks,
      numActiveTasks = info.getNumActiveTasks,
      numCompletedTasks = info.getNumCompletedTasks,
      numFailedTasks = info.getNumFailedTasks,
      numKilledTasks = info.getNumKilledTasks)
  }
}
