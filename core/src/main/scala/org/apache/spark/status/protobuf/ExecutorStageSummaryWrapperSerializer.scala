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

import org.apache.spark.status.ExecutorStageSummaryWrapper
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

private[protobuf] class ExecutorStageSummaryWrapperSerializer
  extends ProtobufSerDe[ExecutorStageSummaryWrapper] {

  override def serialize(input: ExecutorStageSummaryWrapper): Array[Byte] = {
    val info = ExecutorStageSummarySerializer.serialize(input.info)
    val builder = StoreTypes.ExecutorStageSummaryWrapper.newBuilder()
      .setStageId(input.stageId.toLong)
      .setStageAttemptId(input.stageAttemptId)
      .setInfo(info)
    setStringField(input.executorId, builder.setExecutorId)
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): ExecutorStageSummaryWrapper = {
    val binary = StoreTypes.ExecutorStageSummaryWrapper.parseFrom(bytes)
    val info = ExecutorStageSummarySerializer.deserialize(binary.getInfo)
    new ExecutorStageSummaryWrapper(
      stageId = binary.getStageId.toInt,
      stageAttemptId = binary.getStageAttemptId,
      executorId = getStringField(binary.hasExecutorId, () => weakIntern(binary.getExecutorId)),
      info = info)
  }
}
