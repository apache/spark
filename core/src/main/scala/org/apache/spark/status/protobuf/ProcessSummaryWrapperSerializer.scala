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

import java.util.Date

import scala.collection.JavaConverters._

import org.apache.spark.status.ProcessSummaryWrapper
import org.apache.spark.status.api.v1.ProcessSummary
import org.apache.spark.status.protobuf.Utils.getOptional

class ProcessSummaryWrapperSerializer extends ProtobufSerDe {

  override val supportClass: Class[_] = classOf[ProcessSummaryWrapper]

  override def serialize(input: Any): Array[Byte] = {
    serialize(input.asInstanceOf[ProcessSummaryWrapper])
  }

  def serialize(input: ProcessSummaryWrapper): Array[Byte] = {
    val builder = StoreTypes.ProcessSummaryWrapper.newBuilder()
    builder.setInfo(serializeProcessSummary(input.info))
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): ProcessSummaryWrapper = {
    val wrapper = StoreTypes.ProcessSummaryWrapper.parseFrom(bytes)
    new ProcessSummaryWrapper(
      info = deserializeProcessSummary(wrapper.getInfo)
    )
  }

  private def serializeProcessSummary(info: ProcessSummary): StoreTypes.ProcessSummary = {
    val builder = StoreTypes.ProcessSummary.newBuilder()
    builder.setId(info.id)
    builder.setHostPort(info.hostPort)
    builder.setIsActive(info.isActive)
    builder.setTotalCores(info.totalCores)
    builder.setAddTime(info.addTime.getTime)
    info.removeTime.foreach { d =>
      builder.setRemoveTime(d.getTime)
    }
    info.processLogs.foreach { case (k, v) =>
      builder.putProcessLogs(k, v)
    }
    builder.build()
  }

  private def deserializeProcessSummary(info: StoreTypes.ProcessSummary): ProcessSummary = {
    val removeTime = getOptional(info.hasRemoveTime, () => new Date(info.getRemoveTime))
    new ProcessSummary(
      id = info.getId,
      hostPort = info.getHostPort,
      isActive = info.getIsActive,
      totalCores = info.getTotalCores,
      addTime = new Date(info.getAddTime),
      removeTime = removeTime,
      processLogs = info.getProcessLogsMap.asScala.toMap
    )
  }
}
