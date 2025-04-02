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

import scala.jdk.CollectionConverters._

import org.apache.spark.status.ApplicationInfoWrapper
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.status.protobuf.Utils._


private[protobuf] class ApplicationInfoWrapperSerializer
  extends ProtobufSerDe[ApplicationInfoWrapper] {

  override def serialize(j: ApplicationInfoWrapper): Array[Byte] = {
    val jobData = serializeApplicationInfo(j.info)
    val builder = StoreTypes.ApplicationInfoWrapper.newBuilder()
    builder.setInfo(jobData)
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): ApplicationInfoWrapper = {
    val wrapper = StoreTypes.ApplicationInfoWrapper.parseFrom(bytes)
    new ApplicationInfoWrapper(
      info = deserializeApplicationInfo(wrapper.getInfo)
    )
  }

  private def serializeApplicationInfo(info: ApplicationInfo): StoreTypes.ApplicationInfo = {
    val builder = StoreTypes.ApplicationInfo.newBuilder()
    setStringField(info.id, builder.setId)
    setStringField(info.name, builder.setName)
    info.coresGranted.foreach { c =>
      builder.setCoresGranted(c)
    }
    info.maxCores.foreach { c =>
      builder.setMaxCores(c)
    }
    info.coresPerExecutor.foreach { c =>
      builder.setCoresPerExecutor(c)
    }
    info.memoryPerExecutorMB.foreach { m =>
      builder.setMemoryPerExecutorMb(m)
    }
    info.attempts.foreach{ attempt =>
      builder.addAttempts(serializeApplicationAttemptInfo(attempt))
    }
    builder.build()
  }

  private def deserializeApplicationInfo(info: StoreTypes.ApplicationInfo): ApplicationInfo = {
    val coresGranted = getOptional(info.hasCoresGranted, info.getCoresGranted)
    val maxCores = getOptional(info.hasMaxCores, info.getMaxCores)
    val coresPerExecutor = getOptional(info.hasCoresPerExecutor, info.getCoresPerExecutor)
    val memoryPerExecutorMB = getOptional(info.hasMemoryPerExecutorMb, info.getMemoryPerExecutorMb)
    val attempts = info.getAttemptsList.asScala.map(deserializeApplicationAttemptInfo)
    ApplicationInfo(
      id = getStringField(info.hasId, () => info.getId),
      name = getStringField(info.hasName, () => info.getName),
      coresGranted = coresGranted,
      maxCores = maxCores,
      coresPerExecutor = coresPerExecutor,
      memoryPerExecutorMB = memoryPerExecutorMB,
      attempts = attempts
    )
  }

  private def serializeApplicationAttemptInfo(info: ApplicationAttemptInfo):
    StoreTypes.ApplicationAttemptInfo = {
    val builder = StoreTypes.ApplicationAttemptInfo.newBuilder()
    builder.setStartTime(info.startTime.getTime)
      .setEndTime(info.endTime.getTime)
      .setLastUpdated(info.lastUpdated.getTime)
      .setDuration(info.duration)
      .setCompleted(info.completed)
    setStringField(info.sparkUser, builder.setSparkUser)
    setStringField(info.appSparkVersion, builder.setAppSparkVersion)
    info.attemptId.foreach{ id =>
      builder.setAttemptId(id)
    }
    builder.build()
  }

  private def deserializeApplicationAttemptInfo(info: StoreTypes.ApplicationAttemptInfo):
    ApplicationAttemptInfo = {
    val attemptId = getOptional(info.hasAttemptId, info.getAttemptId)

    ApplicationAttemptInfo(
      attemptId = attemptId,
      startTime = new Date(info.getStartTime),
      endTime = new Date(info.getEndTime),
      lastUpdated = new Date(info.getLastUpdated),
      duration = info.getDuration,
      sparkUser = getStringField(info.hasSparkUser, () => info.getSparkUser),
      completed = info.getCompleted,
      appSparkVersion = getStringField(info.hasAppSparkVersion, () => info.getAppSparkVersion)
    )
  }
}
