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

import org.apache.spark.status.JobDataWrapper
import org.apache.spark.status.api.v1.JobData
import org.apache.spark.status.protobuf.Utils.{getOptional, getStringField, setStringField}

private[protobuf] class JobDataWrapperSerializer extends ProtobufSerDe[JobDataWrapper] {

  override def serialize(j: JobDataWrapper): Array[Byte] = {
    val jobData = serializeJobData(j.info)
    val builder = StoreTypes.JobDataWrapper.newBuilder()
    builder.setInfo(jobData)
    j.skippedStages.foreach(builder.addSkippedStages)
    j.sqlExecutionId.foreach(builder.setSqlExecutionId)
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): JobDataWrapper = {
    val wrapper = StoreTypes.JobDataWrapper.parseFrom(bytes)
    val sqlExecutionId = getOptional(wrapper.hasSqlExecutionId, wrapper.getSqlExecutionId)
    new JobDataWrapper(
      deserializeJobData(wrapper.getInfo),
      wrapper.getSkippedStagesList.asScala.map(_.toInt).toSet,
      sqlExecutionId
    )
  }

  private def serializeJobData(jobData: JobData): StoreTypes.JobData = {
    val jobDataBuilder = StoreTypes.JobData.newBuilder()
    jobDataBuilder.setJobId(jobData.jobId.toLong)
      .setStatus(JobExecutionStatusSerializer.serialize(jobData.status))
      .setNumTasks(jobData.numTasks)
      .setNumActiveTasks(jobData.numActiveTasks)
      .setNumCompletedTasks(jobData.numCompletedTasks)
      .setNumSkippedTasks(jobData.numSkippedTasks)
      .setNumFailedTasks(jobData.numFailedTasks)
      .setNumKilledTasks(jobData.numKilledTasks)
      .setNumCompletedIndices(jobData.numCompletedIndices)
      .setNumActiveStages(jobData.numActiveStages)
      .setNumCompletedStages(jobData.numCompletedStages)
      .setNumSkippedStages(jobData.numSkippedStages)
      .setNumFailedStages(jobData.numFailedStages)
    setStringField(jobData.name, jobDataBuilder.setName)
    jobData.description.foreach(jobDataBuilder.setDescription)
    jobData.submissionTime.foreach { d =>
      jobDataBuilder.setSubmissionTime(d.getTime)
    }
    jobData.completionTime.foreach { d =>
      jobDataBuilder.setCompletionTime(d.getTime)
    }
    jobData.stageIds.foreach(id => jobDataBuilder.addStageIds(id.toLong))
    jobData.jobGroup.foreach(jobDataBuilder.setJobGroup)
    jobData.jobTags.foreach(jobDataBuilder.addJobTags)
    jobData.killedTasksSummary.foreach { entry =>
      jobDataBuilder.putKillTasksSummary(entry._1, entry._2)
    }
    jobDataBuilder.build()
  }

  private def deserializeJobData(info: StoreTypes.JobData): JobData = {
    val description = getOptional(info.hasDescription, info.getDescription)
    val submissionTime =
      getOptional(info.hasSubmissionTime, () => new Date(info.getSubmissionTime))
    val completionTime = getOptional(info.hasCompletionTime, () => new Date(info.getCompletionTime))
    val jobGroup = getOptional(info.hasJobGroup, info.getJobGroup)
    val status = JobExecutionStatusSerializer.deserialize(info.getStatus)

    new JobData(
      jobId = info.getJobId.toInt,
      name = getStringField(info.hasName, info.getName),
      description = description,
      submissionTime = submissionTime,
      completionTime = completionTime,
      stageIds = info.getStageIdsList.asScala.map(_.toInt),
      jobGroup = jobGroup,
      jobTags = info.getJobTagsList.asScala,
      status = status,
      numTasks = info.getNumTasks,
      numActiveTasks = info.getNumActiveTasks,
      numCompletedTasks = info.getNumCompletedTasks,
      numSkippedTasks = info.getNumSkippedTasks,
      numFailedTasks = info.getNumFailedTasks,
      numKilledTasks = info.getNumKilledTasks,
      numCompletedIndices = info.getNumCompletedIndices,
      numActiveStages = info.getNumActiveStages,
      numCompletedStages = info.getNumCompletedStages,
      numSkippedStages = info.getNumSkippedStages,
      numFailedStages = info.getNumFailedStages,
      killedTasksSummary = info.getKillTasksSummaryMap.asScala.toMap.transform((_, v) => v.toInt))
  }
}
