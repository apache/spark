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

package org.apache.spark.status

import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.v1.JobData
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.kvstore.{KVIndex, KVStoreRecordCodec}

/** Separates the list fields of completed jobs from details needed by individual job pages. */
private[spark] class CompactJobDataCodec extends KVStoreRecordCodec[JobDataWrapper] {
  private val serializer = new KVStoreProtobufSerializer()

  override def encode(value: JobDataWrapper): KVStoreRecordCodec.Record[JobDataWrapper] = {
    val info = value.info
    if (info.status == JobExecutionStatus.RUNNING || info.status == JobExecutionStatus.UNKNOWN) {
      new ActiveRecord(value)
    } else {
      val summary = new JobDataWrapper(copyJob(info, Nil, Map.empty), Set.empty, None)
      val details = new JobDataWrapper(
        new JobData(0, null, None, None, None, Nil, None, info.jobTags,
          JobExecutionStatus.UNKNOWN, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, info.killedTasksSummary),
        value.skippedStages, value.sqlExecutionId)
      new CompletedRecord(info.jobId, info.completionTime.map(_.getTime).getOrElse(-1L),
        serializer.serialize(summary), serializer.serialize(details))
    }
  }

  private class ActiveRecord(value: JobDataWrapper)
    extends KVStoreRecordCodec.Record[JobDataWrapper] {
    override def decode(): JobDataWrapper = value

    override def indexValue(indexName: String): Object = indexName match {
      case KVIndex.NATURAL_INDEX_NAME => Int.box(value.info.jobId)
      case "completionTime" => Long.box(value.info.completionTime.map(_.getTime).getOrElse(-1L))
      case _ => throw new IllegalArgumentException(s"Unknown job index: $indexName")
    }
  }

  private class CompletedRecord(
      id: Int,
      completionTime: Long,
      summary: Array[Byte],
      details: Array[Byte]) extends KVStoreRecordCodec.Record[JobDataWrapper] {
    override def decodeSummary(): JobDataWrapper = {
      serializer.deserialize(summary, classOf[JobDataWrapper])
    }

    override def decode(): JobDataWrapper = {
      val info = decodeSummary().info
      val detail = serializer.deserialize(details, classOf[JobDataWrapper])
      new JobDataWrapper(copyJob(info, detail.info.jobTags, detail.info.killedTasksSummary),
        detail.skippedStages, detail.sqlExecutionId)
    }

    override def indexValue(indexName: String): Object = indexName match {
      case KVIndex.NATURAL_INDEX_NAME => Int.box(id)
      case "completionTime" => Long.box(completionTime)
      case _ => throw new IllegalArgumentException(s"Unknown job index: $indexName")
    }
  }

  private def copyJob(
      info: JobData,
      tags: collection.Seq[String],
      killedTasks: Map[String, Int]): JobData = {
    new JobData(info.jobId, info.name, info.description, info.submissionTime, info.completionTime,
      info.stageIds, info.jobGroup, tags, info.status, info.numTasks, info.numActiveTasks,
      info.numCompletedTasks, info.numSkippedTasks, info.numFailedTasks, info.numKilledTasks,
      info.numCompletedIndices, info.numActiveStages, info.numCompletedStages,
      info.numSkippedStages, info.numFailedStages, killedTasks)
  }
}
