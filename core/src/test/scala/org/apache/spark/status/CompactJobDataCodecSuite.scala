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

import java.util.Date

import org.apache.spark.{JobExecutionStatus, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Status.COMPACT_UI_STORE_ENABLED
import org.apache.spark.status.api.v1.JobData
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.kvstore.CompactInMemoryStore

class CompactJobDataCodecSuite extends SparkFunSuite {
  test("completed job summaries omit details while normal reads preserve every field") {
    val codec = new CompactJobDataCodec()
    val serializer = new KVStoreProtobufSerializer()
    val input = job(JobExecutionStatus.FAILED)
    val encoded = codec.encode(input)
    val summary = encoded.decodeSummary()
    assert(summary.info.stageIds == input.info.stageIds)
    assert(summary.info.description == input.info.description)
    assert(summary.info.killedTasksSummary.isEmpty)
    assert(summary.info.jobTags.isEmpty)
    assert(summary.skippedStages.isEmpty)
    assert(summary.sqlExecutionId.isEmpty)
    val actual = serializer.serialize(encoded.decode())
    assert(actual.toSeq == serializer.serialize(input).toSeq)
  }

  test("job summary views track replacements, details and eviction together") {
    val conf = new SparkConf(false).set(COMPACT_UI_STORE_ENABLED, true)
    val store = KVUtils.createInMemoryStore(conf).asInstanceOf[CompactInMemoryStore]
    try {
      val active = job(JobExecutionStatus.RUNNING)
      store.write(active)
      val appStore = new AppStatusStore(store)
      assert(appStore.jobSummaries().head.status == JobExecutionStatus.RUNNING)
      store.write(job(JobExecutionStatus.SUCCEEDED))
      val summary = appStore.jobSummaries().head
      assert(summary.status == JobExecutionStatus.SUCCEEDED)
      assert(summary.killedTasksSummary.isEmpty)
      assert(appStore.job(summary.jobId).killedTasksSummary == Map("cancelled" -> 2))
      assert(appStore.jobWithAssociatedSql(summary.jobId)._2.contains(9L))
      store.delete(classOf[JobDataWrapper], summary.jobId)
      assert(appStore.jobSummaries().isEmpty)
      intercept[NoSuchElementException] { appStore.job(summary.jobId) }
    } finally {
      store.close()
    }
  }

  private def job(status: JobExecutionStatus): JobDataWrapper = {
    val info = new JobData(7, "job", Some("description"), Some(new Date(100)),
      if (status == JobExecutionStatus.RUNNING) None else Some(new Date(200)),
      Seq(1, 2, 3), Some("group"), Seq("tag"), status,
      100, 1, 90, 3, 4, 2, 89, 1, 2, 1, 1, Map("cancelled" -> 2))
    new JobDataWrapper(info, Set(2), Some(9L))
  }
}
