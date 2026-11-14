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

package org.apache.spark.ui.jobs

import java.util.Date

import org.apache.spark.{JobExecutionStatus, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Status.COMPACT_UI_STORE_ENABLED
import org.apache.spark.status.{AppStatusStore, JobDataWrapper, KVUtils}
import org.apache.spark.status.api.v1.JobData

class JobDataSourceSuite extends SparkFunSuite {
  test("job sorting reads summaries and only selected rows load complete details") {
    val kvstore = KVUtils.createInMemoryStore(
      new SparkConf(false).set(COMPACT_UI_STORE_ENABLED, true))
    var reads = 0
    val store = new AppStatusStore(kvstore) {
      override def job(id: Int): JobData = {
        reads += 1
        super.job(id)
      }
    }
    try {
      (1 to 4).foreach { id =>
        val info = new JobData(id, s"job $id", Some(s"description $id"),
          Some(new Date(100)), Some(new Date(100 + id * 10)), Nil, None, Nil,
          JobExecutionStatus.SUCCEEDED, 1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, Map("killed" -> id))
        kvstore.write(new JobDataWrapper(info, Set.empty, None))
      }
      val summaries = store.jobSummaries()
      assert(summaries.forall(_.killedTasksSummary.isEmpty))
      val source = new JobDataSource(store, summaries, "", 2, "Duration", desc = true)
      assert(reads == 0)
      assert(source.dataSize == 4)
      val page = source.sliceData(1, 3)
      assert(page.map(_.jobData.jobId) == Seq(3, 2))
      assert(page.map(_.jobData.killedTasksSummary) == Seq(Map("killed" -> 3), Map("killed" -> 2)))
      assert(reads == 2)
    } finally {
      kvstore.close()
    }
  }
}
