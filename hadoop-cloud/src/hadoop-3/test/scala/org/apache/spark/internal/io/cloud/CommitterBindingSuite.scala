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

package org.apache.spark.internal.io.cloud

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{Job, JobStatus, MRJobConfig, TaskAttemptID}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.io.cloud
import org.apache.spark.internal.io.cloud.PathCommitterConstants._

/**
 * Test committer binding logic.
 */
class CommitterBindingSuite extends SparkFunSuite {


  private val jobId = "2007071202143_0101"
  private val attempt0 = "attempt_" + jobId + "_m_000000_0"
  private val taskAttempt0 = TaskAttemptID.forName(attempt0)

  /**
   * Does the
   * [[BindingParquetOutputCommitter]] committer bind to the schema-specific
   * committer declared for the destination path?
   */
  test("BindingParquetOutputCommitter will bind") {
    val path = new Path("http://example/data")
    val job = newJob(path)
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0)
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1)

    StubPathOutputCommitterFactory.bind(conf, "http")
    val tContext = new TaskAttemptContextImpl(conf, taskAttempt0)
    val parquet = new BindingParquetOutputCommitter(path, tContext)
    val inner = parquet.boundCommitter().asInstanceOf[StubPathOutputCommitter]
    parquet.setupJob(tContext)
    assert(inner.setup, s"$inner not setup")
    parquet.commitJob(tContext)
    assert(inner.committed, s"$inner not committed")
    parquet.abortJob(tContext, JobStatus.State.RUNNING)
    assert(inner.aborted, s"$inner not aborted")
  }

  test("cloud binding") {
    val sc = new SparkConf()
    cloud.bind(sc)
  }

  /**
   * Create a a new job. Sets the task attempt ID.
   *
   * @return the new job
   * @throws IOException failure
   */
  @throws[IOException]
  def newJob(outDir: Path): Job = {
    val job = Job.getInstance(new Configuration())
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0)
    conf.setBoolean(CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)
    FileOutputFormat.setOutputPath(job, outDir)
    job
  }
}
