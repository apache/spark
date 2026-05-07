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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.mockito.Mockito._
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.execution.datasources.WriteJobDescription
import org.apache.spark.util.SerializableConfiguration

class FileWriterFactorySuite extends SparkFunSuite with PrivateMethodTester {

  test("SPARK-48484: V2Write uses different TaskAttemptIds for different task attempts") {
    val jobDescription = mock(classOf[WriteJobDescription])
    when(jobDescription.serializableHadoopConf).thenReturn(
      new SerializableConfiguration(new Configuration(false)))
    val committer = mock(classOf[FileCommitProtocol])

    val writerFactory = FileWriterFactory(jobDescription, committer)
    val createTaskAttemptContext =
      PrivateMethod[TaskAttemptContextImpl](Symbol("createTaskAttemptContext"))

    val attemptContext =
      writerFactory.invokePrivate(createTaskAttemptContext(0, 1))
    val attemptContext1 =
      writerFactory.invokePrivate(createTaskAttemptContext(0, 2))
    assert(attemptContext.getTaskAttemptID.getTaskID == attemptContext1.getTaskAttemptID.getTaskID)
    assert(attemptContext.getTaskAttemptID.getId != attemptContext1.getTaskAttemptID.getId)
  }
}
