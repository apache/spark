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

package org.apache.spark.scheduler

import java.io.{ByteArrayOutputStream, DataOutputStream, UTFDataFormatException}
import java.nio.ByteBuffer
import java.util.Properties

import scala.collection.mutable.HashMap

import org.apache.spark.{JobArtifactSet, SparkFunSuite}
import org.apache.spark.resource.ResourceAmountUtils
import org.apache.spark.resource.ResourceUtils.GPU

class TaskDescriptionSuite extends SparkFunSuite {
  test("encoding and then decoding a TaskDescription results in the same TaskDescription") {
    val originalFiles = new HashMap[String, Long]()
    originalFiles.put("fileUrl1", 1824)
    originalFiles.put("fileUrl2", 2)

    val originalArchives = new HashMap[String, Long]()
    originalArchives.put("archiveUrl1", 1824)
    originalArchives.put("archiveUrl2", 2)

    val originalJars = new HashMap[String, Long]()
    originalJars.put("jar1", 3)

    val originalProperties = new Properties()
    originalProperties.put("property1", "18")
    originalProperties.put("property2", "test value")
    // SPARK-19796 -- large property values (like a large job description for a long sql query)
    // can cause problems for DataOutputStream, make sure we handle correctly
    val sb = new StringBuilder()
    (0 to 10000).foreach(_ => sb.append("1234567890"))
    val largeString = sb.toString()
    originalProperties.put("property3", largeString)
    // make sure we've got a good test case
    intercept[UTFDataFormatException] {
      val out = new DataOutputStream(new ByteArrayOutputStream())
      try {
        out.writeUTF(largeString)
      } finally {
        out.close()
      }
    }

    val originalResources = Map(GPU ->
      Map("1" -> ResourceAmountUtils.toInternalResource(0.2),
        "2" -> ResourceAmountUtils.toInternalResource(0.5),
        "3" -> ResourceAmountUtils.toInternalResource(0.1)))

    // Create a dummy byte buffer for the task.
    val taskBuffer = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))

    val artifacts = new JobArtifactSet(
      None,
      jars = Map(originalJars.toSeq: _*),
      files = Map(originalFiles.toSeq: _*),
      archives = Map(originalArchives.toSeq: _*)
    )

    val originalTaskDescription = new TaskDescription(
      taskId = 1520589,
      attemptNumber = 2,
      executorId = "testExecutor",
      name = "task for test",
      index = 19,
      partitionId = 1,
      artifacts,
      originalProperties,
      cpus = 2,
      originalResources,
      taskBuffer
    )

    val serializedTaskDescription = TaskDescription.encode(originalTaskDescription)
    val decodedTaskDescription = TaskDescription.decode(serializedTaskDescription)

    // Make sure that all of the fields in the decoded task description match the original.
    assert(decodedTaskDescription.taskId === originalTaskDescription.taskId)
    assert(decodedTaskDescription.attemptNumber === originalTaskDescription.attemptNumber)
    assert(decodedTaskDescription.executorId === originalTaskDescription.executorId)
    assert(decodedTaskDescription.name === originalTaskDescription.name)
    assert(decodedTaskDescription.index === originalTaskDescription.index)
    assert(decodedTaskDescription.partitionId === originalTaskDescription.partitionId)
    assert(decodedTaskDescription.artifacts.equals(artifacts))
    assert(decodedTaskDescription.properties.equals(originalTaskDescription.properties))
    assert(decodedTaskDescription.cpus.equals(originalTaskDescription.cpus))
    assert(decodedTaskDescription.resources === originalTaskDescription.resources)
    assert(decodedTaskDescription.serializedTask.equals(taskBuffer))
  }

}
