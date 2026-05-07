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
package org.apache.spark.storage

import java.io.File
import java.nio.file.Files

import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{doAnswer, doThrow, mock, spy, times, verify}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.util.Utils

class LogBlockWriterSuite extends SparkFunSuite {
  var tempDir: File = _
  var sparkConf: SparkConf = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    sparkConf = new SparkConf(false)
      .set("spark.local.dir", tempDir.getAbsolutePath)
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      Utils.clearLocalRootDirs()
      super.afterEach()
    }
  }

  test("SPARK-53755: close resources when failed to initialize") {
    val blockManager = mock(classOf[BlockManager])
    val serializerManager =
      spy(new SerializerManager(new JavaSerializer(sparkConf), sparkConf, None))
    doAnswer(_ => serializerManager).when(blockManager).serializerManager
    doThrow(new RuntimeException("Initialization failed"))
      .when(serializerManager).blockSerializationStream(any, any)(any)

    intercept[RuntimeException] {
      new LogBlockWriter(
        blockManager, LogBlockType.TEST, sparkConf)
    }
    verify(serializerManager, times(1)).blockSerializationStream(any, any)(any)
    val leafFiles = Files.walk(tempDir.toPath)
      .filter(Files.isRegularFile(_))
      .toArray
    assert(leafFiles.isEmpty, "Temporary file should be deleted.")
  }

  test("SPARK-53755: bytes written stats") {
    val logBlockWriter = makeLogBlockWriter()

    val log1 = TestLogLine(0L, 1, "Log message 1")
    val log2 = TestLogLine(1L, 2, "Log message 2")
    try {
      logBlockWriter.writeLog(log1)
      logBlockWriter.writeLog(log2)
      logBlockWriter.flush()
      assert(logBlockWriter.bytesWritten() === logBlockWriter.tmpFile.length())
    } finally {
      logBlockWriter.close()
    }
  }

  test("SPARK-53755: writeLog/save operations should fail on closed LogBlockWriter") {
    val logBlockWriter = makeLogBlockWriter()
    val log = TestLogLine(0L, 1, "Log message 1")

    logBlockWriter.writeLog(log)
    logBlockWriter.close()

    val exception1 = intercept[SparkException] {
      logBlockWriter.writeLog(log)
    }
    assert(exception1.getMessage.contains("Writer already closed. Cannot write more data."))

    val exception2 = intercept[SparkException] {
      logBlockWriter.save(TestLogBlockId(0L, "1"))
    }
    assert(exception2.getMessage.contains("Writer already closed. Cannot save."))
  }

  test("SPARK-53755: close writer after saving to block manager") {
    val log = TestLogLine(0L, 1, "Log message 1")

    Seq(true, false).foreach { success =>
      val logBlockWriter = spy(makeLogBlockWriter())
      doAnswer(_ => success).when(logBlockWriter)
        .saveToBlockManager(any[LogBlockId], anyLong)

      logBlockWriter.writeLog(log)
      if (success) {
        logBlockWriter.save(TestLogBlockId(0L, "1"))
      } else {
        val exception = intercept[SparkException] {
          logBlockWriter.save(TestLogBlockId(0L, "1"))
        }
        assert(exception.getMessage.contains("Failed to save log block"))
      }

      verify(logBlockWriter, times(1)).close()
    }
  }

  test("SPARK-53755: skip saving to block manager if no logs written") {
    val logBlockWriter = spy(makeLogBlockWriter())
    logBlockWriter.save(TestLogBlockId(0L, "1"))
    assert(logBlockWriter.bytesWritten() === 0L)
    verify(logBlockWriter, times(0)).saveToBlockManager(any[LogBlockId], anyLong)
  }

  private def makeLogBlockWriter(): LogBlockWriter = {
    val serializerManager = new SerializerManager(new JavaSerializer(sparkConf), sparkConf, None)
    val blockManager = mock(classOf[BlockManager])
    doAnswer(_ => serializerManager).when(blockManager).serializerManager
    new LogBlockWriter(
      blockManager, LogBlockType.TEST, sparkConf)
  }
}
