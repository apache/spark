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

import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils

class BlockObjectWriterSuite extends FunSuite {
  test("verify write metrics") {
    val file = new File(Utils.createTempDir(), "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)

    writer.write(Long.box(20), Long.box(30))
    // Record metrics update on every write
    assert(writeMetrics.shuffleRecordsWritten === 1)
    // Metrics don't update on every write
    assert(writeMetrics.shuffleBytesWritten == 0)
    // After 32 writes, metrics should update
    for (i <- 0 until 32) {
      writer.flush()
      writer.write(Long.box(i), Long.box(i))
    }
    assert(writeMetrics.shuffleBytesWritten > 0)
    assert(writeMetrics.shuffleRecordsWritten === 33)
    writer.commitAndClose()
    assert(file.length() == writeMetrics.shuffleBytesWritten)
  }

  test("verify write metrics on revert") {
    val file = new File(Utils.createTempDir(), "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)

    writer.write(Long.box(20), Long.box(30))
    // Record metrics update on every write
    assert(writeMetrics.shuffleRecordsWritten === 1)
    // Metrics don't update on every write
    assert(writeMetrics.shuffleBytesWritten == 0)
    // After 32 writes, metrics should update
    for (i <- 0 until 32) {
      writer.flush()
      writer.write(Long.box(i), Long.box(i))
    }
    assert(writeMetrics.shuffleBytesWritten > 0)
    assert(writeMetrics.shuffleRecordsWritten === 33)
    writer.revertPartialWritesAndClose()
    assert(writeMetrics.shuffleBytesWritten == 0)
    assert(writeMetrics.shuffleRecordsWritten == 0)
  }

  test("Reopening a closed block writer") {
    val file = new File(Utils.createTempDir(), "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)

    writer.open()
    writer.close()
    intercept[IllegalStateException] {
      writer.open()
    }
  }
}
