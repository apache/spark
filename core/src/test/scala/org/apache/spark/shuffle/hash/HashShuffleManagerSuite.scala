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

package org.apache.spark.shuffle.hash

import java.io.{File, FileWriter}

import scala.language.reflectiveCalls

import org.scalatest.FunSuite

import org.apache.spark.{SparkEnv, SparkContext, LocalSparkContext, SparkConf}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.FileShuffleBlockManager
import org.apache.spark.storage.{ShuffleBlockId, FileSegment}

class HashShuffleManagerSuite extends FunSuite with LocalSparkContext {
  private val testConf = new SparkConf(false)

  private def checkSegments(expected: FileSegment, buffer: ManagedBuffer) {
    assert(buffer.isInstanceOf[FileSegmentManagedBuffer])
    val segment = buffer.asInstanceOf[FileSegmentManagedBuffer]
    assert(expected.file.getCanonicalPath === segment.file.getCanonicalPath)
    assert(expected.offset === segment.offset)
    assert(expected.length === segment.length)
  }

  test("consolidated shuffle can write to shuffle group without messing existing offsets/lengths") {

    val conf = new SparkConf(false)
    // reset after EACH object write. This is to ensure that there are bytes appended after
    // an object is written. So if the codepaths assume writeObject is end of data, this should
    // flush those bugs out. This was common bug in ExternalAppendOnlyMap, etc.
    conf.set("spark.serializer.objectStreamReset", "1")
    conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.hash.HashShuffleManager")

    sc = new SparkContext("local", "test", conf)

    val shuffleBlockManager =
      SparkEnv.get.shuffleManager.shuffleBlockManager.asInstanceOf[FileShuffleBlockManager]

    val shuffle1 = shuffleBlockManager.forMapTask(1, 1, 1, new JavaSerializer(conf),
      new ShuffleWriteMetrics)
    for (writer <- shuffle1.writers) {
      writer.write("test1")
      writer.write("test2")
    }
    for (writer <- shuffle1.writers) {
      writer.commitAndClose()
    }

    val shuffle1Segment = shuffle1.writers(0).fileSegment()
    shuffle1.releaseWriters(success = true)

    val shuffle2 = shuffleBlockManager.forMapTask(1, 2, 1, new JavaSerializer(conf),
      new ShuffleWriteMetrics)

    for (writer <- shuffle2.writers) {
      writer.write("test3")
      writer.write("test4")
    }
    for (writer <- shuffle2.writers) {
      writer.commitAndClose()
    }
    val shuffle2Segment = shuffle2.writers(0).fileSegment()
    shuffle2.releaseWriters(success = true)

    // Now comes the test :
    // Write to shuffle 3; and close it, but before registering it, check if the file lengths for
    // previous task (forof shuffle1) is the same as 'segments'. Earlier, we were inferring length
    // of block based on remaining data in file : which could mess things up when there is concurrent read
    // and writes happening to the same shuffle group.

    val shuffle3 = shuffleBlockManager.forMapTask(1, 3, 1, new JavaSerializer(testConf),
      new ShuffleWriteMetrics)
    for (writer <- shuffle3.writers) {
      writer.write("test3")
      writer.write("test4")
    }
    for (writer <- shuffle3.writers) {
      writer.commitAndClose()
    }
    // check before we register.
    checkSegments(shuffle2Segment, shuffleBlockManager.getBlockData(ShuffleBlockId(1, 2, 0)))
    shuffle3.releaseWriters(success = true)
    checkSegments(shuffle2Segment, shuffleBlockManager.getBlockData(ShuffleBlockId(1, 2, 0)))
    shuffleBlockManager.removeShuffle(1)
  }

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }
}
