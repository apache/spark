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

package org.apache.spark.util

import java.io.File

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.rdd.CheckpointRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkFunSuite

class CheckpointingIteratorSuite extends SparkFunSuite with LocalSparkContext with Logging {
  var checkpointDir: File = _
  val partitioner = new HashPartitioner(2)

  override def beforeEach() {
    super.beforeEach()
    checkpointDir = File.createTempFile("temp", "", Utils.createTempDir())
    checkpointDir.delete()
    sc = new SparkContext("local", "test")
    sc.setCheckpointDir(checkpointDir.toString)
  }

  override def afterEach() {
    super.afterEach()
    Utils.deleteRecursively(checkpointDir)
  }

  test("basic test") {
    val broadcastedConf = sc.broadcast(
      new SerializableConfiguration(sc.hadoopConfiguration))

    val context = new TaskContextImpl(0, 0, 0, 0, null)
    val iter = List(1, 2, 3).iterator
    val checkpoingIter = CheckpointingIterator[Int](
      iter,
      checkpointDir.toString,
      broadcastedConf,
      0,
      context)

    assert(checkpoingIter.hasNext)
    assert(checkpoingIter.next() === 1)

    assert(checkpoingIter.hasNext)
    assert(checkpoingIter.next() === 2)

    assert(checkpoingIter.hasNext)
    assert(checkpoingIter.next() === 3)

    // checkpoint data should be written out now.
    val outputDir = new Path(checkpointDir.toString)
    val finalOutputName = CheckpointRDD.splitIdToFile(0)
    val finalPath = new Path(outputDir, finalOutputName)

    val fs = outputDir.getFileSystem(sc.hadoopConfiguration)
    assert(fs.exists(finalPath))

    assert(!checkpoingIter.hasNext)
    assert(!checkpoingIter.hasNext)
  }

  test("no checkpoing data output if iterator is not consumed to latest element") {
    val broadcastedConf = sc.broadcast(
      new SerializableConfiguration(sc.hadoopConfiguration))

    val context = new TaskContextImpl(0, 0, 0, 0, null)
    val iter = List(1, 2, 3).iterator
    val checkpoingIter = CheckpointingIterator[Int](
      iter,
      checkpointDir.toString,
      broadcastedConf,
      0,
      context)

    assert(checkpoingIter.hasNext)
    assert(checkpoingIter.next() === 1)

    assert(checkpoingIter.hasNext)
    assert(checkpoingIter.next() === 2)

    // checkpoint data should not be written out yet.
    val outputDir = new Path(checkpointDir.toString)
    val finalOutputName = CheckpointRDD.splitIdToFile(0)
    val finalPath = new Path(outputDir, finalOutputName)

    val fs = outputDir.getFileSystem(sc.hadoopConfiguration)
    assert(!fs.exists(finalPath))
  }
}
