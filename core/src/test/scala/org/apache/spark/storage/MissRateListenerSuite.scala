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

import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.Success
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.executor.{BlockAccess, BlockAccessType, InputMetrics, DataReadMethod}
import org.apache.spark.scheduler._

/**
 * Test the behavior of MissRateListener in response to all relevant events.
 */
class MissRateListenerSuite extends FunSuite with Matchers {
  private val bm1 = BlockManagerId("big", "dog", 1)
  private val bm2 = BlockManagerId("fat", "duck", 2)
  private val taskInfo1 = new TaskInfo(0, 0, 0, 0, "big", "dog", TaskLocality.ANY, false)
  private val taskInfo2 = new TaskInfo(0, 0, 0, 0, "fat", "duck", TaskLocality.ANY, false)

  test("task end without accessed blocks") {
    val listener = new MissRateListener
    val taskMetrics = new TaskMetrics

    // Task end with no updated blocks
    assert(listener.missRateFor(1).isEmpty)
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo1, taskMetrics))
    assert(listener.missRateFor(1).isEmpty)
  }

  test("task end with accessed blocks") {
    val listener = new MissRateListener
    assert(listener.missRateFor(1).isEmpty)
    val taskMetrics1 = new TaskMetrics
    val taskMetrics2 = new TaskMetrics
    val missBlock1 = (RDDBlockId(1, 1), BlockAccess(BlockAccessType.Read, None))
    val hitBlock1 = (RDDBlockId(1, 1), BlockAccess(BlockAccessType.Read, Some(InputMetrics(DataReadMethod.Memory))))
    val writeBlock1 = (RDDBlockId(1, 1), BlockAccess(BlockAccessType.Write, None))
    val missBlock2 = (RDDBlockId(1, 2), BlockAccess(BlockAccessType.Read, None))
    val hitBlock2 = (RDDBlockId(1, 2), BlockAccess(BlockAccessType.Read, Some(InputMetrics(DataReadMethod.Memory))))
    val writeBlock2 = (RDDBlockId(1, 2), BlockAccess(BlockAccessType.Write, None))
    taskMetrics1.accessedBlocks = Some(Seq(missBlock1, writeBlock1))
    taskMetrics2.accessedBlocks = Some(Seq(hitBlock1))
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo1, taskMetrics1))
    // 1 compulsory miss
    assert(listener.missRateFor(1).isEmpty)
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo1, taskMetrics1))
    // 1 compulsory miss, 1 miss
    assert(listener.missRateFor(1).isDefined)
    assert(listener.missRateFor(1).get === 1.0 +- 0.01)
    assert(listener.missRateFor(2).isEmpty) // irrelevant RDD
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo2, taskMetrics2))
    // 1 compulsory miss, 1 miss, 1 hit
    assert(listener.missRateFor(1).get === ((1.0 / 2.0) +- 0.01))

    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo2, taskMetrics2))
    // 1 compulsory miss, 1 miss, 2 hits
    assert(listener.missRateFor(1).get === ((1.0 / 3.0) +- 0.01))
    
    taskMetrics1.accessedBlocks = Some(Seq(missBlock2, writeBlock2))
    taskMetrics2.accessedBlocks = Some(Seq(hitBlock2))
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo2, taskMetrics1))
    // 2 compulsory miss, 1 miss, 2 hits
    assert(listener.missRateFor(1).get === ((1.0 / 3.0) +- 0.01))

    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo2, taskMetrics2))
    // 2 compulsory miss, 1 miss, 3 hits
    assert(listener.missRateFor(1).get === ((1.0 / 4.0) +- 0.01))
    
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo2, taskMetrics1))
    // 2 compulsory miss, 2 miss, 3 hits
    assert(listener.missRateFor(1).get === ((2.0 / 5.0) +- 0.01))
 
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo2, taskMetrics1))
    // 2 compulsory miss, 3 miss, 3 hits
    assert(listener.missRateFor(1).get === ((3.0 / 6.0) +- 0.01))
  }

  test("unpersist RDD") {
    val listener = new MissRateListener
    val taskMetrics1 = new TaskMetrics
    val taskMetrics2 = new TaskMetrics
    val missBlock1 = (RDDBlockId(1, 1), BlockAccess(BlockAccessType.Read, None))
    val hitBlock1 = (RDDBlockId(1, 1), BlockAccess(BlockAccessType.Read, Some(InputMetrics(DataReadMethod.Memory))))
    val writeBlock1 = (RDDBlockId(1, 1), BlockAccess(BlockAccessType.Write, None))
    taskMetrics1.accessedBlocks = Some(Seq(missBlock1, writeBlock1))
    taskMetrics2.accessedBlocks = Some(Seq(hitBlock1))
    assert(!listener.missRateFor(1).isDefined)
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo1, taskMetrics1))
    assert(!listener.missRateFor(1).isDefined) // compulsory miss only
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo1, taskMetrics1))
    assert(listener.missRateFor(1).isDefined)
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(100))
    assert(listener.missRateFor(1).isDefined)
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(1))
    assert(!listener.missRateFor(1).isDefined)
  }
}
