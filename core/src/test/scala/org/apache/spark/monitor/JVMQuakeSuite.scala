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
package org.apache.spark.monitor

import java.io.File

import org.scalatest.BeforeAndAfterEach
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

class JVMQuakeSuite extends SparkFunSuite with BeforeAndAfterEach with Logging {

  val alloc = new ArrayBuffer[Array[Byte]]()

  override def afterEach(): Unit = {
    alloc.clear()
    System.gc()
  }

  def easyNonOOM(alloc: ArrayBuffer[Array[Byte]], monitor: JVMQuake): Unit = {
    val goodZone = 0.5 * Runtime.getRuntime.maxMemory
    logInfo(s"Not triggering OutOfMemory by only allocating $goodZone bytes")

    val capacity = 1024 * 1024 * 100
    while (alloc.size * capacity < goodZone) {
      val bytes = new Array[Byte](capacity)
      alloc.append(bytes)
    }

    while (monitor.shouldDumpHeap) {
      for (i <- alloc.indices) {
        val bytes = new Array[Byte](capacity)
        alloc(i) = bytes
      }
    }
  }

  test("check jvm quake status") {
    val conf = new SparkConf
    conf.set("spark.jvmQuake.threshold", "1s")
    conf.set("spark.jvmQuake.enabled", "true")
    conf.set("spark.jvmQuake.checkInterval", "1s")
    conf.set("spark.jvmQuake.runTimeWeight", "1")

    val monitor = new JVMQuake(conf)
    monitor.start()
    easyNonOOM(alloc, monitor)
    monitor.stop()
    assert(monitor.heapExist === true)
    val savePath = s"${monitor.getHeapDumpSavePath("test-app-id")}/spark-quake-heapdump.hprof"
    val linkPath = monitor.getHeapDumpLinkPath("test-app-id")
    assert(new File(savePath).exists())
    Utils.deleteRecursively(new File(linkPath))
  }
}
