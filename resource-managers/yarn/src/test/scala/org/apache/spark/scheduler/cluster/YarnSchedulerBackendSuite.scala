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
package org.apache.spark.scheduler.cluster

import java.util.concurrent.atomic.AtomicReference

import scala.language.reflectiveCalls

import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.{LocalSparkContext, SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.serializer.JavaSerializer

class YarnSchedulerBackendSuite extends SparkFunSuite with MockitoSugar with LocalSparkContext {

  private var yarnSchedulerBackend: YarnSchedulerBackend = _

  override def afterEach(): Unit = {
    try {
      if (yarnSchedulerBackend != null) {
        yarnSchedulerBackend.stop()
      }
    } finally {
      super.afterEach()
    }
  }

  test("RequestExecutors reflects node blacklist and is serializable") {
    sc = new SparkContext("local", "YarnSchedulerBackendSuite")
    // Subclassing the TaskSchedulerImpl here instead of using Mockito. For details see SPARK-26891.
    val sched = new TaskSchedulerImpl(sc) {
      val blacklistedNodes = new AtomicReference[Set[String]]()

      def setNodeBlacklist(nodeBlacklist: Set[String]): Unit = blacklistedNodes.set(nodeBlacklist)

      override def nodeBlacklist(): Set[String] = blacklistedNodes.get()
    }

    val yarnSchedulerBackendExtended = new YarnSchedulerBackend(sched, sc) {
      def setHostToLocalTaskCount(hostToLocalTaskCount: Map[String, Int]): Unit = {
        this.hostToLocalTaskCount = hostToLocalTaskCount
      }
    }
    yarnSchedulerBackend = yarnSchedulerBackendExtended
    val ser = new JavaSerializer(sc.conf).newInstance()
    for {
      blacklist <- IndexedSeq(Set[String](), Set("a", "b", "c"))
      numRequested <- 0 until 10
      hostToLocalCount <- IndexedSeq(
        Map[String, Int](),
        Map("a" -> 1, "b" -> 2)
      )
    } {
      yarnSchedulerBackendExtended.setHostToLocalTaskCount(hostToLocalCount)
      sched.setNodeBlacklist(blacklist)
      val req = yarnSchedulerBackendExtended.prepareRequestExecutors(numRequested)
      assert(req.requestedTotal === numRequested)
      assert(req.nodeBlacklist === blacklist)
      assert(req.hostToLocalTaskCount.keySet.intersect(blacklist).isEmpty)
      // Serialize to make sure serialization doesn't throw an error
      ser.serialize(req)
    }
  }

}
