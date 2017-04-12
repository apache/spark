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

package org.apache.spark.deploy.yarn

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, HashSet, Set}

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.Mockito._

import org.apache.spark.{SparkConf, SparkFunSuite}

class LocalityPlacementStrategySuite extends SparkFunSuite {

  test("handle large number of containers and tasks (SPARK-18750)") {
    // Run the test in a thread with a small stack size, since the original issue
    // surfaced as a StackOverflowError.
    var error: Throwable = null

    val runnable = new Runnable() {
      override def run(): Unit = try {
        runTest()
      } catch {
        case e: Throwable => error = e
      }
    }

    val thread = new Thread(new ThreadGroup("test"), runnable, "test-thread", 32 * 1024)
    thread.start()
    thread.join()

    assert(error === null)
  }

  private def runTest(): Unit = {
    val yarnConf = new YarnConfiguration()

    // The numbers below have been chosen to balance being large enough to replicate the
    // original issue while not taking too long to run when the issue is fixed. The main
    // goal is to create enough requests for localized containers (so there should be many
    // tasks on several hosts that have no allocated containers).

    val resource = Resource.newInstance(8 * 1024, 4)
    val strategy = new LocalityPreferredContainerPlacementStrategy(new SparkConf(),
      yarnConf, resource, new MockResolver())

    val totalTasks = 32 * 1024
    val totalContainers = totalTasks / 16
    val totalHosts = totalContainers / 16

    val mockId = mock(classOf[ContainerId])
    val hosts = (1 to totalHosts).map { i => (s"host_$i", totalTasks % i) }.toMap
    val containers = (1 to totalContainers).map { i => mockId }
    val count = containers.size / hosts.size / 2

    val hostToContainerMap = new HashMap[String, Set[ContainerId]]()
    hosts.keys.take(hosts.size / 2).zipWithIndex.foreach { case (host, i) =>
      val hostContainers = new HashSet[ContainerId]()
      containers.drop(count * i).take(i).foreach { c => hostContainers += c }
      hostToContainerMap(host) = hostContainers
    }

    strategy.localityOfRequestedContainers(containers.size * 2, totalTasks, hosts,
      hostToContainerMap, Nil)
  }

}
