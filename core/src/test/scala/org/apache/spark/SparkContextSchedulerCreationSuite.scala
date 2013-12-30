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

package org.apache.spark

import org.scalatest.{FunSuite, PrivateMethodTester}

import org.apache.spark.scheduler.TaskScheduler
import org.apache.spark.scheduler.cluster.{ClusterScheduler, SimrSchedulerBackend, SparkDeploySchedulerBackend}
import org.apache.spark.scheduler.cluster.mesos.{CoarseMesosSchedulerBackend, MesosSchedulerBackend}
import org.apache.spark.scheduler.local.LocalScheduler

class SparkContextSchedulerCreationSuite
  extends FunSuite with PrivateMethodTester with LocalSparkContext with Logging {

  def createTaskScheduler(master: String): TaskScheduler = {
    // Create local SparkContext to setup a SparkEnv. We don't actually want to start() the
    // real schedulers, so we don't want to create a full SparkContext with the desired scheduler.
    sc = new SparkContext("local", "test")
    val createTaskSchedulerMethod = PrivateMethod[TaskScheduler]('createTaskScheduler)
    SparkContext invokePrivate createTaskSchedulerMethod(sc, master, "test")
  }

  test("bad-master") {
    val e = intercept[SparkException] {
      createTaskScheduler("localhost:1234")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("local") {
    createTaskScheduler("local") match {
      case s: LocalScheduler =>
        assert(s.threads === 1)
        assert(s.maxFailures === 0)
      case _ => fail()
    }
  }

  test("local-n") {
    createTaskScheduler("local[5]") match {
      case s: LocalScheduler =>
        assert(s.threads === 5)
        assert(s.maxFailures === 0)
      case _ => fail()
    }
  }

  test("local-n-failures") {
    createTaskScheduler("local[4, 2]") match {
      case s: LocalScheduler =>
        assert(s.threads === 4)
        assert(s.maxFailures === 2)
      case _ => fail()
    }
  }

  test("simr") {
    createTaskScheduler("simr://uri") match {
      case s: ClusterScheduler =>
        assert(s.backend.isInstanceOf[SimrSchedulerBackend])
      case _ => fail()
    }
  }

  test("local-cluster") {
    createTaskScheduler("local-cluster[3, 14, 512]") match {
      case s: ClusterScheduler =>
        assert(s.backend.isInstanceOf[SparkDeploySchedulerBackend])
      case _ => fail()
    }
  }

  def testYarn(master: String, expectedClassName: String) {
    try {
      createTaskScheduler(master) match {
        case s: ClusterScheduler =>
          assert(s.getClass === Class.forName(expectedClassName))
        case _ => fail()
      }
    } catch {
      case e: SparkException =>
        assert(e.getMessage.contains("YARN mode not available"))
        logWarning("YARN not available, could not test actual YARN scheduler creation")
      case e: Throwable => fail(e)
    }
  }

  test("yarn-standalone") {
    testYarn("yarn-standalone", "org.apache.spark.scheduler.cluster.YarnClusterScheduler")
  }

  test("yarn-client") {
    testYarn("yarn-client", "org.apache.spark.scheduler.cluster.YarnClientClusterScheduler")
  }

  def testMesos(master: String, expectedClass: Class[_]) {
    try {
      createTaskScheduler(master) match {
        case s: ClusterScheduler =>
          assert(s.backend.getClass === expectedClass)
        case _ => fail()
      }
    } catch {
      case e: UnsatisfiedLinkError =>
        assert(e.getMessage.contains("no mesos in"))
        logWarning("Mesos not available, could not test actual Mesos scheduler creation")
      case e: Throwable => fail(e)
    }
  }

  test("mesos fine-grained") {
    System.setProperty("spark.mesos.coarse", "false")
    testMesos("mesos://localhost:1234", classOf[MesosSchedulerBackend])
  }

  test("mesos coarse-grained") {
    System.setProperty("spark.mesos.coarse", "true")
    testMesos("mesos://localhost:1234", classOf[CoarseMesosSchedulerBackend])
  }

  test("mesos with zookeeper") {
    System.setProperty("spark.mesos.coarse", "false")
    testMesos("zk://localhost:1234,localhost:2345", classOf[MesosSchedulerBackend])
  }
}
