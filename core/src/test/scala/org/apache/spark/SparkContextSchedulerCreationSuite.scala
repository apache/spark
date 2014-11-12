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

import org.scalatest.{BeforeAndAfterEach, FunSuite, PrivateMethodTester}

import org.apache.spark.scheduler.{SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{SimrSchedulerBackend, SparkDeploySchedulerBackend}
import org.apache.spark.scheduler.cluster.mesos.{CoarseMesosSchedulerBackend, MesosSchedulerBackend}
import org.apache.spark.scheduler.local.LocalBackend

class SparkContextSchedulerCreationSuite
  extends FunSuite with PrivateMethodTester with Logging with BeforeAndAfterEach {

  def createTaskScheduler(master: String): TaskSchedulerImpl = {
    // Create local SparkContext to setup a SparkEnv. We don't actually want to start() the
    // real schedulers, so we don't want to create a full SparkContext with the desired scheduler.
    val sc = new SparkContext("local", "test")
    val createTaskSchedulerMethod =
      PrivateMethod[Tuple2[SchedulerBackend, TaskScheduler]]('createTaskScheduler)
    val (_, sched) = SparkContext invokePrivate createTaskSchedulerMethod(sc, master)
    sched.asInstanceOf[TaskSchedulerImpl]
  }

  test("bad-master") {
    val e = intercept[SparkException] {
      createTaskScheduler("localhost:1234")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("local") {
    val sched = createTaskScheduler("local")
    sched.backend match {
      case s: LocalBackend => assert(s.totalCores === 1)
      case _ => fail()
    }
  }

  test("local-*") {
    val sched = createTaskScheduler("local[*]")
    sched.backend match {
      case s: LocalBackend => assert(s.totalCores === Runtime.getRuntime.availableProcessors())
      case _ => fail()
    }
  }

  test("local-n") {
    val sched = createTaskScheduler("local[5]")
    assert(sched.maxTaskFailures === 1)
    sched.backend match {
      case s: LocalBackend => assert(s.totalCores === 5)
      case _ => fail()
    }
  }

  test("local-*-n-failures") {
    val sched = createTaskScheduler("local[* ,2]")
    assert(sched.maxTaskFailures === 2)
    sched.backend match {
      case s: LocalBackend => assert(s.totalCores === Runtime.getRuntime.availableProcessors())
      case _ => fail()
    }
  }

  test("local-n-failures") {
    val sched = createTaskScheduler("local[4, 2]")
    assert(sched.maxTaskFailures === 2)
    sched.backend match {
      case s: LocalBackend => assert(s.totalCores === 4)
      case _ => fail()
    }
  }

  test("bad-local-n") {
    val e = intercept[SparkException] {
      createTaskScheduler("local[2*]")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("bad-local-n-failures") {
    val e = intercept[SparkException] {
      createTaskScheduler("local[2*,4]")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("local-default-parallelism") {
    val defaultParallelism = System.getProperty("spark.default.parallelism")
    System.setProperty("spark.default.parallelism", "16")
    val sched = createTaskScheduler("local")

    sched.backend match {
      case s: LocalBackend => assert(s.defaultParallelism() === 16)
      case _ => fail()
    }

    Option(defaultParallelism) match {
      case Some(v) => System.setProperty("spark.default.parallelism", v)
      case _ => System.clearProperty("spark.default.parallelism")
    }
  }

  test("simr") {
    createTaskScheduler("simr://uri").backend match {
      case s: SimrSchedulerBackend => // OK
      case _ => fail()
    }
  }

  test("local-cluster") {
    createTaskScheduler("local-cluster[3, 14, 512]").backend match {
      case s: SparkDeploySchedulerBackend => // OK
      case _ => fail()
    }
  }

  def testYarn(master: String, expectedClassName: String) {
    try {
      val sched = createTaskScheduler(master)
      assert(sched.getClass === Class.forName(expectedClassName))
    } catch {
      case e: SparkException =>
        assert(e.getMessage.contains("YARN mode not available"))
        logWarning("YARN not available, could not test actual YARN scheduler creation")
      case e: Throwable => fail(e)
    }
  }

  test("yarn-cluster") {
    testYarn("yarn-cluster", "org.apache.spark.scheduler.cluster.YarnClusterScheduler")
  }

  test("yarn-standalone") {
    testYarn("yarn-standalone", "org.apache.spark.scheduler.cluster.YarnClusterScheduler")
  }

  test("yarn-client") {
    testYarn("yarn-client", "org.apache.spark.scheduler.cluster.YarnClientClusterScheduler")
  }

  def testMesos(master: String, expectedClass: Class[_]) {
    try {
      val sched = createTaskScheduler(master)
      assert(sched.backend.getClass === expectedClass)
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
