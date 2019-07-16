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

import org.scalatest.PrivateMethodTester

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.util.Utils


class SparkContextSchedulerCreationSuite
  extends SparkFunSuite with LocalSparkContext with PrivateMethodTester with Logging {

  def noOp(taskSchedulerImpl: TaskSchedulerImpl): Unit = {}

  def createTaskScheduler(master: String)(body: TaskSchedulerImpl => Unit = noOp): Unit =
    createTaskScheduler(master, "client")(body)

  def createTaskScheduler(master: String, deployMode: String)(
      body: TaskSchedulerImpl => Unit): Unit =
    createTaskScheduler(master, deployMode, new SparkConf())(body)

  def createTaskScheduler(
      master: String,
      deployMode: String,
      conf: SparkConf)(body: TaskSchedulerImpl => Unit): Unit = {
    // Create local SparkContext to setup a SparkEnv. We don't actually want to start() the
    // real schedulers, so we don't want to create a full SparkContext with the desired scheduler.
    sc = new SparkContext("local", "test", conf)
    val createTaskSchedulerMethod =
      PrivateMethod[Tuple2[SchedulerBackend, TaskScheduler]]('createTaskScheduler)
    val (_, sched) =
      SparkContext invokePrivate createTaskSchedulerMethod(sc, master, deployMode)
    try {
      body(sched.asInstanceOf[TaskSchedulerImpl])
    } finally {
      Utils.tryLogNonFatalError {
        sched.stop()
      }
    }
  }

  test("bad-master") {
    val e = intercept[SparkException] {
      createTaskScheduler("localhost:1234")()
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("local") {
    val sched = createTaskScheduler("local") { sched =>
      sched.backend match {
        case s: LocalSchedulerBackend => assert(s.totalCores === 1)
        case _ => fail()
      }
    }
  }

  test("local-*") {
    val sched = createTaskScheduler("local[*]") { sched =>
      sched.backend match {
        case s: LocalSchedulerBackend =>
          assert(s.totalCores === Runtime.getRuntime.availableProcessors())
        case _ => fail()
      }
    }
  }

  test("local-n") {
    val sched = createTaskScheduler("local[5]") { sched =>
      assert(sched.maxTaskFailures === 1)
      sched.backend match {
        case s: LocalSchedulerBackend => assert(s.totalCores === 5)
        case _ => fail()
      }
    }
  }

  test("local-*-n-failures") {
    val sched = createTaskScheduler("local[* ,2]") { sched =>
      assert(sched.maxTaskFailures === 2)
      sched.backend match {
        case s: LocalSchedulerBackend =>
          assert(s.totalCores === Runtime.getRuntime.availableProcessors())
        case _ => fail()
      }
    }
  }

  test("local-n-failures") {
    val sched = createTaskScheduler("local[4, 2]") { sched =>
      assert(sched.maxTaskFailures === 2)
      sched.backend match {
        case s: LocalSchedulerBackend => assert(s.totalCores === 4)
        case _ => fail()
      }
    }
  }

  test("bad-local-n") {
    val e = intercept[SparkException] {
      createTaskScheduler("local[2*]")()
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("bad-local-n-failures") {
    val e = intercept[SparkException] {
      createTaskScheduler("local[2*,4]")()
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("local-default-parallelism") {
    val conf = new SparkConf().set("spark.default.parallelism", "16")

    val sched = createTaskScheduler("local", "client", conf) { sched =>
      sched.backend match {
        case s: LocalSchedulerBackend => assert(s.defaultParallelism() === 16)
        case _ => fail()
      }
    }
  }

  test("local-cluster") {
    createTaskScheduler("local-cluster[3, 14, 1024]") { sched =>
      sched.backend match {
        case s: StandaloneSchedulerBackend => // OK
        case _ => fail()
      }
    }
  }
}
