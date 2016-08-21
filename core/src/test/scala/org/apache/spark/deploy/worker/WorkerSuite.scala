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

package org.apache.spark.deploy.worker

import org.scalatest.Matchers

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.{Command, ExecutorState}
import org.apache.spark.deploy.DeployMessages.{DriverStateChanged, ExecutorStateChanged}
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.rpc.{RpcAddress, RpcEnv}

class WorkerSuite extends SparkFunSuite with Matchers {

  import org.apache.spark.deploy.DeployTestUtils._

  def cmd(javaOpts: String*): Command = {
    Command("", Seq.empty, Map.empty, Seq.empty, Seq.empty, Seq(javaOpts : _*))
  }
  def conf(opts: (String, String)*): SparkConf = new SparkConf(loadDefaults = false).setAll(opts)

  test("test isUseLocalNodeSSLConfig") {
    Worker.isUseLocalNodeSSLConfig(cmd("-Dasdf=dfgh")) shouldBe false
    Worker.isUseLocalNodeSSLConfig(cmd("-Dspark.ssl.useNodeLocalConf=true")) shouldBe true
    Worker.isUseLocalNodeSSLConfig(cmd("-Dspark.ssl.useNodeLocalConf=false")) shouldBe false
    Worker.isUseLocalNodeSSLConfig(cmd("-Dspark.ssl.useNodeLocalConf=")) shouldBe false
  }

  test("test maybeUpdateSSLSettings") {
    Worker.maybeUpdateSSLSettings(
      cmd("-Dasdf=dfgh", "-Dspark.ssl.opt1=x"),
      conf("spark.ssl.opt1" -> "y", "spark.ssl.opt2" -> "z"))
        .javaOpts should contain theSameElementsInOrderAs Seq(
          "-Dasdf=dfgh", "-Dspark.ssl.opt1=x")

    Worker.maybeUpdateSSLSettings(
      cmd("-Dspark.ssl.useNodeLocalConf=false", "-Dspark.ssl.opt1=x"),
      conf("spark.ssl.opt1" -> "y", "spark.ssl.opt2" -> "z"))
        .javaOpts should contain theSameElementsInOrderAs Seq(
          "-Dspark.ssl.useNodeLocalConf=false", "-Dspark.ssl.opt1=x")

    Worker.maybeUpdateSSLSettings(
      cmd("-Dspark.ssl.useNodeLocalConf=true", "-Dspark.ssl.opt1=x"),
      conf("spark.ssl.opt1" -> "y", "spark.ssl.opt2" -> "z"))
        .javaOpts should contain theSameElementsAs Seq(
          "-Dspark.ssl.useNodeLocalConf=true", "-Dspark.ssl.opt1=y", "-Dspark.ssl.opt2=z")

  }

  test("test clearing of finishedExecutors (small number of executors)") {
    val conf = new SparkConf()
    conf.set("spark.worker.ui.retainedExecutors", 2.toString)
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, new SecurityManager(conf))
    val worker = new Worker(rpcEnv, 50000, 20, 1234 * 5, Array.fill(1)(RpcAddress("1.2.3.4", 1234)),
      "Worker", "/tmp", conf, new SecurityManager(conf))
    // initialize workers
    for (i <- 0 until 5) {
      worker.executors += s"app1/$i" -> createExecutorRunner(i)
    }
    // initialize ExecutorStateChanged Message
    worker.handleExecutorStateChanged(
      ExecutorStateChanged("app1", 0, ExecutorState.EXITED, None, None))
    assert(worker.finishedExecutors.size === 1)
    assert(worker.executors.size === 4)
    for (i <- 1 until 5) {
      worker.handleExecutorStateChanged(
        ExecutorStateChanged("app1", i, ExecutorState.EXITED, None, None))
      assert(worker.finishedExecutors.size === 2)
      if (i > 1) {
        assert(!worker.finishedExecutors.contains(s"app1/${i - 2}"))
      }
      assert(worker.executors.size === 4 - i)
    }
  }

  test("test clearing of finishedExecutors (more executors)") {
    val conf = new SparkConf()
    conf.set("spark.worker.ui.retainedExecutors", 30.toString)
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, new SecurityManager(conf))
    val worker = new Worker(rpcEnv, 50000, 20, 1234 * 5, Array.fill(1)(RpcAddress("1.2.3.4", 1234)),
      "Worker", "/tmp", conf, new SecurityManager(conf))
    // initialize workers
    for (i <- 0 until 50) {
      worker.executors += s"app1/$i" -> createExecutorRunner(i)
    }
    // initialize ExecutorStateChanged Message
    worker.handleExecutorStateChanged(
      ExecutorStateChanged("app1", 0, ExecutorState.EXITED, None, None))
    assert(worker.finishedExecutors.size === 1)
    assert(worker.executors.size === 49)
    for (i <- 1 until 50) {
      val expectedValue = {
        if (worker.finishedExecutors.size < 30) {
          worker.finishedExecutors.size + 1
        } else {
          28
        }
      }
      worker.handleExecutorStateChanged(
        ExecutorStateChanged("app1", i, ExecutorState.EXITED, None, None))
      if (expectedValue == 28) {
        for (j <- i - 30 until i - 27) {
          assert(!worker.finishedExecutors.contains(s"app1/$j"))
        }
      }
      assert(worker.executors.size === 49 - i)
      assert(worker.finishedExecutors.size === expectedValue)
    }
  }

  test("test clearing of finishedDrivers (small number of drivers)") {
    val conf = new SparkConf()
    conf.set("spark.worker.ui.retainedDrivers", 2.toString)
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, new SecurityManager(conf))
    val worker = new Worker(rpcEnv, 50000, 20, 1234 * 5, Array.fill(1)(RpcAddress("1.2.3.4", 1234)),
      "Worker", "/tmp", conf, new SecurityManager(conf))
    // initialize workers
    for (i <- 0 until 5) {
      val driverId = s"driverId-$i"
      worker.drivers += driverId -> createDriverRunner(driverId)
    }
    // initialize DriverStateChanged Message
    worker.handleDriverStateChanged(DriverStateChanged("driverId-0", DriverState.FINISHED, None))
    assert(worker.drivers.size === 4)
    assert(worker.finishedDrivers.size === 1)
    for (i <- 1 until 5) {
      val driverId = s"driverId-$i"
      worker.handleDriverStateChanged(DriverStateChanged(driverId, DriverState.FINISHED, None))
      if (i > 1) {
        assert(!worker.finishedDrivers.contains(s"driverId-${i - 2}"))
      }
      assert(worker.drivers.size === 4 - i)
      assert(worker.finishedDrivers.size === 2)
    }
  }

  test("test clearing of finishedDrivers (more drivers)") {
    val conf = new SparkConf()
    conf.set("spark.worker.ui.retainedDrivers", 30.toString)
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, new SecurityManager(conf))
    val worker = new Worker(rpcEnv, 50000, 20, 1234 * 5, Array.fill(1)(RpcAddress("1.2.3.4", 1234)),
      "Worker", "/tmp", conf, new SecurityManager(conf))
    // initialize workers
    for (i <- 0 until 50) {
      val driverId = s"driverId-$i"
      worker.drivers += driverId -> createDriverRunner(driverId)
    }
    // initialize DriverStateChanged Message
    worker.handleDriverStateChanged(DriverStateChanged("driverId-0", DriverState.FINISHED, None))
    assert(worker.finishedDrivers.size === 1)
    assert(worker.drivers.size === 49)
    for (i <- 1 until 50) {
      val expectedValue = {
        if (worker.finishedDrivers.size < 30) {
          worker.finishedDrivers.size + 1
        } else {
          28
        }
      }
      val driverId = s"driverId-$i"
      worker.handleDriverStateChanged(DriverStateChanged(driverId, DriverState.FINISHED, None))
      if (expectedValue == 28) {
        for (j <- i - 30 until i - 27) {
          assert(!worker.finishedDrivers.contains(s"driverId-$j"))
        }
      }
      assert(worker.drivers.size === 49 - i)
      assert(worker.finishedDrivers.size === expectedValue)
    }
  }
}
