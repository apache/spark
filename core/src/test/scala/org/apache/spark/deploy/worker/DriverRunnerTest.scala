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

import java.io.File

import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.FunSuite

import org.apache.spark.deploy.{Command, DriverDescription}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import org.apache.spark.deploy.master.{WorkerInfo, DriverState, Master, DriverInfo}
import java.util.Date
import org.apache.spark.SparkConf
import akka.actor.{Props, AddressFromURIString, ActorSystem}
import akka.testkit.TestActorRef
import org.apache.spark.deploy.DeployMessages.DriverStateChanged

class DriverRunnerTest extends FunSuite {
  private def createDriverRunner() = {
    val command = new Command("mainClass", Seq(), Map())
    val driverDescription = new DriverDescription("jarUrl", 512, 1, true, command)
    val driverInfo = new DriverInfo(System.currentTimeMillis(), "driverId", driverDescription, new Date())
    new DriverRunner(driverInfo.id, driverDescription, new File("workDir"), new File("sparkHome"),
      null, "akka://1.2.3.4/worker/", new SparkConf())
  }

  private def createProcessBuilderAndProcess(): (ProcessBuilderLike, Process) = {
    val processBuilder = mock(classOf[ProcessBuilderLike])
    when(processBuilder.command).thenReturn(Seq("mocked", "command"))
    val process = mock(classOf[Process])
    when(processBuilder.start()).thenReturn(process)
    (processBuilder, process)
  }

  test("Process succeeds instantly") {
    val runner = createDriverRunner()

    val sleeper = mock(classOf[Sleeper])
    runner.setSleeper(sleeper)

    val (processBuilder, process) = createProcessBuilderAndProcess()
    // One failure then a successful run
    when(process.waitFor()).thenReturn(0)
    runner.runCommandWithRetry(processBuilder, p => (), supervise = true)

    verify(process, times(1)).waitFor()
    verify(sleeper, times(0)).sleep(anyInt())
  }

  test("Process failing several times and then succeeding") {
    val runner = createDriverRunner()
    runner.conf.set("spark.driver.maxRetry", "5")

    val sleeper = mock(classOf[Sleeper])
    runner.setSleeper(sleeper)

    val (processBuilder, process) = createProcessBuilderAndProcess()
    // fail, fail, fail, success
    when(process.waitFor()).thenReturn(-1).thenReturn(-1).thenReturn(-1).thenReturn(0)
    runner.runCommandWithRetry(processBuilder, p => (), supervise = true)

    verify(process, times(4)).waitFor()
    verify(sleeper, times(3)).sleep(anyInt())
    verify(sleeper, times(1)).sleep(1)
    verify(sleeper, times(1)).sleep(2)
    verify(sleeper, times(1)).sleep(4)
  }

  test("Process doesn't restart if not supervised") {
    val runner = createDriverRunner()

    val sleeper = mock(classOf[Sleeper])
    runner.setSleeper(sleeper)

    val (processBuilder, process) = createProcessBuilderAndProcess()
    when(process.waitFor()).thenReturn(-1)

    runner.runCommandWithRetry(processBuilder, p => (), supervise = false)

    verify(process, times(1)).waitFor()
    verify(sleeper, times(0)).sleep(anyInt())
  }

  test("Process doesn't restart if killed") {
    val runner = createDriverRunner()

    val sleeper = mock(classOf[Sleeper])
    runner.setSleeper(sleeper)

    val (processBuilder, process) = createProcessBuilderAndProcess()
    when(process.waitFor()).thenAnswer(new Answer[Int] {
      def answer(invocation: InvocationOnMock): Int = {
        runner.kill()
        -1
      }
    })

    runner.runCommandWithRetry(processBuilder, p => (), supervise = true)

    verify(process, times(1)).waitFor()
    verify(sleeper, times(0)).sleep(anyInt())
  }

  test ("Driver will be retried for several times in master end when received DriverStateChanged") {
    val actorSystem = ActorSystem("test")
    val actorRef = TestActorRef[Master](Props(classOf[Master], "127.0.0.1", 7707, 80))(actorSystem)
    val driverInfo = new DriverInfo(0, "0",
      new DriverDescription("jar/driver.jar", 1000, 4, true, null), new Date)
    actorRef.underlyingActor.conf.set("spark.driver.maxRetry", "3")
    actorRef.underlyingActor.workers += new WorkerInfo("worker-1", "127.0.0.1", 7077, 2,
      3, null, 80, "http://192.168.55.110")
    actorRef.underlyingActor.workers += new WorkerInfo("worker-2", "127.0.0.1", 7077, 2,
      3, null, 80, "http://192.168.55.110")
    actorRef.underlyingActor.drivers += ("0" -> driverInfo)
    actorRef.underlyingActor.receive(new DriverStateChanged("0", DriverState.FAILED, null))
    actorRef.underlyingActor.receive(new DriverStateChanged("0", DriverState.FAILED, null))
    assert(driverInfo.retriedcountOnMaster == 2)
  }

  test ("Driver will be retried for at most specified times in master end when received DriverStateChanged") {
    val actorSystem = ActorSystem("test")
    val actorRef = TestActorRef[Master](Props(classOf[Master], "127.0.0.1", 7707, 80))(actorSystem)
    val driverInfo = new DriverInfo(0, "0",
      new DriverDescription("jar/driver.jar", 1000, 4, true, null), new Date)
    actorRef.underlyingActor.conf.set("spark.driver.maxRetry", "1")
    actorRef.underlyingActor.workers += new WorkerInfo("worker-1", "127.0.0.1", 7077, 2,
      3, null, 80, "http://192.168.55.110")
    actorRef.underlyingActor.workers += new WorkerInfo("worker-2", "127.0.0.1", 7077, 2,
      3, null, 80, "http://192.168.55.110")
    actorRef.underlyingActor.drivers += ("0" -> driverInfo)
    actorRef.underlyingActor.receive(new DriverStateChanged("0", DriverState.FAILED, null))
    actorRef.underlyingActor.receive(new DriverStateChanged("0", DriverState.FAILED, null))
    assert(driverInfo.retriedcountOnMaster == 1)
  }

  test ("Driver will not be retried in master end if it was killed") {
    val actorSystem = ActorSystem("test")
    val actorRef = TestActorRef[Master](Props(classOf[Master], "127.0.0.1", 7707, 80))(actorSystem)
    val driverInfo = new DriverInfo(0, "0",
      new DriverDescription("jar/driver.jar", 1000, 4, true, null), new Date)
    actorRef.underlyingActor.conf.set("spark.driver.maxRetry", "3")
    actorRef.underlyingActor.drivers += ("0" -> driverInfo)
    assert(driverInfo.retriedcountOnMaster == 0)
  }

  test("Reset of backoff counter") {
    val runner = createDriverRunner()
    runner.conf.set("spark.driver.maxRetry", "5")

    val sleeper = mock(classOf[Sleeper])
    runner.setSleeper(sleeper)

    val clock = mock(classOf[Clock])
    runner.setClock(clock)

    val (processBuilder, process) = createProcessBuilderAndProcess()

    when(process.waitFor())
      .thenReturn(-1) // fail 1
      .thenReturn(-1) // fail 2
      .thenReturn(-1) // fail 3
      .thenReturn(-1) // fail 4
      .thenReturn(0) // success
    when(clock.currentTimeMillis())
      .thenReturn(0).thenReturn(1000) // fail 1 (short)
      .thenReturn(1000).thenReturn(2000) // fail 2 (short)
      .thenReturn(2000).thenReturn(10000) // fail 3 (long)
      .thenReturn(10000).thenReturn(11000) // fail 4 (short)
      .thenReturn(11000).thenReturn(21000) // success (long)

    runner.runCommandWithRetry(processBuilder, p => (), supervise = true)

    verify(sleeper, times(4)).sleep(anyInt())
    // Expected sequence of sleeps is 1,2,1,2
    verify(sleeper, times(2)).sleep(1)
    verify(sleeper, times(2)).sleep(2)
  }

}
