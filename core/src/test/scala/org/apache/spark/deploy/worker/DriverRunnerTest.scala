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
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSuite

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{Command, DriverDescription}
import org.apache.spark.util.Clock

class DriverRunnerTest extends FunSuite {
  private def createDriverRunner() = {
    val command = new Command("mainClass", Seq(), Map(), Seq(), Seq(), Seq())
    val driverDescription = new DriverDescription("jarUrl", 512, 1, true, command)
    val conf = new SparkConf()
    new DriverRunner(conf, "driverId", new File("workDir"), new File("sparkHome"),
      driverDescription, null, "akka://1.2.3.4/worker/", new SecurityManager(conf))
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

  test("Reset of backoff counter") {
    val runner = createDriverRunner()

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
    when(clock.getTimeMillis())
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
