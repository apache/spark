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

package org.apache.spark.deploy

import java.io.File
import java.util.Date

import org.apache.spark.deploy.master.{ApplicationInfo, DriverInfo, WorkerInfo}
import org.apache.spark.deploy.worker.{DriverRunner, ExecutorRunner}
import org.apache.spark.{SecurityManager, SparkConf}

private[deploy] object DeployTestUtils {
  def createAppDesc(): ApplicationDescription = {
    val cmd = new Command("mainClass", List("arg1", "arg2"), Map(), Seq(), Seq(), Seq())
    new ApplicationDescription("name", Some(4), 1234, cmd, "appUiUrl")
  }

  def createAppInfo() : ApplicationInfo = {
    val appDesc = createAppDesc()
    val appInfo = new ApplicationInfo(JsonConstants.appInfoStartTime,
      "id", appDesc, JsonConstants.submitDate, null, Int.MaxValue)
    appInfo.endTime = JsonConstants.currTimeInMillis
    appInfo
  }

  def createDriverCommand(): Command = new Command(
    "org.apache.spark.FakeClass", Seq("some arg --and-some options -g foo"),
    Map(("K1", "V1"), ("K2", "V2")), Seq("cp1", "cp2"), Seq("lp1", "lp2"), Seq("-Dfoo")
  )

  def createDriverDesc(): DriverDescription =
    new DriverDescription("hdfs://some-dir/some.jar", 100, 3, false, createDriverCommand())

  def createDriverInfo(): DriverInfo = new DriverInfo(3, "driver-3",
    createDriverDesc(), new Date())

  def createWorkerInfo(): WorkerInfo = {
    val workerInfo = new WorkerInfo("id", "host", 8080, 4, 1234, null, 80, "publicAddress")
    workerInfo.lastHeartbeat = JsonConstants.currTimeInMillis
    workerInfo
  }

  def createExecutorRunner(execId: Int): ExecutorRunner = {
    new ExecutorRunner(
      "appId",
      execId,
      createAppDesc(),
      4,
      1234,
      null,
      "workerId",
      "host",
      123,
      "publicAddress",
      new File("sparkHome"),
      new File("workDir"),
      "akka://worker",
      new SparkConf,
      Seq("localDir"),
      ExecutorState.RUNNING)
  }

  def createDriverRunner(driverId: String): DriverRunner = {
    val conf = new SparkConf()
    new DriverRunner(
      conf,
      driverId,
      new File("workDir"),
      new File("sparkHome"),
      createDriverDesc(),
      null,
      "akka://worker",
      new SecurityManager(conf))
  }
}
