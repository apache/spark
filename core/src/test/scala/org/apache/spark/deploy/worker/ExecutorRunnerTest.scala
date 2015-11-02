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

import org.apache.spark.deploy.{ApplicationDescription, Command, ExecutorState}
import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}

class ExecutorRunnerTest extends SparkFunSuite {
  test("command includes appId") {
    val appId = "12345-worker321-9876"
    val conf = new SparkConf
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val appDesc = new ApplicationDescription("app name", Some(8), 500,
      Command("foo", Seq(appId), Map(), Seq(), Seq(), Seq()), "appUiUrl")
    val processSetup = ChildProcessCommonSetup("1", 8, 500, "worker123", "publicAddr", 123,
      new File("ooga"), appDesc)
    val workerSetup = WorkerSetup("blah", new File(sparkHome), conf, new SecurityManager(conf))
    val er = new ExecutorRunnerImpl(processSetup, workerSetup, (x, y, z) => {},
      appId, Seq("localDir"), ExecutorState.RUNNING)
    val builder = CommandUtils.buildProcessBuilder(
      appDesc.command, new SecurityManager(conf), 512, sparkHome, er.substituteVariables)
    val builderCommand = builder.command()
    assert(builderCommand.get(builderCommand.size() - 1) === appId)
  }
}
