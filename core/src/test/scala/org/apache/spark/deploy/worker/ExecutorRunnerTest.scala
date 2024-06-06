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

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.{ApplicationDescription, Command, DeployTestUtils, ExecutorState}
import org.apache.spark.resource.ResourceProfile

class ExecutorRunnerTest extends SparkFunSuite {
  test("command includes appId") {
    val appId = "12345-worker321-9876"
    val conf = new SparkConf()
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val appDesc = new ApplicationDescription("app name", Some(8),
      Command("foo", Seq(appId), Map(), Seq(), Seq(), Seq()), "appUiUrl",
      DeployTestUtils.defaultResourceProfile)
    val er = new ExecutorRunner(appId, 1, appDesc, 8, 1234, null, "blah", "http://", "worker321",
      123, "publicAddr", new File(sparkHome), new File("ooga"), "blah", conf, Seq("localDir"),
      ExecutorState.RUNNING, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val builder = CommandUtils.buildProcessBuilder(
      appDesc.command, new SecurityManager(conf), 1234, sparkHome, er.substituteVariables)
    val builderCommand = builder.command()
    assert(builderCommand.get(builderCommand.size() - 1) === appId)
  }
}
