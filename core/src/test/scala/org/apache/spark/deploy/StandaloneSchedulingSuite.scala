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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark._
import org.apache.spark.deploy.master._
import org.apache.spark.rpc.RpcEnv

/**
 * The tests for setting up scheduling across Spark applications in standalone mode.
 */
class StandaloneSchedulingSuite extends SparkFunSuite {

  test("default scheduling mode") {
    val conf = new SparkConf()
    val securityManager = new SecurityManager(conf)
    val masterRpcEnv: RpcEnv =
      RpcEnv.create(Master.SYSTEM_NAME, "localhost", 0, conf, securityManager)

    val master = new Master(masterRpcEnv, masterRpcEnv.address, 0, securityManager, conf)
    val schedulingSetting = master.schedulingSetting
    assert(schedulingSetting.mode == SchedulingMode.FIFO)
    assert(schedulingSetting.configFile == None)
  }
}
