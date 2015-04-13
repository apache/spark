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

package org.apache.spark.deploy.client

import org.apache.spark.{SecurityManager, SparkConf, Logging}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.util.{AkkaUtils, Utils}

private[spark] object TestClient {

  private class TestListener extends AppClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got app ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }

    def dead(reason: String) {
      logInfo("Application died with error: " + reason)
      System.exit(0)
    }

    def executorAdded(id: String, workerId: String, hostPort: String, cores: Int, memory: Int) {}

    def executorRemoved(id: String, message: String, exitStatus: Option[Int]) {}
  }

  def main(args: Array[String]) {
    val url = args(0)
    val conf = new SparkConf
    val (actorSystem, _) = AkkaUtils.createActorSystem("spark", Utils.localHostName(), 0,
      conf = conf, securityManager = new SecurityManager(conf))
    val desc = new ApplicationDescription("TestClient", Some(1), 512,
      Command("spark.deploy.client.TestExecutor", Seq(), Map(), Seq(), Seq(), Seq()), "ignored")
    val listener = new TestListener
    val client = new AppClient(actorSystem, Array(url), desc, listener, new SparkConf)
    client.start()
    actorSystem.awaitTermination()
  }
}
