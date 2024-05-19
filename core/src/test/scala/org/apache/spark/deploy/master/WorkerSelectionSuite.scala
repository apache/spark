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

package org.apache.spark.deploy.master

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.internal.config.Deploy.WorkerSelectionPolicy._
import org.apache.spark.internal.config.UI._
import org.apache.spark.rpc.RpcAddress

class WorkerSelectionSuite extends MasterSuiteBase {

  private val workerSelectionPolicyTestCases = Seq(
    (CORES_FREE_ASC, true, List("10001", "10002")),
    (CORES_FREE_ASC, false, List("10001")),
    (CORES_FREE_DESC, true, List("10002", "10003")),
    (CORES_FREE_DESC, false, List("10003")),
    (MEMORY_FREE_ASC, true, List("10001", "10003")),
    (MEMORY_FREE_ASC, false, List("10001")),
    (MEMORY_FREE_DESC, true, List("10002", "10003")),
    (MEMORY_FREE_DESC, false, Seq("10002")),
    (WORKER_ID, true, Seq("10001", "10002")),
    (WORKER_ID, false, Seq("10001")))

  workerSelectionPolicyTestCases.foreach { case (policy, spreadOut, expected) =>
    test(s"SPARK-46881: scheduling with workerSelectionPolicy - $policy ($spreadOut)") {
      val conf = new SparkConf()
        .set(WORKER_SELECTION_POLICY.key, policy.toString)
        .set(SPREAD_OUT_APPS.key, spreadOut.toString)
        .set(UI_ENABLED.key, "false")
        .set(Network.RPC_NETTY_DISPATCHER_NUM_THREADS, 1)
        .set(Network.RPC_IO_THREADS, 1)
      val master = makeAliveMaster(conf)

      // Use different core and memory values to simplify the tests
      MockWorker.counter.set(10000)
      (1 to 3).foreach { idx =>
        val worker = new MockWorker(master.self, conf)
        worker.rpcEnv.setupEndpoint(s"worker-$idx", worker)
        val workerReg = RegisterWorker(
          worker.id,
          "localhost",
          worker.self.address.port,
          worker.self,
          4 + idx,
          1280 * (if (idx < 2) idx else (6 - idx)),
          "http://localhost:8080",
          RpcAddress("localhost", 10000))
        master.self.send(workerReg)
        eventually(timeout(10.seconds)) {
          assert(master.self.askSync[MasterStateResponse](RequestMasterState).workers.size === idx)
        }
      }

      // An application with two executors
      val appInfo = makeAppInfo(128, Some(2), Some(4))
      master.registerApplication(appInfo)
      startExecutorsOnWorkers(master)
      assert(appInfo.executors.map(_._2.worker.id).toSeq.distinct.sorted === expected)
    }
  }
}
