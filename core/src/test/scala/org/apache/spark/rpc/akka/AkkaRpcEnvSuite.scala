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

package org.apache.spark.rpc.akka

import org.apache.spark.rpc._
import org.apache.spark.{SecurityManager, SparkConf}

class AkkaRpcEnvSuite extends RpcEnvSuite {

  override def createRpcEnv(conf: SparkConf, port: Int): RpcEnv = {
    AkkaRpcEnv(RpcEnvConfig(conf, s"test-$port", "localhost", port, new SecurityManager(conf)))
  }

  test("setupEndpointRef: systemName, address, endpointName") {
    val ref = env.setupEndpoint("test_endpoint", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case _ =>
      }
    })
    val conf = new SparkConf()
    val newRpcEnv =
      AkkaRpcEnv(RpcEnvConfig(conf, "test", "localhost", 12346, new SecurityManager(conf)))
    try {
      val newRef = newRpcEnv.setupEndpointRef(s"test-${env.address.port}", ref.address, "test_endpoint")
      assert(s"akka.tcp://test-${env.address.port}@localhost:12345/user/test_endpoint" ===
        newRef.asInstanceOf[AkkaRpcEndpointRef].actorRef.path.toString)
    } finally {
      newRpcEnv.shutdown()
    }
  }

}
