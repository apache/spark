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
import org.apache.spark.{SSLSampleConfigs, SecurityManager, SparkConf}

class AkkaRpcEnvSuite extends RpcEnvSuite {

  override def createRpcEnv(conf: SparkConf,
      name: String,
      port: Int,
      clientMode: Boolean = false): RpcEnv = {
    new AkkaRpcEnvFactory().create(
      RpcEnvConfig(conf, name, "localhost", port, new SecurityManager(conf), clientMode))
  }

  test("setupEndpointRef: systemName, address, endpointName") {
    val ref = env.setupEndpoint("test_endpoint", new RpcEndpoint {
      override val rpcEnv = env

      override def receive = {
        case _ =>
      }
    })
    val conf = new SparkConf()
    val newRpcEnv = new AkkaRpcEnvFactory().create(
      RpcEnvConfig(conf, "test", "localhost", 0, new SecurityManager(conf), false))
    try {
      val newRef = newRpcEnv.setupEndpointRef("local", ref.address, "test_endpoint")
      assert(s"akka.tcp://local@${env.address}/user/test_endpoint" ===
        newRef.asInstanceOf[AkkaRpcEndpointRef].actorRef.path.toString)
    } finally {
      newRpcEnv.shutdown()
    }
  }

  test("uriOf") {
    val uri = env.uriOf("local", RpcAddress("1.2.3.4", 12345), "test_endpoint")
    assert("akka.tcp://local@1.2.3.4:12345/user/test_endpoint" === uri)
  }

  test("uriOf: ssl") {
    val conf = SSLSampleConfigs.sparkSSLConfig()
    val securityManager = new SecurityManager(conf)
    val rpcEnv = new AkkaRpcEnvFactory().create(
      RpcEnvConfig(conf, "test", "localhost", 0, securityManager, false))
    try {
      val uri = rpcEnv.uriOf("local", RpcAddress("1.2.3.4", 12345), "test_endpoint")
      assert("akka.ssl.tcp://local@1.2.3.4:12345/user/test_endpoint" === uri)
    } finally {
      rpcEnv.shutdown()
    }
  }

}
