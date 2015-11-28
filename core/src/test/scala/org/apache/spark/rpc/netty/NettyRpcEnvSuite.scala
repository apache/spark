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

package org.apache.spark.rpc.netty

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc._

class NettyRpcEnvSuite extends RpcEnvSuite {

  override def createRpcEnv(
      conf: SparkConf,
      name: String,
      port: Int,
      clientMode: Boolean = false): RpcEnv = {
    val config = RpcEnvConfig(conf, "test", "localhost", port, new SecurityManager(conf),
      clientMode)
    new NettyRpcEnvFactory().create(config)
  }

  test("non-existent endpoint") {
    val uri = env.uriOf("test", env.address, "nonexist-endpoint")
    val e = intercept[RpcEndpointNotFoundException] {
      env.setupEndpointRef("test", env.address, "nonexist-endpoint")
    }
    assert(e.getMessage.contains(uri))
  }

}
