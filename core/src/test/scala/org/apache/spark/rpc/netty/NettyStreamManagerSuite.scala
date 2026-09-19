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

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.rpc.RpcEnvConfig

class NettyStreamManagerSuite extends SparkFunSuite {

  private def withStreamManager(f: NettyStreamManager => Unit): Unit = {
    val conf = new SparkConf()
    val config = RpcEnvConfig(conf, "test", "localhost", "localhost", 0,
      new SecurityManager(conf), 0, clientMode = false)
    val rpcEnv = new NettyRpcEnvFactory().create(config)
    try {
      f(rpcEnv.fileServer.asInstanceOf[NettyStreamManager])
    } finally {
      rpcEnv.shutdown()
    }
  }

  test("openStream rejects path traversal outside the registered directory") {
    withTempDir { tempDir =>
      val registeredDir = new File(tempDir, "registered")
      assert(registeredDir.mkdir())
      val realFile = new File(registeredDir, "real.txt")
      Files.writeString(realFile.toPath, UUID.randomUUID().toString)

      // A file that lives outside the registered directory and must never be served.
      val secretFile = new File(tempDir, "secret.txt")
      Files.writeString(secretFile.toPath, UUID.randomUUID().toString)

      withStreamManager { manager =>
        manager.addDirectory("/classes", registeredDir)

        // Normal case: a real file under the registered directory is served.
        val buffer = manager.openStream("/classes/real.txt")
        assert(buffer != null)
        assert(buffer.size() === realFile.length())

        // Traversal is rejected.
        assert(manager.openStream("/classes/../secret.txt") === null)

        // Normalization bypass attempts are rejected.
        assert(manager.openStream("/classes/../../secret.txt") === null)
        assert(manager.openStream(s"/classes/${secretFile.getAbsolutePath}") === null)

        // A non-existent file under the registered directory is not served.
        assert(manager.openStream("/classes/nope.txt") === null)
      }
    }
  }
}
