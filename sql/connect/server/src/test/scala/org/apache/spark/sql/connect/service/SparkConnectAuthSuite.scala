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
package org.apache.spark.sql.connect.service

// import io.grpc.StatusRuntimeException

import org.apache.spark.SparkException
import org.apache.spark.sql.connect.{SparkConnectServerTest, SparkSession}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SparkConnectService

class SparkConnectAuthSuite extends SparkConnectServerTest {
  override protected def sparkConf = {
    super.sparkConf
      .set("spark.connect.authenticate.token", "deadbeef")
      .set("spark.connect.test.marker", "server-side-only")
  }

  test("Test local authentication") {
    val session = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/;token=deadbeef")
      .create()
    session.range(5).collect()

    val invalidSession = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/;token=invalid")
      .create()
    val exception = intercept[SparkException] {
      invalidSession.range(5).collect()
    }
    assert(exception.getMessage.contains("Invalid authentication token"))
  }

  test("The authentication token is not readable through the Config RPC") {
    val session = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/;token=deadbeef")
      .create()
    val tokenKey = Connect.CONNECT_AUTHENTICATE_TOKEN.key

    // Every read path reports the key the way an unset key is reported.
    assert(session.conf.getOption(tokenKey).isEmpty)
    assert(session.conf.get(tokenKey, "fallback") === "fallback")
    intercept[NoSuchElementException] {
      session.conf.get(tokenKey)
    }
    val all = session.conf.getAll
    assert(!all.contains(tokenKey))
    // Nothing else carries the value either.
    assert(!all.exists { case (_, value) => value == "deadbeef" })

    // A control key proves the Config RPC is otherwise working and really
    // is reading server-side configuration: it is set in `sparkConf` below,
    // never sent by the client.
    assert(session.conf.get("spark.connect.test.marker") === "server-side-only")
    assert(all.get("spark.connect.test.marker").contains("server-side-only"))
  }
}
