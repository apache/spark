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
import org.apache.spark.sql.connect.service.SparkConnectService

class SparkConnectAuthSuite extends SparkConnectServerTest {
  private val tokenKey = "spark.connect.authenticate.token"
  private val markerKey = "spark.connect.test.marker"

  override protected def sparkConf = {
    super.sparkConf
      .set(tokenKey, "deadbeef")
      .set(markerKey, "visible")
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

  test("Test the authentication token is not readable through the Config RPC") {
    val session = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/;token=deadbeef")
      .create()

    assert(session.conf.getOption(tokenKey).isEmpty)
    assert(session.conf.get(tokenKey, "absent") === "absent")
    intercept[NoSuchElementException](session.conf.get(tokenKey))
    assert(!session.conf.getAll.contains(tokenKey))

    // Server-side configurations that are not sensitive stay readable.
    assert(session.conf.get(markerKey) === "visible")
  }
}
