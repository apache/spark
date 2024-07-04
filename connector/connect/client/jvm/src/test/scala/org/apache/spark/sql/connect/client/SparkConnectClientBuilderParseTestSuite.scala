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
package org.apache.spark.sql.connect.client

import java.util.UUID

import org.apache.spark.sql.test.ConnectFunSuite

/**
 * Test suite for [[SparkConnectClient.Builder]] parsing and configuration.
 */
class SparkConnectClientBuilderParseTestSuite extends ConnectFunSuite {
  private def build(args: String*): SparkConnectClient.Builder = {
    SparkConnectClient.builder().parse(args.toArray)
  }

  private def argumentTest(
      name: String,
      value: String,
      extractor: SparkConnectClient.Builder => String): Unit = {
    test("Argument - " + name) {
      val builder = build("--" + name, value)
      assert(value === extractor(builder))
      val e = intercept[IllegalArgumentException] {
        build("--" + name)
      }
      assert(e.getMessage.contains("option requires a value"))
    }
  }

  argumentTest("host", "www.apache.org", _.host)
  argumentTest("port", "1506", _.port.toString)
  argumentTest("token", "azbycxdwev1234567890", _.token.get)
  argumentTest("user_id", "U1238", _.userId.get)
  argumentTest("user_name", "alice", _.userName.get)
  argumentTest("user_agent", "robert", _.userAgent.split(" ")(0))
  argumentTest("session_id", UUID.randomUUID().toString, _.sessionId.get)

  test("Argument - remote") {
    val builder =
      build("--remote", "sc://srv.apache.org/;user_id=x127;user_name=Q;token=nahnah;param1=x")
    assert(builder.host === "srv.apache.org")
    assert(builder.port === 15002)
    assert(builder.token.contains("nahnah"))
    assert(builder.userId.contains("x127"))
    assert(builder.options === Map(("user_name", "Q"), ("param1", "x")))
    assert(builder.sessionId.isEmpty)
  }

  test("Argument - use_ssl") {
    val builder = build("--use_ssl")
    assert(builder.sslEnabled)
  }

  test("Argument - option") {
    val builder =
      build("--option", "foo=bar", "--option", "c1=s8", "--option", "ns.sns.setting=baz")
    assert(builder.options === Map(("foo", "bar"), ("c1", "s8"), ("ns.sns.setting", "baz")))
    val e1 = intercept[NoSuchElementException](build("--option"))
    // assert(e1.getMessage.contains("requires a key-value pair"))
    intercept[MatchError](build("--option", "not_a_config"))
    val e2 = intercept[IllegalArgumentException](build("--option", "bar=baz=bak"))
    assert(e2.getMessage.contains("should contain key=value"))
  }

  test("Argument - unsupported") {
    val e = intercept[IllegalArgumentException](build("--unknown"))
    assert(e.getMessage.contains("is an unsupported argument"))
  }

  test("SparkSession - create") {
    {
      val builder = build(
        "--remote",
        "sc://localhost:15033",
        "--port",
        "1507",
        "--user_agent",
        "U8912",
        "--user_id",
        "Q12")
      assert(builder.host === "localhost")
      assert(builder.port === 1507)
      assert(builder.userAgent.contains("U8912"))
      assert(!builder.sslEnabled)
      assert(builder.token.isEmpty)
      assert(builder.userId.contains("Q12"))
      assert(builder.userName.isEmpty)
      assert(builder.options.isEmpty)
    }
    {
      val builder = build(
        "--use_ssl",
        "--user_name",
        "Nico",
        "--option",
        "mode=turbo",
        "--option",
        "cluster=mycl")
      assert(builder.host === "localhost")
      assert(builder.port === 15002)
      assert(builder.userAgent.contains("_SPARK_CONNECT_SCALA"))
      assert(builder.sslEnabled)
      assert(builder.token.isEmpty)
      assert(builder.userId.isEmpty)
      assert(builder.userName.contains("Nico"))
      assert(builder.options === Map(("mode", "turbo"), ("cluster", "mycl")))
    }
    {
      val builder = build("--token", "thisismysecret")
      assert(builder.host === "localhost")
      assert(builder.port === 15002)
      assert(builder.userAgent.contains("_SPARK_CONNECT_SCALA"))
      assert(builder.sslEnabled)
      assert(builder.token.contains("thisismysecret"))
      assert(builder.userId.isEmpty)
      assert(builder.userName.isEmpty)
      assert(builder.options.isEmpty)
    }
  }
}
