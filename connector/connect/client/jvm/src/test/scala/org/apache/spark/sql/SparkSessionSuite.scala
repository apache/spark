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
package org.apache.spark.sql

import org.apache.spark.sql.connect.client.util.ConnectFunSuite

/**
 * Tests for non-dataframe related SparkSession operations.
 */
class SparkSessionSuite extends ConnectFunSuite {
  test("default") {
    val session = SparkSession.builder().getOrCreate()
    assert(session.client.configuration.host == "localhost")
    assert(session.client.configuration.port == 15002)
    session.close()
  }

  test("remote") {
    val session = SparkSession.builder().remote("sc://test.me:14099").getOrCreate()
    assert(session.client.configuration.host == "test.me")
    assert(session.client.configuration.port == 14099)
    session.close()
  }

  test("getOrCreate") {
    val connectionString = "sc://test.it:17865"
    val session1 = SparkSession.builder().remote(connectionString).getOrCreate()
    val session2 = SparkSession.builder().remote(connectionString).getOrCreate()
    try {
      assert(session1 eq session2)
    } finally {
      session1.close()
      session2.close()
    }
  }

  test("create") {
    val connectionString = "sc://test.it:17845"
    val session1 = SparkSession.builder().remote(connectionString).create()
    val session2 = SparkSession.builder().remote(connectionString).create()
    try {
      assert(session1 ne session2)
      assert(session1.client.configuration == session2.client.configuration)
    } finally {
      session1.close()
      session2.close()
    }
  }

  test("newSession") {
    val connectionString = "sc://doit:16845"
    val session1 = SparkSession.builder().remote(connectionString).create()
    val session2 = session1.newSession()
    try {
      assert(session1 ne session2)
      assert(session1.client.configuration == session2.client.configuration)
    } finally {
      session1.close()
      session2.close()
    }
  }
}
