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

import java.util.concurrent.{Executors, Phaser}

import scala.util.control.NonFatal

import io.grpc.{CallOptions, Channel, ClientCall, ClientInterceptor, MethodDescriptor}

import org.apache.spark.sql.test.ConnectFunSuite
import org.apache.spark.util.SparkSerDeUtils

/**
 * Tests for non-dataframe related SparkSession operations.
 */
class SparkSessionSuite extends ConnectFunSuite {
  private val connectionString1: String = "sc://test.it:17845"
  private val connectionString2: String = "sc://test.me:14099"
  private val connectionString3: String = "sc://doit:16845"

  private def closeSession(session: SparkSession): Unit = {
    // Don't call client.releaseSession on close(), because the connection details are dummy.
    session.releaseSessionOnClose = false
    session.close()
  }

  test("default") {
    val session = SparkSession.builder().getOrCreate()
    assert(session.client.configuration.host == "localhost")
    assert(session.client.configuration.port == 15002)
    closeSession(session)
  }

  test("remote") {
    val session = SparkSession.builder().remote(connectionString2).getOrCreate()
    assert(session.client.configuration.host == "test.me")
    assert(session.client.configuration.port == 14099)
    closeSession(session)
  }

  test("getOrCreate") {
    val session1 = SparkSession.builder().remote(connectionString1).getOrCreate()
    val session2 = SparkSession.builder().remote(connectionString1).getOrCreate()
    try {
      assert(session1 eq session2)
    } finally {
      closeSession(session1)
      closeSession(session2)
    }
  }

  test("create") {
    val session1 = SparkSession.builder().remote(connectionString1).create()
    val session2 = SparkSession.builder().remote(connectionString1).create()
    try {
      assert(session1 ne session2)
      assert(session1.client.configuration == session2.client.configuration)
    } finally {
      closeSession(session1)
      closeSession(session2)
    }
  }

  test("newSession") {
    val session1 = SparkSession.builder().remote(connectionString3).create()
    val session2 = session1.newSession()
    try {
      assert(session1 ne session2)
      assert(session1.client.configuration == session2.client.configuration)
    } finally {
      closeSession(session1)
      closeSession(session2)
    }
  }

  test("Custom Interceptor") {
    val session = SparkSession
      .builder()
      .interceptor(new ClientInterceptor {
        override def interceptCall[ReqT, RespT](
            methodDescriptor: MethodDescriptor[ReqT, RespT],
            callOptions: CallOptions,
            channel: Channel): ClientCall[ReqT, RespT] = {
          throw new RuntimeException("Blocked")
        }
      })
      .create()

    assertThrows[RuntimeException] {
      session.range(10).count()
    }
    closeSession(session)
  }

  test("Default/Active session") {
    // Make sure we start with a clean slate.
    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession()
    assert(SparkSession.getDefaultSession.isEmpty)
    assert(SparkSession.getActiveSession.isEmpty)
    intercept[IllegalStateException](SparkSession.active)

    // Create a session
    val session1 = SparkSession.builder().remote(connectionString1).getOrCreate()
    assert(SparkSession.getDefaultSession.contains(session1))
    assert(SparkSession.getActiveSession.contains(session1))
    assert(SparkSession.active == session1)

    // Create another session...
    val session2 = SparkSession.builder().remote(connectionString2).create()
    assert(SparkSession.getDefaultSession.contains(session1))
    assert(SparkSession.getActiveSession.contains(session1))
    SparkSession.setActiveSession(session2)
    assert(SparkSession.getDefaultSession.contains(session1))
    assert(SparkSession.getActiveSession.contains(session2))

    // Clear sessions
    SparkSession.clearDefaultSession()
    assert(SparkSession.getDefaultSession.isEmpty)
    SparkSession.clearActiveSession()
    assert(SparkSession.getDefaultSession.isEmpty)

    // Flip sessions
    SparkSession.setActiveSession(session1)
    SparkSession.setDefaultSession(session2)
    assert(SparkSession.getDefaultSession.contains(session2))
    assert(SparkSession.getActiveSession.contains(session1))

    // Close session1
    closeSession(session1)
    assert(SparkSession.getDefaultSession.contains(session2))
    assert(SparkSession.getActiveSession.isEmpty)

    // Close session2
    closeSession(session2)
    assert(SparkSession.getDefaultSession.isEmpty)
    assert(SparkSession.getActiveSession.isEmpty)
  }

  test("active session in multiple threads") {
    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession()
    val session1 = SparkSession.builder().remote(connectionString1).create()
    val session2 = SparkSession.builder().remote(connectionString1).create()
    SparkSession.setActiveSession(session2)
    assert(SparkSession.getDefaultSession.contains(session1))
    assert(SparkSession.getActiveSession.contains(session2))

    val phaser = new Phaser(2)
    val executor = Executors.newFixedThreadPool(2)
    def execute(block: Phaser => Unit): java.util.concurrent.Future[Boolean] = {
      executor.submit[Boolean] { () =>
        try {
          block(phaser)
          true
        } catch {
          case NonFatal(e) =>
            phaser.forceTermination()
            throw e
        }
      }
    }

    try {
      val script1 = execute { phaser =>
        // Step 0 - check initial state
        phaser.arriveAndAwaitAdvance()
        assert(SparkSession.getDefaultSession.contains(session1))
        assert(SparkSession.getActiveSession.contains(session2))

        // Step 1 - new active session in script 2
        phaser.arriveAndAwaitAdvance()

        // Step2 - script 1 is unchanged, script 2 has new active session
        phaser.arriveAndAwaitAdvance()
        assert(SparkSession.getDefaultSession.contains(session1))
        assert(SparkSession.getActiveSession.contains(session2))

        // Step 3 - close session 1, no more default session in both scripts
        phaser.arriveAndAwaitAdvance()
        closeSession(session1)

        // Step 4 - no default session, same active session.
        phaser.arriveAndAwaitAdvance()
        assert(SparkSession.getDefaultSession.isEmpty)
        assert(SparkSession.getActiveSession.contains(session2))

        // Step 5 - clear active session in script 1
        phaser.arriveAndAwaitAdvance()
        SparkSession.clearActiveSession()

        // Step 6 - no default/no active session in script 1, script2 unchanged.
        phaser.arriveAndAwaitAdvance()
        assert(SparkSession.getDefaultSession.isEmpty)
        assert(SparkSession.getActiveSession.isEmpty)

        // Step 7 - close active session in script2
        phaser.arriveAndAwaitAdvance()
      }
      val script2 = execute { phaser =>
        // Step 0 - check initial state
        phaser.arriveAndAwaitAdvance()
        assert(SparkSession.getDefaultSession.contains(session1))
        assert(SparkSession.getActiveSession.contains(session2))

        // Step 1 - new active session in script 2
        phaser.arriveAndAwaitAdvance()
        SparkSession.clearActiveSession()
        val internalSession = SparkSession.builder().remote(connectionString3).getOrCreate()

        // Step2 - script 1 is unchanged, script 2 has new active session
        phaser.arriveAndAwaitAdvance()
        assert(SparkSession.getDefaultSession.contains(session1))
        assert(SparkSession.getActiveSession.contains(internalSession))

        // Step 3 - close session 1, no more default session in both scripts
        phaser.arriveAndAwaitAdvance()

        // Step 4 - no default session, same active session.
        phaser.arriveAndAwaitAdvance()
        assert(SparkSession.getDefaultSession.isEmpty)
        assert(SparkSession.getActiveSession.contains(internalSession))

        // Step 5 - clear active session in script 1
        phaser.arriveAndAwaitAdvance()

        // Step 6 - no default/no active session in script 1, script2 unchanged.
        phaser.arriveAndAwaitAdvance()
        assert(SparkSession.getDefaultSession.isEmpty)
        assert(SparkSession.getActiveSession.contains(internalSession))

        // Step 7 - close active session in script2
        phaser.arriveAndAwaitAdvance()
        closeSession(internalSession)
        assert(SparkSession.getActiveSession.isEmpty)
      }
      assert(script1.get())
      assert(script2.get())
      assert(SparkSession.getActiveSession.contains(session2))
      closeSession(session2)
      assert(SparkSession.getActiveSession.isEmpty)
    } finally {
      executor.shutdown()
    }
  }

  test("deprecated methods") {
    val session = SparkSession
      .builder()
      .master("yayay")
      .appName("bob")
      .enableHiveSupport()
      .create()
    closeSession(session)
  }

  test("serialize as null") {
    val session = SparkSession.builder().create()
    val bytes = SparkSerDeUtils.serialize(session)
    assert(SparkSerDeUtils.deserialize[SparkSession](bytes) == null)
  }
}
