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

package org.apache.spark.executor

import java.io.File

import scala.collection.mutable.HashMap

import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.util.{MutableURLClassLoader, Utils}

/**
 * Unit tests for IsolatedSessionState lifecycle management.
 * These tests verify the fix for race conditions in session acquire/release/eviction.
 */
class ExecutorSideSessionManagementSuite
    extends SparkFunSuite
    with BeforeAndAfterEach
    with MockitoSugar {

  private var testSessionCounter = 0
  private var tempDir: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear the sessions map before each test
    IsolatedSessionState.sessions.clear()
    testSessionCounter = 0

    // Set up a mock SparkEnv so that cleanup() can access SparkFiles.getRootDirectory()
    tempDir = Utils.createTempDir()
    val mockEnv = mock[SparkEnv]
    val conf = new SparkConf(false)
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.driverTmpDir).thenReturn(Some(tempDir.getAbsolutePath))
    SparkEnv.set(mockEnv)
  }

  override def afterEach(): Unit = {
    // Clear the sessions map after each test
    IsolatedSessionState.sessions.clear()
    SparkEnv.set(null)
    if (tempDir != null && tempDir.exists()) {
      Utils.deleteRecursively(tempDir)
      tempDir = null
    }
    super.afterEach()
  }

  /**
   * Creates a test IsolatedSessionState with a mock classloader and unique UUID.
   */
  private def createTestSession(uuid: String): IsolatedSessionState = {
    val classLoader = new MutableURLClassLoader(
      Array.empty,
      Thread.currentThread().getContextClassLoader
    )
    val session = new IsolatedSessionState(
      sessionUUID = uuid,
      urlClassLoader = classLoader,
      replClassLoader = classLoader,
      currentFiles = new HashMap[String, Long](),
      currentJars = new HashMap[String, Long](),
      currentArchives = new HashMap[String, Long](),
      replClassDirUri = None
    )
    // Register in authoritative sessions map as would happen in production
    IsolatedSessionState.sessions.put(uuid, session)
    session
  }

  private def nextUniqueUuid(): String = {
    testSessionCounter += 1
    s"test-uuid-$testSessionCounter"
  }

  test("acquire returns true for new session") {
    val session = createTestSession(nextUniqueUuid())
    assert(session.acquire())
  }

  test("acquire returns true for session acquired multiple times") {
    val session = createTestSession(nextUniqueUuid())
    assert(session.acquire())
    assert(session.acquire())
    assert(session.acquire())
  }

  test("acquire returns false after session is evicted with no references") {
    val session = createTestSession(nextUniqueUuid())
    session.markEvicted()
    // Session should be cleaned up immediately since refCount is 0
    assert(!IsolatedSessionState.sessions.containsKey(session.sessionUUID))
    // Cannot acquire an evicted session.
    assert(!session.acquire())
  }

  test("acquire returns false after session is evicted even with existing references") {
    val uuid = nextUniqueUuid()
    val session = createTestSession(uuid)

    // First task acquires the session
    assert(session.acquire())

    // Session gets evicted (e.g., due to cache pressure)
    session.markEvicted()
    // Session should still be in the map because refCount > 0 (deferred cleanup)
    assert(IsolatedSessionState.sessions.containsKey(uuid))

    // A new task tries to acquire the same session - should fail because it's evicted
    assert(!session.acquire())

    // The original task releases - now cleanup happens
    session.release()
    assert(!IsolatedSessionState.sessions.containsKey(uuid))
  }

  test("deferred cleanup with multiple references") {
    val uuid = nextUniqueUuid()
    val session = createTestSession(uuid)

    // Acquire the session multiple times (simulating multiple tasks)
    assert(session.acquire())
    assert(session.acquire())
    assert(session.acquire())

    // Evict the session - cleanup should be deferred
    session.markEvicted()
    assert(IsolatedSessionState.sessions.containsKey(uuid))

    // Release twice - cleanup should still be deferred
    session.release()
    assert(IsolatedSessionState.sessions.containsKey(uuid))
    session.release()
    assert(IsolatedSessionState.sessions.containsKey(uuid))

    // Release the last reference - cleanup should happen now
    session.release()
    assert(!IsolatedSessionState.sessions.containsKey(uuid))
  }

  test("tryUnEvict succeeds when session is evicted but still has references") {
    val session = createTestSession(nextUniqueUuid())

    // Acquire the session
    assert(session.acquire())

    // Evict the session
    session.markEvicted()

    // Try to un-evict - should succeed because refCount > 0
    assert(session.tryUnEvict())

    // Now acquire should succeed again
    assert(session.acquire())
  }

  test("tryUnEvict fails when session is not evicted") {
    val session = createTestSession(nextUniqueUuid())

    // Acquire without eviction
    assert(session.acquire())

    // Try to un-evict - should fail because session is not evicted
    assert(!session.tryUnEvict())
  }

  test("tryUnEvict and acquire fail when session has no references") {
    val uuid = nextUniqueUuid()
    val session = createTestSession(uuid)

    // Evict with no references - triggers immediate cleanup
    session.markEvicted()
    assert(!IsolatedSessionState.sessions.containsKey(uuid))

    // tryUnEvict should fail because refCount is 0 and session is already cleaned up
    assert(!session.tryUnEvict())

    // acquire should also fail
    assert(!session.acquire())
  }

  test("session reuse via tryUnEvict keeps session in map when not evicted") {
    // Note: This test verifies `IsolatedSessionState.sessions` behavior in isolation.
    // In production, after tryUnEvict(), the session is put back into the Guava cache.
    // When the cache eventually evicts it again (due to LRU policy), markEvicted() will be called,
    // and cleanup will happen if refCount is 0. So there's no resource leak in practice.
    val uuid = nextUniqueUuid()
    val session = createTestSession(uuid)

    // Simulate task 1 acquiring the session
    assert(session.acquire())

    // Session gets evicted (e.g., due to cache pressure)
    session.markEvicted()
    assert(IsolatedSessionState.sessions.containsKey(uuid)) // Deferred cleanup

    // Simulate cache loader trying to reuse the session via tryUnEvict
    assert(session.tryUnEvict())

    // Now a new task can acquire the session
    assert(session.acquire())

    // Task 1 releases
    session.release()
    assert(IsolatedSessionState.sessions.containsKey(uuid)) // Still has 1 reference

    // Task 2 releases - session stays in map because it's not evicted
    session.release()
    // Session stays in map because it's not evicted anymore (was un-evicted).
    // In production, the Guava cache would eventually evict it again, triggering cleanup.
    assert(IsolatedSessionState.sessions.containsKey(uuid))
  }
}
