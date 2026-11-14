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

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import io.grpc.stub.StreamObserver
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.pipelines.graph.{DataflowGraph, PipelineUpdateContextImpl}
import org.apache.spark.sql.pipelines.logging.PipelineEvent
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectSessionManagerSuite extends SharedSparkSession {

  override def beforeEach(): Unit = {
    super.beforeEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
    SparkConnectService.sessionManager.initializeBaseSession(() => spark.newSession())
  }

  test("sessionId needs to be an UUID") {
    val key = SessionKey("user", "not an uuid")
    val exGetOrCreate = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    }
    assert(exGetOrCreate.getCondition == "INVALID_HANDLE.FORMAT")
  }

  test(
    "getOrCreateIsolatedSession/getIsolatedSession/getIsolatedSessionIfPresent " +
      "gets the existing session") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)

    val sessionGetOrCreate =
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    assert(sessionGetOrCreate === sessionHolder)

    val sessionGet = SparkConnectService.sessionManager.getIsolatedSession(key, None)
    assert(sessionGet === sessionHolder)

    val sessionGetIfPresent = SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key)
    assert(sessionGetIfPresent.get === sessionHolder)
  }

  test("client-observed session id validation works") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    // Works if the client doesn't set the observed session id.
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    // Works with the correct existing session id.
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(
      key,
      Some(sessionHolder.session.sessionUUID))
    // Fails with the different session id.
    val exGet = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(
        key,
        Some(sessionHolder.session.sessionUUID + "invalid"))
    }
    assert(exGet.getCondition == "INVALID_HANDLE.SESSION_CHANGED")
    assert(exGet.getSqlState == "08003")
  }

  private def assertSessionChangedAndNotCreated(key: SessionKey, observedId: String): Unit = {
    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, Some(observedId))
    }
    assert(ex.getCondition == "INVALID_HANDLE.SESSION_CHANGED")
    assert(SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key).isEmpty)
    assert(!SparkConnectService.sessionManager.listActiveSessions.exists(_.key == key))
  }

  test("a client that observed a previous session does not create a new session") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    assertSessionChangedAndNotCreated(key, UUID.randomUUID().toString)
    // Without an observed session id, the session is created as usual.
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    assert(SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key).isDefined)
  }

  test("a stale client of an expired reconnect-allowed session does not create a session") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val oldHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    oldHolder.session.conf.set(Connect.CONNECT_SESSION_ALLOW_RECONNECT_AFTER_CLOSE.key, "true")
    oldHolder.session.conf.set("spark.sql.shuffle.partitions", "7")
    SparkConnectService.sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)

    assertSessionChangedAndNotCreated(key, oldHolder.session.sessionUUID)

    // A new SparkSession with the same session id gets a new, empty session.
    val newHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    assert(newHolder ne oldHolder)
    assert(newHolder.session.sessionUUID != oldHolder.session.sessionUUID)
    assert(newHolder.session.conf.get("spark.sql.shuffle.partitions") != "7")
  }

  test("an artifact status request from a stale client does not create a session") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val oldHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    oldHolder.session.conf.set(Connect.CONNECT_SESSION_ALLOW_RECONNECT_AFTER_CLOSE.key, "true")
    SparkConnectService.sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)

    val observer = new StreamObserver[proto.ArtifactStatusesResponse] {
      override def onNext(v: proto.ArtifactStatusesResponse): Unit = {}
      override def onError(t: Throwable): Unit = throw t
      override def onCompleted(): Unit = {}
    }
    val request = proto.ArtifactStatusesRequest
      .newBuilder()
      .setUserContext(proto.UserContext.newBuilder().setUserId(key.userId))
      .setSessionId(key.sessionId)
      .setClientObservedServerSideSessionId(oldHolder.session.sessionUUID)
      .addNames("cache/abc")
      .build()
    val ex = intercept[SparkSQLException] {
      new SparkConnectArtifactStatusesHandler(observer).handle(request)
    }
    assert(ex.getCondition == "INVALID_HANDLE.SESSION_CHANGED")
    assert(SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key).isEmpty)
  }

  test("lookups by a stale client of an expired reconnect-allowed session get SESSION_CHANGED") {
    val sessionManager = SparkConnectService.sessionManager
    val key = SessionKey("user", UUID.randomUUID().toString)
    val oldHolder = sessionManager.getOrCreateIsolatedSession(key, None)
    val oldSessionId = oldHolder.session.sessionUUID
    // A live session with a matching observed session id is returned as usual.
    assert(sessionManager.getIsolatedSession(key, Some(oldSessionId)) eq oldHolder)
    oldHolder.session.conf.set(Connect.CONNECT_SESSION_ALLOW_RECONNECT_AFTER_CLOSE.key, "true")
    sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)

    val exGet = intercept[SparkSQLException] {
      sessionManager.getIsolatedSession(key, Some(oldSessionId))
    }
    assert(exGet.getCondition == "INVALID_HANDLE.SESSION_CHANGED")
    val exClone = intercept[SparkSQLException] {
      sessionManager.cloneSession(key, UUID.randomUUID().toString, Some(oldSessionId))
    }
    assert(exClone.getCondition == "INVALID_HANDLE.SESSION_CHANGED")
    // Without an observed session id, the session is simply not found.
    val exNotFound = intercept[SparkSQLException] {
      sessionManager.getIsolatedSession(key, None)
    }
    assert(exNotFound.getCondition == "INVALID_HANDLE.SESSION_NOT_FOUND")
    assert(sessionManager.getIsolatedSessionIfPresent(key).isEmpty)
  }

  test("a closed session id with an observed session id still gets SESSION_CLOSED") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val holder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    SparkConnectService.sessionManager.closeSession(key)
    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(
        key,
        Some(holder.session.sessionUUID))
    }
    assert(ex.getCondition == "INVALID_HANDLE.SESSION_CLOSED")
  }

  test(
    "getOrCreateIsolatedSession/getIsolatedSession/getIsolatedSessionIfPresent " +
      "doesn't recreate closed session") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    SparkConnectService.sessionManager.closeSession(key)

    val exGetOrCreate = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    }
    assert(exGetOrCreate.getCondition == "INVALID_HANDLE.SESSION_CLOSED")
    assert(exGetOrCreate.getSqlState == "08003")

    val exGet = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getIsolatedSession(key, None)
    }
    assert(exGet.getCondition == "INVALID_HANDLE.SESSION_CLOSED")
    assert(exGet.getSqlState == "08003")

    val sessionGetIfPresent = SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key)
    assert(sessionGetIfPresent.isEmpty)
  }

  test("getIsolatedSession/getIsolatedSessionIfPresent when session doesn't exist") {
    val key = SessionKey("user", UUID.randomUUID().toString)

    val exGet = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getIsolatedSession(key, None)
    }
    assert(exGet.getCondition == "INVALID_HANDLE.SESSION_NOT_FOUND")
    assert(exGet.getSqlState == "08003")

    val sessionGetIfPresent = SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key)
    assert(sessionGetIfPresent.isEmpty)
  }

  test("SessionHolder with custom expiration time is not cleaned up due to inactivity") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)

    assert(
      SparkConnectService.sessionManager.listActiveSessions.exists(
        _.sessionId == sessionHolder.sessionId))
    sessionHolder.setCustomInactiveTimeoutMs(Some(5.days.toMillis))

    // clean up with inactivity timeout of 0.
    SparkConnectService.sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)
    // session should still be there.
    assert(
      SparkConnectService.sessionManager.listActiveSessions.exists(
        _.sessionId == sessionHolder.sessionId))

    sessionHolder.setCustomInactiveTimeoutMs(None)
    // it will be cleaned up now.
    SparkConnectService.sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)
    assert(SparkConnectService.sessionManager.listActiveSessions.isEmpty)
    assert(
      SparkConnectService.sessionManager.listClosedSessions.exists(
        _.sessionId == sessionHolder.sessionId))
  }

  test("SessionHolder is recorded with status closed after close") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    val activeSessionInfo = SparkConnectService.sessionManager.listActiveSessions.find(
      _.sessionId == sessionHolder.sessionId)
    assert(activeSessionInfo.isDefined)
    assert(activeSessionInfo.get.status == SessionStatus.Started)
    assert(activeSessionInfo.get.closedTimeMs.isEmpty)

    SparkConnectService.sessionManager.closeSession(sessionHolder.key)

    assert(SparkConnectService.sessionManager.listActiveSessions.isEmpty)
    val closedSessionInfo = SparkConnectService.sessionManager.listClosedSessions.find(
      _.sessionId == sessionHolder.sessionId)
    assert(closedSessionInfo.isDefined)
    assert(closedSessionInfo.get.status == SessionStatus.Closed)
    assert(closedSessionInfo.get.closedTimeMs.isDefined)
  }

  private def assertSessionClosed(key: SessionKey): Unit = {
    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    }
    assert(ex.getCondition == "INVALID_HANDLE.SESSION_CLOSED")
  }

  private def releaseSession(key: SessionKey, allowReconnect: Boolean): Unit = {
    val observer = new StreamObserver[proto.ReleaseSessionResponse] {
      override def onNext(v: proto.ReleaseSessionResponse): Unit = {}
      override def onError(t: Throwable): Unit = throw t
      override def onCompleted(): Unit = {}
    }
    val request = proto.ReleaseSessionRequest
      .newBuilder()
      .setUserContext(proto.UserContext.newBuilder().setUserId(key.userId))
      .setSessionId(key.sessionId)
      .setAllowReconnect(allowReconnect)
      .build()
    new SparkConnectReleaseSessionHandler(observer).handle(request)
  }

  test("expired session is tombstoned by default") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)

    SparkConnectService.sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)

    assertSessionClosed(key)
  }

  test("expired session that allows reconnect after close can be recreated as a new session") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val oldHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    oldHolder.session.conf.set(Connect.CONNECT_SESSION_ALLOW_RECONNECT_AFTER_CLOSE.key, "true")
    oldHolder.session.conf.set("spark.sql.shuffle.partitions", "7")

    SparkConnectService.sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)
    assert(SparkConnectService.sessionManager.listActiveSessions.isEmpty)

    // The id is reusable, and the recreated session starts from a clean state.
    val newHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    assert(newHolder.session.conf.get("spark.sql.shuffle.partitions") != "7")
    // Opting in is per session: the new session is tombstoned again unless it opts in too.
    SparkConnectService.sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)
    assertSessionClosed(key)
  }

  /**
   * A session holder whose close() blocks until `release` is counted down. If `blockInfoOn` is
   * set, the first getSessionHolderInfo call on that thread blocks until `releaseInfo` is counted
   * down.
   */
  private class BlockingCloseSessionHolder(key: SessionKey)
      extends SessionHolder(key.userId, key.sessionId, spark.newSession()) {
    val closeStarted = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val closeCalls = new AtomicInteger(0)

    @volatile var blockInfoOn: Thread = null
    val infoBlocked = new CountDownLatch(1)
    val releaseInfo = new CountDownLatch(1)

    override private[connect] def close(): Unit = {
      closeCalls.incrementAndGet()
      closeStarted.countDown()
      assert(release.await(1, TimeUnit.MINUTES))
      super.close()
    }

    override def getSessionHolderInfo: SessionHolderInfo = {
      if (blockInfoOn eq Thread.currentThread()) {
        blockInfoOn = null
        infoBlocked.countDown()
        assert(releaseInfo.await(1, TimeUnit.MINUTES))
      }
      super.getSessionHolderInfo
    }
  }

  private def startBlockingCloseSession(
      key: SessionKey,
      allowReconnectAfterClose: Boolean = true): BlockingCloseSessionHolder = {
    val holder = new BlockingCloseSessionHolder(key)
    holder.initializeSession()
    holder.session.conf.set(
      Connect.CONNECT_SESSION_ALLOW_RECONNECT_AFTER_CLOSE.key,
      allowReconnectAfterClose.toString)
    SparkConnectService.sessionManager.putSessionForTesting(holder)
    holder
  }

  test("session id stays tombstoned until the owning close completes") {
    val sessionManager = SparkConnectService.sessionManager
    Seq(true, false).foreach { allowReconnectAfterClose =>
      val key = SessionKey("user", UUID.randomUUID().toString)
      val holder = startBlockingCloseSession(key, allowReconnectAfterClose)

      val owner = new Thread(() => sessionManager.closeSession(key))
      owner.start()
      try {
        assert(holder.closeStarted.await(1, TimeUnit.MINUTES))
        // The old session is removed but still closing: it must not be recreated in this window.
        assertSessionClosed(key)

        // Callers that lose the removal, such as a concurrent ReleaseSession or a later
        // maintenance run, must not close it again or touch its fence.
        assert(sessionManager.tryRemoveSessionHolder(holder).isEmpty)
        sessionManager.closeSession(key)
        sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)
        assertSessionClosed(key)
      } finally {
        holder.release.countDown()
        owner.join(TimeUnit.MINUTES.toMillis(1))
      }
      assert(!owner.isAlive)
      assert(holder.closeCalls.get == 1)

      if (allowReconnectAfterClose) {
        // The owning close() has finished, so the id can be reused.
        assert(sessionManager.getOrCreateIsolatedSession(key, None) ne holder)
      } else {
        // A default close keeps the tombstone, now with its closed time.
        assertSessionClosed(key)
        val closed = sessionManager.listClosedSessions.find(_.key == key)
        assert(closed.exists(_.closedTimeMs.isDefined))
      }
    }
  }

  test("maintenance that observed a session but lost its removal does not close it") {
    val sessionManager = SparkConnectService.sessionManager
    val key = SessionKey("user", UUID.randomUUID().toString)
    val holder = startBlockingCloseSession(key)

    val maintenance =
      new Thread(() => sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L))
    val owner = new Thread(() => sessionManager.closeSession(key))
    holder.blockInfoOn = maintenance
    maintenance.start()
    try {
      // Maintenance has observed the expired holder in sessionStore but not yet removed it.
      assert(holder.infoBlocked.await(1, TimeUnit.MINUTES))
      // A concurrent close wins the removal and blocks in close().
      owner.start()
      assert(holder.closeStarted.await(1, TimeUnit.MINUTES))
      holder.releaseInfo.countDown()
      maintenance.join(TimeUnit.MINUTES.toMillis(1))
      assert(!maintenance.isAlive)
      assert(holder.closeCalls.get == 1)
      // The owner's close() is still running, so the id stays reserved.
      assertSessionClosed(key)
    } finally {
      holder.releaseInfo.countDown()
      holder.release.countDown()
      maintenance.join(TimeUnit.MINUTES.toMillis(1))
      if (owner.getState != Thread.State.NEW) {
        owner.join(TimeUnit.MINUTES.toMillis(1))
      }
    }
    assert(!owner.isAlive)
    assert(holder.closeCalls.get == 1)

    sessionManager.getOrCreateIsolatedSession(key, None)
  }

  test("a reconnect-allowed close fences its id without taking a tombstone slot") {
    val sessionManager = SparkConnectService.sessionManager
    // Opted in through the session conf, or through ReleaseSession(allow_reconnect = true).
    Seq(true, false).foreach { optedIn =>
      val key = SessionKey("user", UUID.randomUUID().toString)
      val holder = startBlockingCloseSession(key, allowReconnectAfterClose = optedIn)

      val owner = new Thread(() => sessionManager.closeSession(key, allowReconnect = !optedIn))
      owner.start()
      try {
        assert(holder.closeStarted.await(1, TimeUnit.MINUTES))
        assert(!sessionManager.listClosedSessions.exists(_.key == key))
        assertSessionClosed(key)
        Seq(None, Some(holder.session.sessionUUID)).foreach { observedSessionId =>
          val exGet = intercept[SparkSQLException] {
            sessionManager.getIsolatedSession(key, observedSessionId)
          }
          assert(exGet.getCondition == "INVALID_HANDLE.SESSION_CLOSED")
        }
      } finally {
        holder.release.countDown()
        owner.join(TimeUnit.MINUTES.toMillis(1))
      }
      assert(!owner.isAlive)
      assert(!sessionManager.listClosedSessions.exists(_.key == key))

      // The id is reusable for a new, empty session.
      val newHolder = sessionManager.getOrCreateIsolatedSession(key, None)
      assert(newHolder ne holder)

      // A default close of the new session is tombstoned and listed as closed.
      sessionManager.closeSession(key)
      assert(sessionManager.listClosedSessions.exists(_.key == key))
      assertSessionClosed(key)
    }
  }

  private class FailingCloseSessionHolder(key: SessionKey)
      extends SessionHolder(key.userId, key.sessionId, spark.newSession()) {
    override private[connect] def close(): Unit = {
      super.close()
      throw new IllegalStateException("close failed")
    }
  }

  test("a close that throws still releases or tombstones the session id") {
    val sessionManager = SparkConnectService.sessionManager
    Seq(true, false).foreach { allowReconnectAfterClose =>
      val key = SessionKey("user", UUID.randomUUID().toString)
      val holder = new FailingCloseSessionHolder(key)
      holder.initializeSession()
      holder.session.conf.set(
        Connect.CONNECT_SESSION_ALLOW_RECONNECT_AFTER_CLOSE.key,
        allowReconnectAfterClose.toString)
      sessionManager.putSessionForTesting(holder)

      intercept[IllegalStateException] {
        sessionManager.closeSession(key)
      }
      if (allowReconnectAfterClose) {
        // The id is released for a new session.
        assert(sessionManager.getOrCreateIsolatedSession(key, None) ne holder)
      } else {
        assertSessionClosed(key)
        val closed = sessionManager.listClosedSessions.find(_.key == key)
        assert(closed.exists(_.closedTimeMs.isDefined))
      }
    }
  }

  test("cloning into a session id that is still closing fails until the close completes") {
    val sessionManager = SparkConnectService.sessionManager
    val sourceKey = SessionKey("user", UUID.randomUUID().toString)
    sessionManager.getOrCreateIsolatedSession(sourceKey, None)
    val targetKey = SessionKey("user", UUID.randomUUID().toString)
    val holder = startBlockingCloseSession(targetKey)

    val owner = new Thread(() => sessionManager.closeSession(targetKey))
    owner.start()
    try {
      assert(holder.closeStarted.await(1, TimeUnit.MINUTES))
      val ex = intercept[SparkSQLException] {
        sessionManager.cloneSession(sourceKey, targetKey.sessionId, None)
      }
      assert(ex.getCondition == "INVALID_CLONE_SESSION_REQUEST.TARGET_SESSION_ID_ALREADY_CLOSED")
    } finally {
      holder.release.countDown()
      owner.join(TimeUnit.MINUTES.toMillis(1))
    }
    assert(!owner.isAlive)

    // The reconnect-allowed close has finished, so the id can be cloned into.
    val cloned = sessionManager.cloneSession(sourceKey, targetKey.sessionId, None)
    assert(cloned.key == targetKey)
  }

  test("a late duplicate shutdown does not re-tombstone or remove a recreated session") {
    val sessionManager = SparkConnectService.sessionManager
    val key = SessionKey("user", UUID.randomUUID().toString)
    val oldHolder = sessionManager.getOrCreateIsolatedSession(key, None)
    releaseSession(key, allowReconnect = true)

    // For example, a maintenance run that observed the old holder before the release removed it.
    assert(sessionManager.tryRemoveSessionHolder(oldHolder).isEmpty)
    val newHolder = sessionManager.getOrCreateIsolatedSession(key, None)
    assert(newHolder ne oldHolder)

    // Removal keyed on the old holder must not remove the new session with the same key.
    assert(sessionManager.tryRemoveSessionHolder(oldHolder).isEmpty)
    assert(sessionManager.getIsolatedSessionIfPresent(key).exists(_ eq newHolder))
  }

  test("ReleaseSession honors allow_reconnect and the session opt-in") {
    val released = SessionKey("user", UUID.randomUUID().toString)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(released, None)
    releaseSession(released, allowReconnect = false)
    assertSessionClosed(released)

    val reconnect = SessionKey("user", UUID.randomUUID().toString)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(reconnect, None)
    releaseSession(reconnect, allowReconnect = true)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(reconnect, None)

    val optedIn = SessionKey("user", UUID.randomUUID().toString)
    val optedInHolder =
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(optedIn, None)
    optedInHolder.session.conf
      .set(Connect.CONNECT_SESSION_ALLOW_RECONNECT_AFTER_CLOSE.key, "true")
    releaseSession(optedIn, allowReconnect = false)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(optedIn, None)
  }

  test("Pipeline execution cache is cleared when the session holder is closed") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    val graphId = "test_graph"
    val pipelineUpdateContext = new PipelineUpdateContextImpl(
      new DataflowGraph(Seq(), Seq(), Seq(), Seq()),
      (_: PipelineEvent) => None,
      storageRoot = "file:///test_storage_root")
    sessionHolder.cachePipelineExecution(graphId, pipelineUpdateContext)
    assert(
      sessionHolder.getPipelineExecution(graphId).nonEmpty,
      "pipeline execution was not cached")
    SparkConnectService.sessionManager.closeSession(sessionHolder.key)
    assert(
      sessionHolder.getPipelineExecution(graphId).isEmpty,
      "pipeline execution was not removed")
  }

  test("baseSession allows creating sessions after default session is cleared") {
    // Create a new session manager to test initialization
    val sessionManager = new SparkConnectSessionManager()

    // Initialize the base session with the test SparkContext
    sessionManager.initializeBaseSession(() => spark.newSession())

    // Clear the default and active sessions to simulate the scenario where
    // SparkSession.active or SparkSession.getDefaultSession would fail
    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession()

    // Create an isolated session - this should still work because we have baseSession
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = sessionManager.getOrCreateIsolatedSession(key, None)

    // Verify the session was created successfully
    assert(sessionHolder != null)
    assert(sessionHolder.session != null)

    // Clean up
    sessionManager.closeSession(key)
  }

  test("initializeBaseSession is idempotent") {
    // Create a new session manager to test initialization
    val sessionManager = new SparkConnectSessionManager()

    // Initialize the base session multiple times
    sessionManager.initializeBaseSession(() => spark.newSession())
    val key1 = SessionKey("user1", UUID.randomUUID().toString)
    val sessionHolder1 = sessionManager.getOrCreateIsolatedSession(key1, None)
    val baseSessionUUID1 = sessionHolder1.session.sessionUUID

    // Initialize again - should not change the base session
    sessionManager.initializeBaseSession(() => spark.newSession())
    val key2 = SessionKey("user2", UUID.randomUUID().toString)
    val sessionHolder2 = sessionManager.getOrCreateIsolatedSession(key2, None)

    // Both sessions should be isolated from each other
    assert(sessionHolder1.session.sessionUUID != sessionHolder2.session.sessionUUID)

    // Clean up
    sessionManager.closeSession(key1)
    sessionManager.closeSession(key2)
  }
}
