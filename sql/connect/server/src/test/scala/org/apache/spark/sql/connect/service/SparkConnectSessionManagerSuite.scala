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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.UUID

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkSQLException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.CloseableCatalog
import org.apache.spark.sql.pipelines.graph.{DataflowGraph, PipelineExecution, PipelineUpdateContextImpl}
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

  test("Catalogs loaded by the session are closed when the session holder is closed") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    sessionHolder.session.conf
      .set("spark.sql.catalog.closeable", classOf[CloseableCatalog].getName)
    // Catalogs are loaded with the active session's conf.
    val catalog = sessionHolder.withSession {
      _.sessionState.catalogManager.catalog("closeable").asInstanceOf[CloseableCatalog]
    }
    assert(!catalog.closed)

    SparkConnectService.sessionManager.closeSession(sessionHolder.key)
    assert(catalog.closed)
  }

  test("Catalogs are closed before the session artifacts are released") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    val artifact = Files.createTempFile("test-artifact", ".jar")
    Files.write(artifact, "test content".getBytes(StandardCharsets.UTF_8))
    sessionHolder.artifactManager.addArtifact(Paths.get("jars/test.jar"), artifact, None)
    sessionHolder.session.conf
      .set("spark.sql.catalog.checking", classOf[ArtifactCheckingCatalog].getName)
    val catalog = sessionHolder.withSession {
      _.sessionState.catalogManager.catalog("checking").asInstanceOf[ArtifactCheckingCatalog]
    }

    @volatile var artifactPresentWhenClosing = false
    ArtifactCheckingCatalog.onClose = () => {
      artifactPresentWhenClosing = sessionHolder.artifactManager.getAddedJars.nonEmpty
    }
    try {
      SparkConnectService.sessionManager.closeSession(sessionHolder.key)
      assert(catalog.closed)
      assert(artifactPresentWhenClosing)
      assert(sessionHolder.artifactManager.getAddedJars.isEmpty)
    } finally {
      ArtifactCheckingCatalog.onClose = () => ()
    }
  }

  test("Catalogs and artifacts are released when an earlier teardown step fails") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    val artifact = Files.createTempFile("test-artifact", ".jar")
    Files.write(artifact, "test content".getBytes(StandardCharsets.UTF_8))
    sessionHolder.artifactManager.addArtifact(Paths.get("jars/test.jar"), artifact, None)
    sessionHolder.session.conf
      .set("spark.sql.catalog.closeable", classOf[CloseableCatalog].getName)
    val catalog = sessionHolder.withSession {
      _.sessionState.catalogManager.catalog("closeable").asInstanceOf[CloseableCatalog]
    }
    // A pipeline execution that fails to stop makes the session teardown fail.
    val failingPipeline = new PipelineUpdateContextImpl(
      new DataflowGraph(Seq(), Seq(), Seq(), Seq()),
      (_: PipelineEvent) => None,
      storageRoot = "file:///test_storage_root") {
      override val pipelineExecution: PipelineExecution = new PipelineExecution(this) {
        override def executionStarted: Boolean = true
        override def stopPipeline(): Unit = throw new IllegalStateException("failed to stop")
      }
    }
    sessionHolder.cachePipelineExecution("failing_graph", failingPipeline)

    val e = intercept[IllegalStateException] {
      SparkConnectService.sessionManager.closeSession(sessionHolder.key)
    }
    assert(e.getMessage == "failed to stop")
    assert(catalog.closed)
    assert(sessionHolder.artifactManager.getAddedJars.isEmpty)
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

class ArtifactCheckingCatalog extends CloseableCatalog {
  override def close(): Unit = {
    ArtifactCheckingCatalog.onClose()
    super.close()
  }
}

object ArtifactCheckingCatalog {
  @volatile var onClose: () => Unit = () => ()
}

