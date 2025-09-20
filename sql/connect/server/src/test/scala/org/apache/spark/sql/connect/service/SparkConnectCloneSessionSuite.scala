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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkSQLException
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectCloneSessionSuite extends SharedSparkSession with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
  }

  test("clone session with invalid target session ID format") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val invalidSessionId = "not-a-valid-uuid"

    // Create the source session first
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(sourceKey, None)

    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.cloneSession(sourceKey, invalidSessionId, None)
    }

    assert(ex.getCondition == "INVALID_CLONE_SESSION_REQUEST.TARGET_SESSION_ID_FORMAT")
    assert(ex.getMessage.contains("Target session ID not-a-valid-uuid"))
    assert(ex.getMessage.contains("must be an UUID string"))
  }

  test("clone session with target session ID already closed") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString
    val targetKey = SessionKey("testUser", targetSessionId)

    // Create and then close a session to put it in the closed cache
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(sourceKey, None)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(targetKey, None)
    SparkConnectService.sessionManager.closeSession(targetKey)

    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.cloneSession(sourceKey, targetSessionId, None)
    }

    assert(
      ex.getCondition ==
        "INVALID_CLONE_SESSION_REQUEST.TARGET_SESSION_ID_ALREADY_CLOSED")
    assert(ex.getMessage.contains(s"target session ID $targetSessionId"))
    assert(ex.getMessage.contains("was previously closed"))
  }

  test("clone session with target session ID already exists") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString
    val targetKey = SessionKey("testUser", targetSessionId)

    // Create both source and target sessions
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(sourceKey, None)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(targetKey, None)

    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.cloneSession(sourceKey, targetSessionId, None)
    }

    assert(
      ex.getCondition ==
        "INVALID_CLONE_SESSION_REQUEST.TARGET_SESSION_ID_ALREADY_EXISTS")
    assert(ex.getMessage.contains(s"target session ID $targetSessionId"))
    assert(ex.getMessage.contains("already exists"))
  }

  test("clone session with source session not found") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString

    // Don't create the source session, so it doesn't exist
    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.cloneSession(sourceKey, targetSessionId, None)
    }

    // Source session errors should remain as standard INVALID_HANDLE errors
    assert(ex.getCondition == "INVALID_HANDLE.SESSION_NOT_FOUND")
    assert(ex.getMessage.contains("Session not found"))
  }

  test("successful clone session creates new session") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString

    // Create source session
    val sourceSession = SparkConnectService.sessionManager
      .getOrCreateIsolatedSession(sourceKey, None)

    // Clone the session
    val clonedSession = SparkConnectService.sessionManager
      .cloneSession(sourceKey, targetSessionId, None)

    // Verify the cloned session has the expected session ID
    assert(clonedSession.sessionId == targetSessionId)
    assert(clonedSession.sessionId != sourceSession.sessionId)

    // Both sessions should be different objects
    assert(clonedSession != sourceSession)
  }

  test("cloned session copies all state (configs, temp views, UDFs, current database)") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString

    // Create source session and set up state
    val sourceSession = SparkConnectService.sessionManager
      .getOrCreateIsolatedSession(sourceKey, None)

    // Set SQL configs
    sourceSession.session.conf.set("spark.sql.custom.test.config", "test-value")
    sourceSession.session.conf.set("spark.sql.shuffle.partitions", "42")

    // Create temp views
    sourceSession.session.sql("CREATE TEMPORARY VIEW temp_view1 AS SELECT 1 as col1, 2 as col2")
    sourceSession.session.sql("CREATE TEMPORARY VIEW temp_view2 AS SELECT 3 as col3")

    // Register UDFs
    sourceSession.session.udf.register("double_value", (x: Int) => x * 2)
    sourceSession.session.udf.register("concat_strings", (a: String, b: String) => a + b)

    // Change database
    sourceSession.session.sql("CREATE DATABASE IF NOT EXISTS test_db")
    sourceSession.session.sql("USE test_db")

    // Clone the session
    val clonedSession = SparkConnectService.sessionManager
      .cloneSession(sourceKey, targetSessionId, None)

    // Verify all state is copied

    // Configs are copied
    assert(clonedSession.session.conf.get("spark.sql.custom.test.config") == "test-value")
    assert(clonedSession.session.conf.get("spark.sql.shuffle.partitions") == "42")

    // Temp views are accessible
    val view1Result = clonedSession.session.sql("SELECT * FROM temp_view1").collect()
    assert(view1Result.length == 1)
    assert(view1Result(0).getInt(0) == 1)
    assert(view1Result(0).getInt(1) == 2)

    val view2Result = clonedSession.session.sql("SELECT * FROM temp_view2").collect()
    assert(view2Result.length == 1)
    assert(view2Result(0).getInt(0) == 3)

    // UDFs are accessible
    val udfResult1 = clonedSession.session.sql("SELECT double_value(5)").collect()
    assert(udfResult1(0).getInt(0) == 10)

    val udfResult2 =
      clonedSession.session.sql("SELECT concat_strings('hello', 'world')").collect()
    assert(udfResult2(0).getString(0) == "helloworld")

    // Current database is copied
    assert(clonedSession.session.catalog.currentDatabase == "test_db")
  }

  test("sessions are independent after cloning (configs, temp views, UDFs)") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString

    // Create and set up source session
    val sourceSession = SparkConnectService.sessionManager
      .getOrCreateIsolatedSession(sourceKey, None)
    sourceSession.session.conf.set("spark.sql.custom.config", "initial")
    sourceSession.session.sql("CREATE TEMPORARY VIEW shared_view AS SELECT 1 as value")
    sourceSession.session.udf.register("shared_udf", (x: Int) => x + 1)

    // Clone the session
    val clonedSession = SparkConnectService.sessionManager
      .cloneSession(sourceKey, targetSessionId, None)

    // Test independence of configs
    sourceSession.session.conf.set("spark.sql.custom.config", "modified-source")
    clonedSession.session.conf.set("spark.sql.custom.config", "modified-clone")
    assert(sourceSession.session.conf.get("spark.sql.custom.config") == "modified-source")
    assert(clonedSession.session.conf.get("spark.sql.custom.config") == "modified-clone")

    // Test independence of temp views - modify shared view differently
    sourceSession.session.sql(
      "CREATE OR REPLACE TEMPORARY VIEW shared_view AS SELECT 10 as value")
    clonedSession.session.sql(
      "CREATE OR REPLACE TEMPORARY VIEW shared_view AS SELECT 20 as value")

    // Each session should see its own version of the view
    val sourceViewResult = sourceSession.session.sql("SELECT * FROM shared_view").collect()
    assert(sourceViewResult(0).getInt(0) == 10)

    val cloneViewResult = clonedSession.session.sql("SELECT * FROM shared_view").collect()
    assert(cloneViewResult(0).getInt(0) == 20)

    // Test independence of UDFs
    sourceSession.session.udf.register("shared_udf", (x: Int) => x + 10)
    clonedSession.session.udf.register("shared_udf", (x: Int) => x + 100)
    assert(sourceSession.session.sql("SELECT shared_udf(5)").collect()(0).getInt(0) == 15)
    assert(clonedSession.session.sql("SELECT shared_udf(5)").collect()(0).getInt(0) == 105)
  }

  test("cloned session copies artifacts and maintains independence") {
    import java.nio.charset.StandardCharsets
    import java.nio.file.{Files, Paths}

    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString

    // Create source session
    val sourceSession = SparkConnectService.sessionManager
      .getOrCreateIsolatedSession(sourceKey, None)

    // Add some test artifacts to source session
    val tempFile = Files.createTempFile("test-artifact", ".txt")
    Files.write(tempFile, "test content".getBytes(StandardCharsets.UTF_8))

    // Add artifact to source session
    val remotePath = Paths.get("test/artifact.txt")
    sourceSession.artifactManager.addArtifact(remotePath, tempFile, None)

    // Clone the session
    val clonedSession = SparkConnectService.sessionManager
      .cloneSession(sourceKey, targetSessionId, None)

    // Verify sessions have different artifact managers
    assert(sourceSession.artifactManager ne clonedSession.artifactManager)
    assert(sourceSession.session.sessionUUID != clonedSession.session.sessionUUID)

    // Test independence: add new artifacts to each session
    val sourceOnlyFile = Files.createTempFile("source-only", ".txt")
    Files.write(sourceOnlyFile, "source only content".getBytes(StandardCharsets.UTF_8))
    val sourceOnlyPath = Paths.get("jars/source.jar")
    sourceSession.artifactManager.addArtifact(sourceOnlyPath, sourceOnlyFile, None)

    val clonedOnlyFile = Files.createTempFile("cloned-only", ".txt")
    Files.write(clonedOnlyFile, "cloned only content".getBytes(StandardCharsets.UTF_8))
    val clonedOnlyPath = Paths.get("jars/cloned.jar")
    clonedSession.artifactManager.addArtifact(clonedOnlyPath, clonedOnlyFile, None)

    // Use getAddedJars to verify independence (since it's a public API)
    val sourceJars = sourceSession.artifactManager.getAddedJars.map(_.toString)
    val clonedJars = clonedSession.artifactManager.getAddedJars.map(_.toString)

    // Source should have source.jar but not cloned.jar
    assert(sourceJars.exists(_.contains("source.jar")))
    assert(!sourceJars.exists(_.contains("cloned.jar")))

    // Cloned should have cloned.jar but not source.jar
    assert(clonedJars.exists(_.contains("cloned.jar")))
    assert(!clonedJars.exists(_.contains("source.jar")))

    // Clean up
    Files.deleteIfExists(tempFile)
    Files.deleteIfExists(sourceOnlyFile)
    Files.deleteIfExists(clonedOnlyFile)
    sourceSession.artifactManager.close()
    clonedSession.artifactManager.close()
  }
}
