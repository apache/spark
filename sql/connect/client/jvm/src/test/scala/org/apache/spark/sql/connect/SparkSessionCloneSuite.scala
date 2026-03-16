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

package org.apache.spark.sql.connect

import java.util.UUID

import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

class SparkSessionCloneSuite extends ConnectFunSuite with RemoteSparkSession {

  test("cloneSession() creates independent session with auto-generated ID") {
    spark.sql("SET spark.test.original = 'value1'")

    val clonedSession = spark.cloneSession()

    // Sessions should have different IDs
    assert(spark.sessionId !== clonedSession.sessionId)

    // Modify original session setting
    spark.sql("SET spark.test.original = 'modified_original'")

    // Verify original session has the modified value
    val originalFinal = spark.sql("SET spark.test.original").collect().head.getString(1)
    assert(originalFinal === "'modified_original'")

    // Verify cloned session retains the original value
    val clonedValue = clonedSession.sql("SET spark.test.original").collect().head.getString(1)
    assert(clonedValue === "'value1'")
  }

  test("cloneSession(sessionId) creates session with specified ID") {
    val customSessionId = UUID.randomUUID().toString

    val clonedSession = spark.cloneSession(customSessionId)

    // Verify the cloned session has the specified ID
    assert(clonedSession.sessionId === customSessionId)
    assert(spark.sessionId !== clonedSession.sessionId)
  }

  test("invalid session ID format throws exception") {
    val ex = intercept[org.apache.spark.SparkException] {
      spark.cloneSession("not-a-valid-uuid")
    }
    // Verify it contains our clone-specific error message
    assert(ex.getMessage.contains("INVALID_CLONE_SESSION_REQUEST.TARGET_SESSION_ID_FORMAT"))
    assert(ex.getMessage.contains("not-a-valid-uuid"))
  }
}
