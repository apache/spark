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

import java.io.File

import org.apache.spark.{JobArtifactSet, SparkFunSuite}

class SessionJobArtifactStateSuite extends SparkFunSuite {

  protected var rootSession: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    rootSession = SparkSession.builder()
      .master("local")
      .config("default-config", "default")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (rootSession != null) {
        rootSession.stop()
        rootSession = null
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    } finally {
      super.afterAll()
    }
  }

  test("use new job artifact state in new spark session") {
    val newSession = rootSession.newSession()
    assert(newSession ne rootSession)
    assert(newSession.sessionUUID ne rootSession.sessionUUID)
    assert(newSession.artifactManager.state ne rootSession.artifactManager.state)
  }

  test("clone job artifact state from parent while clone spark session") {
    withTempDir { dir =>
      val jarPath = File.createTempFile("testJar", ".jar", dir).getAbsolutePath
      JobArtifactSet.withActiveJobArtifactState(rootSession.artifactManager.state) {
        rootSession.sparkContext.addJar(jarPath)
      }
      val newSession = rootSession.cloneSession()
      assert(newSession ne rootSession)
      assert(newSession.sessionUUID ne rootSession.sessionUUID)
      JobArtifactSet.withActiveJobArtifactState(newSession.artifactManager.state) {
        val jobArtifactSet = JobArtifactSet.getActiveOrDefault(newSession.sparkContext)

        // Artifacts from the other state must be not visible to this state.
        assert(jobArtifactSet.state.contains(newSession.artifactManager.state))
        assert(jobArtifactSet.jars.nonEmpty)
      }
    }
  }
}
