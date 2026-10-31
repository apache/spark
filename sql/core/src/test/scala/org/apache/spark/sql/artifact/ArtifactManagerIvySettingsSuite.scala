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
package org.apache.spark.sql.artifact

import java.net.URI
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{JAR_IVY_CONNECT_TIMEOUT, JAR_IVY_READ_TIMEOUT}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{IvyTestUtils, MavenUtils, Utils}
import org.apache.spark.util.MavenUtils.MavenCoordinate

class ArtifactManagerIvySettingsSuite extends SharedSparkSession {

  private val ivySettingsDir = Utils.createTempDir(namePrefix = "artifact-ivy-settings")
  private val coordinate = MavenCoordinate("my.runtime.lib", "mylib", "0.1")
  private val repository = IvyTestUtils.createLocalRepositoryForTests(
    coordinate,
    dependencies = None,
    rootDir = Some(ivySettingsDir.toPath.resolve("repository").toFile))
  private val ivySettings = ivySettingsDir.toPath.resolve("ivysettings.xml")
  Files.writeString(
    ivySettings,
    s"""<ivysettings>
       |  <settings defaultResolver="runtime"/>
       |  <resolvers>
       |    <ibiblio name="runtime" m2compatible="true" root="${repository.toURI}"/>
       |  </resolvers>
       |</ivysettings>""".stripMargin)

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.test.artifact.ivySettingsDir", ivySettingsDir.toString)
      .set(
        MavenUtils.JAR_IVY_SETTING_PATH_KEY,
        "${spark.test.artifact.ivySettingsDir}/ivysettings.xml")
      .set(JAR_IVY_CONNECT_TIMEOUT, 7000L)
      .set(JAR_IVY_READ_TIMEOUT, 11000L)
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      Utils.deleteRecursively(ivySettingsDir)
    }
  }

  test("Ivy timeout configurations have defaults") {
    val conf = new SparkConf(loadDefaults = false)
    assert(conf.get(JAR_IVY_CONNECT_TIMEOUT) == 30000L)
    assert(conf.get(JAR_IVY_READ_TIMEOUT) == 300000L)
  }

  test("SparkSession artifact APIs use spark.jars.ivySettings") {
    val ivyUri = URI.create(s"ivy://${coordinate.toString}")
    val otherSession = spark.newSession()

    spark.addArtifact(ivyUri)
    val jarName = s"${coordinate.groupId}_${coordinate.artifactId}-${coordinate.version}.jar"
    assert(Files.exists(spark.artifactManager.artifactPath.resolve("jars").resolve(jarName)))
    assert(
      !Files.exists(otherSession.artifactManager.artifactPath.resolve("jars").resolve(jarName)))

    otherSession.addArtifacts(ivyUri)

    assert(Files.exists(
      otherSession.artifactManager.artifactPath.resolve("jars").resolve(jarName)))
    assert(
      spark.artifactManager.artifactPath.resolve(".ivy-cache") !=
        otherSession.artifactManager.artifactPath.resolve(".ivy-cache"))
    assert(spark.artifactManager.ivyConnectTimeoutMs == 7000)
    assert(spark.artifactManager.ivyReadTimeoutMs == 11000)
  }

  test("SparkSession artifact APIs use session Ivy settings") {
    withTempDir { dir =>
      val sessionCoordinate = MavenCoordinate("my.session.lib", "mylib", "0.1")
      val sessionRepository = IvyTestUtils.createLocalRepositoryForTests(
        sessionCoordinate,
        dependencies = None,
        rootDir = Some(dir.toPath.resolve("repository").toFile))
      val sessionIvySettings = dir.toPath.resolve("session-ivysettings.xml")
      Files.writeString(
        sessionIvySettings,
        s"""<ivysettings>
           |  <settings defaultResolver="session"/>
           |  <resolvers>
           |    <ibiblio name="session" m2compatible="true" root="${sessionRepository.toURI}"/>
           |  </resolvers>
           |</ivysettings>""".stripMargin)
      val session = spark.newSession()
      try {
        session.sessionState.conf.setConfString(
          "spark.test.artifact.ivySettingsDir", dir.toString)
        session.sessionState.conf.setConfString(
          MavenUtils.JAR_IVY_SETTING_PATH_KEY,
          "${spark.test.artifact.ivySettingsDir}/session-ivysettings.xml")
        session.addArtifact(URI.create(s"ivy://$sessionCoordinate"))
        val jarName =
          s"${sessionCoordinate.groupId}_${sessionCoordinate.artifactId}-" +
            s"${sessionCoordinate.version}.jar"
        assert(Files.exists(session.artifactManager.artifactPath.resolve("jars").resolve(jarName)))
        assert(spark.conf.get("spark.test.artifact.ivySettingsDir") == ivySettingsDir.toString)
      } finally {
        session.artifactManager.cleanUpResourcesForTesting()
      }
    }
  }

  test("SPARK-60012: cloning skips the Ivy cache and creates a fresh cache on resolution") {
    val ivyUri = URI.create(s"ivy://${coordinate.toString}")
    val jarName = s"${coordinate.groupId}_${coordinate.artifactId}-${coordinate.version}.jar"
    val parentSession = spark.newSession()

    try {
      parentSession.addArtifact(ivyUri)
      val parentArtifactPath = parentSession.artifactManager.artifactPath
      val parentIvyCachePath = parentArtifactPath.resolve(".ivy-cache")
      val parentCacheMarker = parentIvyCachePath.resolve("parent-only")
      Files.writeString(parentCacheMarker, "parent cache")

      val clonedSession = parentSession.cloneSession()
      try {
        val clonedArtifactPath = clonedSession.artifactManager.artifactPath
        val clonedIvyCachePath = clonedArtifactPath.resolve(".ivy-cache")

        assert(Files.exists(clonedArtifactPath.resolve("jars").resolve(jarName)))
        assert(!Files.exists(clonedIvyCachePath))

        clonedSession.addArtifact(ivyUri)

        assert(Files.isDirectory(clonedIvyCachePath))
        assert(!Files.exists(clonedIvyCachePath.resolve(parentCacheMarker.getFileName)))
      } finally {
        clonedSession.artifactManager.cleanUpResourcesForTesting()
      }
    } finally {
      parentSession.artifactManager.cleanUpResourcesForTesting()
    }
  }
}
