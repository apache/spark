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
      .set(MavenUtils.JAR_IVY_SETTING_PATH_KEY, ivySettings.toString)
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
}
