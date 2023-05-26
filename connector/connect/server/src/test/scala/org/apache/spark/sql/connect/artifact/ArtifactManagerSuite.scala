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
package org.apache.spark.sql.connect.artifact

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.connect.ResourceHelper
import org.apache.spark.sql.connect.service.{SessionHolder, SparkConnectService}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.CacheId
import org.apache.spark.util.Utils

class ArtifactManagerSuite extends SharedSparkSession with ResourceHelper {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
  }

  private val artifactPath = commonResourcePath.resolve("artifact-tests")
  private lazy val artifactManager = SparkConnectArtifactManager.getOrCreateArtifactManager

  private def sessionHolder(): SessionHolder = {
    SessionHolder("test", spark.sessionUUID, spark)
  }

  test("Jar artifacts are added to spark session") {
    val copyDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("smallJar.jar")
    val remotePath = Paths.get("jars/smallJar.jar")
    artifactManager.addArtifact(sessionHolder, remotePath, stagingPath, None)

    val jarList = spark.sparkContext.listJars()
    assert(jarList.exists(_.contains(remotePath.toString)))
  }

  test("Class artifacts are added to the correct directory.") {
    val copyDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("smallClassFile.class")
    val remotePath = Paths.get("classes/smallClassFile.class")
    assert(stagingPath.toFile.exists())
    artifactManager.addArtifact(sessionHolder, remotePath, stagingPath, None)

    val classFileDirectory = artifactManager.classArtifactDir
    val movedClassFile = classFileDirectory.resolve("smallClassFile.class").toFile
    assert(movedClassFile.exists())
  }

  test("Class file artifacts are added to SC classloader") {
    val copyDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("Hello.class")
    val remotePath = Paths.get("classes/Hello.class")
    assert(stagingPath.toFile.exists())
    artifactManager.addArtifact(sessionHolder, remotePath, stagingPath, None)

    val classFileDirectory = artifactManager.classArtifactDir
    val movedClassFile = classFileDirectory.resolve("Hello.class").toFile
    assert(movedClassFile.exists())

    val classLoader = SparkConnectArtifactManager.classLoaderWithArtifacts

    val instance = classLoader
      .loadClass("Hello")
      .getDeclaredConstructor(classOf[String])
      .newInstance("Talon")

    val msg = instance.getClass.getMethod("msg").invoke(instance)
    assert(msg == "Hello Talon! Nice to meet you!")
  }

  test("UDF can reference added class file") {
    val copyDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("Hello.class")
    val remotePath = Paths.get("classes/Hello.class")
    assert(stagingPath.toFile.exists())
    artifactManager.addArtifact(sessionHolder, remotePath, stagingPath, None)

    val classFileDirectory = artifactManager.classArtifactDir
    val movedClassFile = classFileDirectory.resolve("Hello.class").toFile
    assert(movedClassFile.exists())

    val classLoader = SparkConnectArtifactManager.classLoaderWithArtifacts

    val instance = classLoader
      .loadClass("Hello")
      .getDeclaredConstructor(classOf[String])
      .newInstance("Talon")
      .asInstanceOf[String => String]
    val udf = org.apache.spark.sql.functions.udf(instance)
    val session = SparkConnectService.getOrCreateIsolatedSession("c1", "session").session
    session.range(10).select(udf(col("id").cast("string"))).collect()
  }

  test("add a cache artifact to the Block Manager") {
    withTempPath { path =>
      val stagingPath = path.toPath
      Files.write(path.toPath, "test".getBytes(StandardCharsets.UTF_8))
      val remotePath = Paths.get("cache/abc")
      val session = sessionHolder()
      val blockManager = spark.sparkContext.env.blockManager
      val blockId = CacheId(session.userId, session.sessionId, "abc")
      try {
        artifactManager.addArtifact(session, remotePath, stagingPath, None)
        val bytes = blockManager.getLocalBytes(blockId)
        assert(bytes.isDefined)
        val readback = new String(bytes.get.toByteBuffer().array(), StandardCharsets.UTF_8)
        assert(readback === "test")
      } finally {
        blockManager.releaseLock(blockId)
        blockManager.removeCache(session.userId, session.sessionId)
      }
    }
  }

  test("Check Python includes when zipped package is added") {
    withTempPath { path =>
      val stagingPath = path.toPath
      Files.write(path.toPath, "test".getBytes(StandardCharsets.UTF_8))
      val session = sessionHolder()
      val remotePath = Paths.get("pyfiles/abc.zip")
      artifactManager.addArtifact(session, remotePath, stagingPath, None)
      assert(artifactManager.getSparkConnectPythonIncludes == Seq("abc.zip"))
    }
  }
}
