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
import java.util.UUID

import org.apache.commons.io.FileUtils

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.sql.connect.ResourceHelper
import org.apache.spark.sql.connect.service.{SessionHolder, SparkConnectService}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.CacheId
import org.apache.spark.util.Utils

class ArtifactManagerSuite extends SharedSparkSession with ResourceHelper {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf
      .set("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
      .set("spark.connect.copyFromLocalToFs.allowDestLocal", "true")
  }

  private val artifactPath = commonResourcePath.resolve("artifact-tests")
  private def sessionHolder(): SessionHolder = {
    SessionHolder("test", spark.sessionUUID, spark)
  }
  private lazy val artifactManager = new SparkConnectArtifactManager(sessionHolder())

  private def sessionUUID: String = spark.sessionUUID

  override def afterEach(): Unit = {
    artifactManager.cleanUpResources()
    super.afterEach()
  }

  test("Class artifacts are added to the correct directory.") {
    val copyDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("smallClassFile.class")
    val remotePath = Paths.get("classes/smallClassFile.class")
    assert(stagingPath.toFile.exists())
    artifactManager.addArtifact(remotePath, stagingPath, None)

    val movedClassFile = SparkConnectArtifactManager.artifactRootPath
      .resolve(s"$sessionUUID/classes/smallClassFile.class")
      .toFile
    assert(movedClassFile.exists())
  }

  test("Class file artifacts are added to SC classloader") {
    val copyDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("Hello.class")
    val remotePath = Paths.get("classes/Hello.class")
    assert(stagingPath.toFile.exists())
    artifactManager.addArtifact(remotePath, stagingPath, None)

    val movedClassFile = SparkConnectArtifactManager.artifactRootPath
      .resolve(s"$sessionUUID/classes/Hello.class")
      .toFile
    assert(movedClassFile.exists())

    val classLoader = artifactManager.classloader

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

    val sessionHolder =
      SparkConnectService.getOrCreateIsolatedSession("c1", UUID.randomUUID.toString())
    sessionHolder.addArtifact(remotePath, stagingPath, None)

    val movedClassFile = SparkConnectArtifactManager.artifactRootPath
      .resolve(s"${sessionHolder.session.sessionUUID}/classes/Hello.class")
      .toFile
    assert(movedClassFile.exists())

    val classLoader = sessionHolder.classloader
    val instance = classLoader
      .loadClass("Hello")
      .getDeclaredConstructor(classOf[String])
      .newInstance("Talon")
      .asInstanceOf[String => String]
    val udf = org.apache.spark.sql.functions.udf(instance)

    sessionHolder.withSession { session =>
      session.range(10).select(udf(col("id").cast("string"))).collect()
    }
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
        artifactManager.addArtifact(remotePath, stagingPath, None)
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
      val remotePath = Paths.get("pyfiles/abc.zip")
      artifactManager.addArtifact(remotePath, stagingPath, None)
      assert(artifactManager.getSparkConnectPythonIncludes == Seq("abc.zip"))
    }
  }

  test("SPARK-43790: Forward artifact file to cloud storage path") {
    val copyDir = Utils.createTempDir().toPath
    val destFSDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("smallClassFile.class")
    val remotePath = Paths.get("forward_to_fs", destFSDir.toString, "smallClassFileCopied.class")
    assert(stagingPath.toFile.exists())
    artifactManager.uploadArtifactToFs(remotePath, stagingPath)
    artifactManager.addArtifact(remotePath, stagingPath, None)

    val copiedClassFile = Paths.get(destFSDir.toString, "smallClassFileCopied.class").toFile
    assert(copiedClassFile.exists())
  }

  test("Removal of resources") {
    withTempPath { path =>
      // Setup cache
      val stagingPath = path.toPath
      Files.write(path.toPath, "test".getBytes(StandardCharsets.UTF_8))
      val remotePath = Paths.get("cache/abc")
      val session = sessionHolder()
      val blockManager = spark.sparkContext.env.blockManager
      val blockId = CacheId(session.userId, session.sessionId, "abc")
      // Setup artifact dir
      val copyDir = Utils.createTempDir().toPath
      FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
      try {
        artifactManager.addArtifact(remotePath, stagingPath, None)
        val stagingPathFile = copyDir.resolve("smallClassFile.class")
        val remotePathFile = Paths.get("classes/smallClassFile.class")
        artifactManager.addArtifact(remotePathFile, stagingPathFile, None)

        // Verify resources exist
        val bytes = blockManager.getLocalBytes(blockId)
        assert(bytes.isDefined)
        blockManager.releaseLock(blockId)
        val expectedPath = SparkConnectArtifactManager.artifactRootPath
          .resolve(s"$sessionUUID/classes/smallClassFile.class")
        assert(expectedPath.toFile.exists())

        // Remove resources
        artifactManager.cleanUpResources()

        assert(!blockManager.getLocalBytes(blockId).isDefined)
        assert(!expectedPath.toFile.exists())
      } finally {
        try {
          blockManager.releaseLock(blockId)
        } catch {
          case _: SparkException =>
          case throwable: Throwable => throw throwable
        } finally {
          FileUtils.deleteDirectory(copyDir.toFile)
          blockManager.removeCache(session.userId, session.sessionId)
        }
      }
    }
  }

  test("Classloaders for spark sessions are isolated") {
    // use same sessionId - different users should still make it isolated.
    val sessionId = UUID.randomUUID.toString()
    val holder1 = SparkConnectService.getOrCreateIsolatedSession("c1", sessionId)
    val holder2 = SparkConnectService.getOrCreateIsolatedSession("c2", sessionId)
    val holder3 = SparkConnectService.getOrCreateIsolatedSession("c3", sessionId)

    def addHelloClass(holder: SessionHolder): Unit = {
      val copyDir = Utils.createTempDir().toPath
      FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
      val stagingPath = copyDir.resolve("Hello.class")
      val remotePath = Paths.get("classes/Hello.class")
      assert(stagingPath.toFile.exists())
      holder.addArtifact(remotePath, stagingPath, None)
    }

    // Add the "Hello" classfile for the first user
    addHelloClass(holder1)

    val classLoader1 = holder1.classloader
    val instance1 = classLoader1
      .loadClass("Hello")
      .getDeclaredConstructor(classOf[String])
      .newInstance("Talon")
      .asInstanceOf[String => String]
    val udf1 = org.apache.spark.sql.functions.udf(instance1)

    holder1.withSession { session =>
      val result = session.range(10).select(udf1(col("id").cast("string"))).collect()
      assert(result.forall(_.getString(0).contains("Talon")))
    }

    assertThrows[ClassNotFoundException] {
      val classLoader2 = holder2.classloader
      val instance2 = classLoader2
        .loadClass("Hello")
        .getDeclaredConstructor(classOf[String])
        .newInstance("Talon")
        .asInstanceOf[String => String]
    }

    // Add the "Hello" classfile for the third user
    addHelloClass(holder3)
    val instance3 = holder3.classloader
      .loadClass("Hello")
      .getDeclaredConstructor(classOf[String])
      .newInstance("Ahri")
      .asInstanceOf[String => String]
    val udf3 = org.apache.spark.sql.functions.udf(instance3)

    holder3.withSession { session =>
      val result = session.range(10).select(udf3(col("id").cast("string"))).collect()
      assert(result.forall(_.getString(0).contains("Ahri")))
    }
  }

  test("SPARK-44300: Cleaning up resources only deletes session-specific resources") {
    val copyDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("Hello.class")
    val remotePath = Paths.get("classes/Hello.class")

    val sessionHolder =
      SparkConnectService.getOrCreateIsolatedSession("c1", UUID.randomUUID.toString)
    sessionHolder.addArtifact(remotePath, stagingPath, None)

    val sessionDirectory =
      SparkConnectArtifactManager.getArtifactDirectoryAndUriForSession(sessionHolder)._1.toFile
    assert(sessionDirectory.exists())

    sessionHolder.artifactManager.cleanUpResources()
    assert(!sessionDirectory.exists())
    assert(SparkConnectArtifactManager.artifactRootPath.toFile.exists())
  }
}

class ArtifactUriSuite extends SparkFunSuite with LocalSparkContext {

  private def createSparkContext(): Unit = {
    resetSparkContext()
    sc = new SparkContext("local[4]", "test", new SparkConf())

  }
  override def beforeEach(): Unit = {
    super.beforeEach()
    createSparkContext()
  }

  test("Artifact URI is reset when SparkContext is restarted") {
    val oldUri = SparkConnectArtifactManager.artifactRootURI
    createSparkContext()
    val newUri = SparkConnectArtifactManager.artifactRootURI
    assert(newUri != oldUri)
  }
}
