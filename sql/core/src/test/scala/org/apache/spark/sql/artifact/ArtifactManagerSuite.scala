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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.apache.spark.{SparkConf, SparkException, SparkRuntimeException}
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.Artifact
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.storage.CacheId
import org.apache.spark.util.Utils

class ArtifactManagerSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.artifact.copyFromLocalToFs.allowDestLocal", "true")
    conf.set(SQLConf.ARTIFACTS_SESSION_ISOLATION_ENABLED, true)
    conf.set(SQLConf.ARTIFACTS_SESSION_ISOLATION_ALWAYS_APPLY_CLASSLOADER, true)
  }

  private val artifactPath = new File("src/test/resources/artifact-tests").toPath

  private lazy val artifactManager = spark.artifactManager

  private def sessionUUID: String = spark.sessionUUID

  override def afterEach(): Unit = {
    artifactManager.cleanUpResourcesForTesting()
    super.afterEach()
  }

  test("Class artifacts are added to the correct directory.") {
    assume(artifactPath.resolve("smallClassFile.class").toFile.exists)

    val copyDir = Utils.createTempDir().toPath
    Utils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("smallClassFile.class")
    assert(stagingPath.toFile.exists())
    val remotePath = Paths.get("classes/smallClassFile.class")
    artifactManager.addArtifact(remotePath, stagingPath, None)

    val movedClassFile = ArtifactManager.artifactRootDirectory
      .resolve(s"$sessionUUID/classes/smallClassFile.class")
      .toFile
    assert(movedClassFile.exists())
  }

  test("Class file artifacts are added to SC classloader") {
    assume(artifactPath.resolve("Hello.class").toFile.exists)

    val copyDir = Utils.createTempDir().toPath
    Utils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("Hello.class")
    assert(stagingPath.toFile.exists())
    val remotePath = Paths.get("classes/Hello.class")
    artifactManager.addArtifact(remotePath, stagingPath, None)

    val movedClassFile = ArtifactManager.artifactRootDirectory
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
    assume(artifactPath.resolve("Hello.class").toFile.exists)

    val copyDir = Utils.createTempDir().toPath
    Utils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("Hello.class")
    assert(stagingPath.toFile.exists())
    val remotePath = Paths.get("classes/Hello.class")

    artifactManager.addArtifact(remotePath, stagingPath, None)

    val movedClassFile = ArtifactManager.artifactRootDirectory
      .resolve(s"${spark.sessionUUID}/classes/Hello.class")
      .toFile
    assert(movedClassFile.exists())

    val classLoader = spark.artifactManager.classloader
    val instance = classLoader
      .loadClass("Hello")
      .getDeclaredConstructor(classOf[String])
      .newInstance("Talon")
      .asInstanceOf[String => String]
    val udf = org.apache.spark.sql.functions.udf(instance)

    spark.artifactManager.withResources {
      spark.range(10).select(udf(col("id").cast("string"))).collect()
    }
  }

  test("add a cache artifact to the Block Manager") {
    withTempPath { path =>
      val stagingPath = path.toPath
      Files.write(path.toPath, "test".getBytes(StandardCharsets.UTF_8))
      val remotePath = Paths.get("cache/abc")
      val blockManager = spark.sparkContext.env.blockManager
      val blockId = CacheId(spark.sessionUUID, "abc")
      try {
        artifactManager.addArtifact(remotePath, stagingPath, None)
        val bytes = blockManager.getLocalBytes(blockId)
        assert(bytes.isDefined)
        val readback = new String(bytes.get.toByteBuffer().array(), StandardCharsets.UTF_8)
        assert(readback === "test")
      } finally {
        blockManager.releaseLock(blockId)
        blockManager.removeCache(spark.sessionUUID)
      }
    }
  }

  test("Check Python includes when zipped package is added") {
    withTempPath { path =>
      val stagingPath = path.toPath
      Files.write(path.toPath, "test".getBytes(StandardCharsets.UTF_8))
      val remotePath = Paths.get("pyfiles/abc.zip")
      artifactManager.addArtifact(remotePath, stagingPath, None)
      assert(artifactManager.getPythonIncludes == Seq("abc.zip"))
    }
  }

  test("Add artifact idempotency") {
    val remotePath = Paths.get("pyfiles/abc.zip")

    withTempPath { path =>
      Files.write(path.toPath, "test".getBytes(StandardCharsets.UTF_8))
      artifactManager.addArtifact(remotePath, path.toPath, None)
    }

    withTempPath { path =>
      // subsequent call succeeds
      Files.write(path.toPath, "test".getBytes(StandardCharsets.UTF_8))
      artifactManager.addArtifact(remotePath, path.toPath, None)
    }

    withTempPath { path =>
      Files.write(path.toPath, "updated file".getBytes(StandardCharsets.UTF_8))
      assertThrows[RuntimeException] {
        artifactManager.addArtifact(remotePath, path.toPath, None)
      }
    }
  }

  test("SPARK-43790: Forward artifact file to cloud storage path") {
    assume(artifactPath.resolve("smallClassFile.class").toFile.exists)

    val copyDir = Utils.createTempDir().toPath
    val destFSDir = Utils.createTempDir().toPath
    Utils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("smallClassFile.class")
    val remotePath = Paths.get("forward_to_fs", destFSDir.toString, "smallClassFileCopied.class")
    assert(stagingPath.toFile.exists())
    artifactManager.uploadArtifactToFs(remotePath, stagingPath)
    artifactManager.addArtifact(remotePath, stagingPath, None)

    val copiedClassFile = Paths.get(destFSDir.toString, "smallClassFileCopied.class").toFile
    assert(copiedClassFile.exists())
  }

  test("Removal of resources") {
    assume(artifactPath.resolve("smallClassFile.class").toFile.exists)

    withTempPath { path =>
      // Setup cache
      val stagingPath = path.toPath
      Files.write(path.toPath, "test".getBytes(StandardCharsets.UTF_8))
      val remotePath = Paths.get("cache/abc")
      val blockManager = spark.sparkContext.env.blockManager
      val blockId = CacheId(spark.sessionUUID, "abc")
      // Setup artifact dir
      val copyDir = Utils.createTempDir().toPath
      Utils.copyDirectory(artifactPath.toFile, copyDir.toFile)
      try {
        artifactManager.addArtifact(remotePath, stagingPath, None)
        val stagingPathFile = copyDir.resolve("smallClassFile.class")
        val remotePathFile = Paths.get("classes/smallClassFile.class")
        artifactManager.addArtifact(remotePathFile, stagingPathFile, None)

        // Verify resources exist
        val bytes = blockManager.getLocalBytes(blockId)
        assert(bytes.isDefined)
        blockManager.releaseLock(blockId)
        val expectedPath = ArtifactManager.artifactRootDirectory
          .resolve(s"$sessionUUID/classes/smallClassFile.class")
        assert(expectedPath.toFile.exists())

        // Remove resources
        artifactManager.cleanUpResourcesForTesting()

        assert(blockManager.getLocalBytes(blockId).isEmpty)
        assert(!expectedPath.toFile.exists())
      } finally {
        try {
          blockManager.releaseLock(blockId)
        } catch {
          case _: SparkException =>
          case throwable: Throwable => throw throwable
        } finally {
          Utils.deleteRecursively(copyDir.toFile)
          blockManager.removeCache(spark.sessionUUID)
        }
      }
    }
  }

  test("Classloaders for spark sessions are isolated") {
    assume(artifactPath.resolve("Hello.class").toFile.exists)

    val session1 = spark.newSession()
    val session2 = spark.newSession()
    val session3 = spark.newSession()

    def addHelloClass(session: SparkSession): Unit = {
      val copyDir = Utils.createTempDir().toPath
      Utils.copyDirectory(artifactPath.toFile, copyDir.toFile)
      val stagingPath = copyDir.resolve("Hello.class")
      val remotePath = Paths.get("classes/Hello.class")
      assert(stagingPath.toFile.exists())
      session.artifactManager.addArtifact(remotePath, stagingPath, None)
    }

    // Add the "Hello" classfile for the first user
    addHelloClass(session1)

    val classLoader1 = session1.artifactManager.classloader
    val instance1 = classLoader1
      .loadClass("Hello")
      .getDeclaredConstructor(classOf[String])
      .newInstance("Talon")
      .asInstanceOf[String => String]
    val udf1 = org.apache.spark.sql.functions.udf(instance1)

    session1.artifactManager.withResources {
      val result1 = session1.range(10).select(udf1(col("id").cast("string"))).collect()
      assert(result1.forall(_.getString(0).contains("Talon")))
    }

    assertThrows[ClassNotFoundException] {
      val classLoader2 = session2.artifactManager.classloader
      val instance2 = classLoader2
        .loadClass("Hello")
        .getDeclaredConstructor(classOf[String])
        .newInstance("Talon")
        .asInstanceOf[String => String]
    }

    // Add the "Hello" classfile for the third user
    addHelloClass(session3)

    val classLoader3 = session3.artifactManager.classloader
    val instance3 = classLoader3
      .loadClass("Hello")
      .getDeclaredConstructor(classOf[String])
      .newInstance("Ahri")
      .asInstanceOf[String => String]
    val udf3 = org.apache.spark.sql.functions.udf(instance3)

    session3.artifactManager.withResources {
      val result3 = session3.range(10).select(udf3(col("id").cast("string"))).collect()
      assert(result3.forall(_.getString(0).contains("Ahri")))
    }
  }

  test("SPARK-44300: Cleaning up resources only deletes session-specific resources") {
    assume(artifactPath.resolve("Hello.class").toFile.exists)

    val copyDir = Utils.createTempDir().toPath
    Utils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("Hello.class")
    val remotePath = Paths.get("classes/Hello.class")

    artifactManager.addArtifact(remotePath, stagingPath, None)

    val sessionDirectory = artifactManager.artifactPath.toFile
    assert(sessionDirectory.exists())

    artifactManager.cleanUpResourcesForTesting()
    assert(!sessionDirectory.exists())
    assert(ArtifactManager.artifactRootDirectory.toFile.exists())
  }

  test("Add artifact to local session - by path") {
    val (fileName, binaryName) = ("Hello.class", "Hello")
    testAddArtifactToLocalSession(fileName, binaryName) { classPath =>
      spark.addArtifact(classPath.toString)
      fileName
    }
  }

  test("Add artifact to local session - by URI") {
    val (fileName, binaryName) = ("Hello.class", "Hello")
    testAddArtifactToLocalSession(fileName, binaryName) { classPath =>
      spark.addArtifact(classPath.toUri)
      fileName
    }
  }

  test("Add artifact to local session - custom target path") {
    val (fileName, binaryName) = ("HelloWithPackage.class", "my.custom.pkg.HelloWithPackage")
    val filePath = "my/custom/pkg/HelloWithPackage.class"
    testAddArtifactToLocalSession(fileName, binaryName) { classPath =>
      spark.addArtifact(classPath.toString, filePath)
      filePath
    }
  }

  test("Add artifact to local session - in memory") {
    val (fileName, binaryName) = ("HelloWithPackage.class", "my.custom.pkg.HelloWithPackage")
    val filePath = "my/custom/pkg/HelloWithPackage.class"
    testAddArtifactToLocalSession(fileName, binaryName) { classPath =>
      val buffer = Files.readAllBytes(classPath)
      spark.addArtifact(buffer, filePath)
      filePath
    }
  }

  test("Add multiple artifacts to local session and check if all are added despite exception") {
    val copyDir = Utils.createTempDir().toPath
    Utils.copyDirectory(artifactPath.toFile, copyDir.toFile)

    val artifact1Path = "my/custom/pkg/artifact1.jar"
    val artifact2Path = "my/custom/pkg/artifact2.jar"
    val targetPath = Paths.get(artifact1Path)
    val targetPath2 = Paths.get(artifact2Path)

    val classPath1 = copyDir.resolve("Hello.class")
    val classPath2 = copyDir.resolve("udf_noA.jar")
    assume(artifactPath.resolve("Hello.class").toFile.exists)
    assume(artifactPath.resolve("smallClassFile.class").toFile.exists)

    val artifact1 = Artifact.newArtifactFromExtension(
      targetPath.getFileName.toString,
      targetPath,
      new Artifact.LocalFile(Paths.get(classPath1.toString)))

    val alreadyExistingArtifact = Artifact.newArtifactFromExtension(
      targetPath2.getFileName.toString,
      targetPath,
      new Artifact.LocalFile(Paths.get(classPath2.toString)))

    val artifact2 = Artifact.newArtifactFromExtension(
      targetPath2.getFileName.toString,
      targetPath2,
      new Artifact.LocalFile(Paths.get(classPath2.toString)))

    spark.artifactManager.addLocalArtifacts(Seq(artifact1))

    val ex = intercept[SparkRuntimeException] {
      spark.artifactManager.addLocalArtifacts(
        Seq(alreadyExistingArtifact, artifact2, alreadyExistingArtifact))
    }

    checkError(
      exception = ex,
      condition = "ARTIFACT_ALREADY_EXISTS",
      parameters = Map("normalizedRemoteRelativePath" -> s"jars/${targetPath.toString}"))

    assert(ex.getSuppressed.length == 1)
    assert(ex.getSuppressed.head.isInstanceOf[SparkRuntimeException])
    val suppressed = ex.getSuppressed.head.asInstanceOf[SparkRuntimeException]

    checkError(
      exception = suppressed,
      condition = "ARTIFACT_ALREADY_EXISTS",
      parameters = Map("normalizedRemoteRelativePath" -> s"jars/${targetPath.toString}"))

    // Artifact1 should have been added
    val expectedFile1 = ArtifactManager.artifactRootDirectory
      .resolve(s"$sessionUUID/jars/$artifact1Path")
      .toFile
    assert(expectedFile1.exists())

    // Artifact2 should have been added despite exception
    val expectedFile2 = ArtifactManager.artifactRootDirectory
      .resolve(s"$sessionUUID/jars/$artifact2Path")
      .toFile
    assert(expectedFile2.exists())

    // Cleanup
    artifactManager.cleanUpResourcesForTesting()
    val sessionDir = ArtifactManager.artifactRootDirectory.resolve(sessionUUID).toFile

    assert(!expectedFile1.exists())
    assert(!sessionDir.exists())
  }

  test("Added artifact can be loaded by the current SparkSession") {
    val path = artifactPath.resolve("IntSumUdf.class")
    assume(path.toFile.exists)
    val buffer = Files.readAllBytes(path)
    spark.addArtifact(buffer, "IntSumUdf.class")

    spark.udf.registerJava("intSum", "IntSumUdf", DataTypes.LongType)

    val r = spark.range(5)
      .withColumn("id2", col("id") + 1)
      .selectExpr("intSum(id, id2)")
      .collect()
    assert(r.map(_.getLong(0)).toSeq == Seq(1, 3, 5, 7, 9))
  }

  private def testAddArtifactToLocalSession(
      classFileToUse: String, binaryName: String)(addFunc: Path => String): Unit = {
    val copyDir = Utils.createTempDir().toPath
    assume(artifactPath.resolve(classFileToUse).toFile.exists)

    Utils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val classPath = copyDir.resolve(classFileToUse)
    assert(classPath.toFile.exists())

    val movedClassPath = addFunc(classPath)

    val movedClassFile = ArtifactManager.artifactRootDirectory
      .resolve(s"$sessionUUID/classes/$movedClassPath")
      .toFile
    assert(movedClassFile.exists())

    val classLoader = artifactManager.classloader

    val instance = classLoader
      .loadClass(binaryName)
      .getDeclaredConstructor(classOf[String])
      .newInstance("Talon")

    val msg = instance.getClass.getMethod("msg").invoke(instance)
    assert(msg == "Hello Talon! Nice to meet you!")
  }

  test("Support Windows style paths") {
    withTempPath { path =>
      val stagingPath = path.toPath
      Files.write(path.toPath, "test".getBytes(StandardCharsets.UTF_8))
      val remotePath = Paths.get("windows\\abc.txt")
      artifactManager.addArtifact(remotePath, stagingPath, None)
      val file = ArtifactManager.artifactRootDirectory
        .resolve(s"$sessionUUID/windows/abc.txt")
        .toFile
      assert(file.exists())
    }
  }

  test("Cloning artifact manager will clone all artifacts") {
    withTempPath { dir =>
      val path = dir.toPath
      // Setup artifact dir
      Utils.copyDirectory(artifactPath.toFile, dir)
      val randomFilePath = path.resolve("random_file")
      val testBytes = "test".getBytes(StandardCharsets.UTF_8)
      Files.write(randomFilePath, testBytes)

      // Register multiple kinds of artifacts
      val clsPath = path.resolve("Hello.class")
      assume(clsPath.toFile.exists)
      artifactManager.addArtifact( // Class
        Paths.get("classes/Hello.class"), clsPath, None)
      artifactManager.addArtifact( // Python
        Paths.get("pyfiles/abc.zip"), randomFilePath, None, deleteStagedFile = false)
      val jarPath = Paths.get("jars/udf_noA.jar")
      assume(jarPath.toFile.exists)
      artifactManager.addArtifact( // JAR
        jarPath, path.resolve("udf_noA.jar"), None)
      artifactManager.addArtifact( // Cached
        Paths.get("cache/test"), randomFilePath, None)
      assert(Utils.listPaths(artifactManager.artifactPath.toFile).size() === 3)

      // Clone the artifact manager
      val newSession = spark.cloneSession()
      val newArtifactManager = newSession.artifactManager
      assert(newArtifactManager !== artifactManager)
      assert(newArtifactManager.artifactPath !== artifactManager.artifactPath)

      // Load the cached artifact
      assert(spark.artifactManager.getCachedBlockId("test")
        == newArtifactManager.getCachedBlockId("test"))

      val allFiles = Utils.listFiles(newArtifactManager.artifactPath.toFile)
      assert(allFiles.size() === 3)
      allFiles.forEach { file =>
        assert(!file.getCanonicalPath.contains(spark.sessionUUID))
        assert(file.getCanonicalPath.contains(newSession.sessionUUID))
        val originalFile = Paths.get(file.getCanonicalPath.replace(
          newSession.sessionUUID, spark.sessionUUID))
        assert(Files.exists(originalFile))
        assert(Files.readAllBytes(originalFile) === Files.readAllBytes(file.toPath))
      }
      assert(artifactManager.getPythonIncludes === newArtifactManager.getPythonIncludes)
      assert(
        artifactManager.getAddedJars.map(_.toString.replace(spark.sessionUUID, "")) ===
          newArtifactManager.getAddedJars.map(_.toString.replace(newSession.sessionUUID, "")))

      // Try load class from the cloned artifact manager
      val instance = newArtifactManager
        .classloader
        .loadClass("Hello")
        .getDeclaredConstructor(classOf[String])
        .newInstance("Talon")

      val msg = instance.getClass.getMethod("msg").invoke(instance)
      assert(msg == "Hello Talon! Nice to meet you!")
    }
  }

  test("Share blocks between ArtifactManagers") {
    def isBlockRegistered(id: CacheId): Boolean = {
      sparkContext.env.blockManager.getStatus(id).isDefined
    }

    def addCachedArtifact(session: SparkSession, name: String, data: String): CacheId = {
      val bytes = new Artifact.InMemory(data.getBytes(StandardCharsets.UTF_8))
      session.artifactManager.addLocalArtifacts(Artifact.newCacheArtifact(name, bytes) :: Nil)
      val id = CacheId(session.sessionUUID, name)
      assert(isBlockRegistered(id))
      id
    }

    // Create fresh session so there is no interference with other tests.
    val session1 = spark.newSession()
    val b1 = addCachedArtifact(session1, "b1", "b_one")
    val b2 = addCachedArtifact(session1, "b2", "b_two")

    // Clone, check that existing blocks are the same, add another block, clean-up, make sure
    // shared blocks survive and new block is cleaned.
    val session2 = session1.cloneSession()
    val b3 = addCachedArtifact(session2, "b3", "b_three")
    session2.artifactManager.cleanUpResourcesForTesting()
    assert(isBlockRegistered(b1))
    assert(isBlockRegistered(b2))
    assert(!isBlockRegistered(b3))

    // Clone, check that existing blocks are the same, replace existing blocks, clone parent, check
    // that inherited blocks are removed now.
    val session3 = session1.cloneSession()
    session1.artifactManager.cleanUpResourcesForTesting()
    assert(isBlockRegistered(b1))
    assert(isBlockRegistered(b2))
    assert(session3.artifactManager.getCachedBlockId("b1").get == b1)
    assert(session3.artifactManager.getCachedBlockId("b2").get == b2)

    val b1a = addCachedArtifact(session3, "b1", "b_one_a")
    val b2a = addCachedArtifact(session3, "b2", "b_two_a")
    assert(!isBlockRegistered(b1))
    assert(!isBlockRegistered(b2))
    assert(session3.artifactManager.getCachedBlockId("b1").get == b1a)
    assert(session3.artifactManager.getCachedBlockId("b2").get == b2a)

    // Clean-up last AM. No block should be left.
    session3.artifactManager.cleanUpResourcesForTesting()
    assert(!isBlockRegistered(b1a))
    assert(!isBlockRegistered(b2a))
  }

  test("Codegen cache should be invalid when artifacts are added - class artifact") {
    withTempDir { dir =>
      runCodegenTest("class artifact") {
        val randomFilePath = dir.toPath.resolve("random.class")
        val testBytes = "test".getBytes(StandardCharsets.UTF_8)
        Files.write(randomFilePath, testBytes)
        spark.addArtifact(randomFilePath.toString)
      }
    }
  }

  test("Codegen cache should be invalid when artifacts are added - JAR artifact") {
    withTempDir { dir =>
      runCodegenTest("JAR artifact") {
        val randomFilePath = dir.toPath.resolve("random.jar")
        val testBytes = "test".getBytes(StandardCharsets.UTF_8)
        Files.write(randomFilePath, testBytes)
        spark.addArtifact(randomFilePath.toString)
      }
    }
  }

  private def getCodegenCount: Long = CodegenMetrics.METRIC_COMPILATION_TIME.getCount

  private def runCodegenTest(msg: String)(addOneArtifact: => Unit): Unit = {
    withSQLConf(SQLConf.ARTIFACTS_SESSION_ISOLATION_ALWAYS_APPLY_CLASSLOADER.key -> "true") {
      val s = spark
      import s.implicits._

      val count1 = getCodegenCount
      // trigger codegen for Dataset
      Seq(Seq("abc")).toDS().collect()
      val count2 = getCodegenCount
      // codegen happens
      assert(count2 > count1, s"$msg: codegen should happen at the first time")

      // add one artifact, codegen cache should be invalid after this
      addOneArtifact

      // trigger codegen for another Dataset of same type
      Seq(Seq("abc")).toDS().collect()
      // codegen cache should not work for Datasets of same type.
      val count3 = getCodegenCount
      assert(count3 > count2, s"$msg: codegen should happen again after adding artifact")

      // trigger again
      Seq(Seq("abc")).toDS().collect()
      // codegen should work now as classloader is not changed
      val count4 = getCodegenCount
      assert(count4 == count3,
        s"$msg: codegen should not happen again as classloader is not changed")
    }
  }
}
