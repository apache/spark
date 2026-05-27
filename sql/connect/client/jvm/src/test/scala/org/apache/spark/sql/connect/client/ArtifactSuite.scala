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
package org.apache.spark.sql.connect.client

import java.io.{FileOutputStream, InputStream}
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import java.util.jar.{JarEntry, JarOutputStream}
import java.util.zip.CRC32

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}

import org.apache.spark.connect.proto.AddArtifactsRequest
import org.apache.spark.network.util.JavaUtils.sha256Hex
import org.apache.spark.sql.Artifact
import org.apache.spark.sql.connect.client.SparkConnectClient.Configuration
import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.util.IvyTestUtils
import org.apache.spark.util.MavenUtils.MavenCoordinate

class ArtifactSuite extends ConnectFunSuite {

  private var client: SparkConnectClient = _
  private var service: DummySparkConnectService = _
  private var server: Server = _
  private var artifactManager: ArtifactManager = _
  private var channel: ManagedChannel = _
  private var bstub: CustomSparkConnectBlockingStub = _
  private var stub: CustomSparkConnectStub = _
  private var state: SparkConnectStubState = _

  private def startDummyServer(): Unit = {
    service = new DummySparkConnectService()
    server = InProcessServerBuilder
      .forName(getClass.getName)
      .addService(service)
      .build()
    server.start()
  }

  private def createArtifactManager(): Unit = {
    channel = InProcessChannelBuilder.forName(getClass.getName).directExecutor().build()
    state = new SparkConnectStubState(channel, RetryPolicy.defaultPolicies())
    bstub = new CustomSparkConnectBlockingStub(channel, state)
    stub = new CustomSparkConnectStub(channel, state)
    artifactManager = new ArtifactManager(Configuration(), "", bstub, stub)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    startDummyServer()
    createArtifactManager()
    client = null
  }

  override def afterEach(): Unit = {
    if (server != null) {
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS), "server failed to shutdown")
    }

    if (channel != null) {
      channel.shutdownNow()
    }

    if (client != null) {
      client.shutdown()
    }
  }

  private val CHUNK_SIZE: Int = 32 * 1024

  protected lazy val artifactFilePath: Path = {
    val dir = Files.createTempDirectory("artifact-tests")
    dir.toFile.deleteOnExit()
    // Generate test artifacts. The .class files and JAR entries are not valid Java
    // class files -- they contain arbitrary bytes. The tests only verify byte-level
    // transfer protocol (chunking, CRC), so valid class content is not required.
    val small = "public class smallClassFile {}".getBytes
    Files.write(dir.resolve("smallClassFile.class"), small)
    Files.write(dir.resolve("smallClassFileDup.class"), small)
    Files.write(dir.resolve("Hello.class"), "Hello".getBytes)
    // small JAR
    val smallJar = dir.resolve("smallJar.jar")
    val jos = new JarOutputStream(new FileOutputStream(smallJar.toFile))
    try {
      jos.putNextEntry(new JarEntry("Dummy.class"))
      jos.write(small)
    } finally { jos.close() }
    // large JAR (>32KB) - use STORED to avoid compression
    val largeJar = dir.resolve("largeJar.jar")
    val jos2 = new JarOutputStream(new FileOutputStream(largeJar.toFile))
    try {
      for (i <- 0 until 100) {
        val data = new Array[Byte](4096)
        java.util.Arrays.fill(data, i.toByte)
        val entry = new JarEntry(s"pkg/Class$i.class")
        entry.setMethod(java.util.zip.ZipEntry.STORED)
        entry.setSize(data.length)
        entry.setCompressedSize(data.length)
        val crc = new CRC32()
        crc.update(data)
        entry.setCrc(crc.getValue)
        jos2.putNextEntry(entry)
        jos2.write(data)
      }
    } finally { jos2.close() }
    dir
  }

  private def getCrcValues(filePath: Path): Seq[Long] = {
    val data = Files.readAllBytes(filePath)
    data
      .grouped(CHUNK_SIZE)
      .map { chunk =>
        val c = new CRC32()
        c.update(chunk)
        c.getValue
      }
      .toSeq
  }

  /**
   * Check if the data sent to the server (stored in `artifactChunk`) is equivalent to the local
   * data at `localPath`.
   * @param artifactChunk
   * @param localPath
   */
  private def assertFileDataEquality(
      artifactChunk: AddArtifactsRequest.ArtifactChunk,
      localPath: Path): Unit = {
    val localData = ByteString.readFrom(Files.newInputStream(localPath))
    val expectedCrc = getCrcValues(localPath).head
    assert(artifactChunk.getData == localData)
    assert(artifactChunk.getCrc == expectedCrc)
  }

  private def singleChunkArtifactTest(path: String): Unit = {
    test(s"Single Chunk Artifact - $path") {
      val artifactPath = artifactFilePath.resolve(path)
      artifactManager.addArtifact(artifactPath.toString)

      val receivedRequests = service.getAndClearLatestAddArtifactRequests()
      // Single `AddArtifactRequest`
      assert(receivedRequests.size == 1)

      val request = receivedRequests.head
      assert(request.hasBatch)

      val batch = request.getBatch
      // Single artifact in batch
      assert(batch.getArtifactsList.size() == 1)

      val singleChunkArtifact = batch.getArtifacts(0)
      val namePrefix = artifactPath.getFileName.toString match {
        case jar if jar.endsWith(".jar") => "jars"
        case cf if cf.endsWith(".class") => "classes"
      }
      assert(singleChunkArtifact.getName.equals(namePrefix + "/" + path))
      assertFileDataEquality(singleChunkArtifact.getData, artifactPath)
    }
  }

  singleChunkArtifactTest("smallClassFile.class")

  singleChunkArtifactTest("smallJar.jar")

  private def readNextChunk(in: InputStream): ByteString = {
    val buf = new Array[Byte](CHUNK_SIZE)
    var bytesRead = 0
    var count = 0
    while (count != -1 && bytesRead < CHUNK_SIZE) {
      count = in.read(buf, bytesRead, CHUNK_SIZE - bytesRead)
      if (count != -1) {
        bytesRead += count
      }
    }
    if (bytesRead == 0) ByteString.empty()
    else ByteString.copyFrom(buf, 0, bytesRead)
  }

  /**
   * Reads data in a chunk of `CHUNK_SIZE` bytes from `in` and verify equality with server-side
   * data stored in `chunk`.
   * @param in
   * @param chunk
   * @return
   */
  private def checkChunksDataAndCrc(
      filePath: Path,
      chunks: Seq[AddArtifactsRequest.ArtifactChunk]): Unit = {
    val in = Files.newInputStream(filePath)
    val crcs = getCrcValues(filePath)
    chunks.zip(crcs).foreach { case (chunk, expectedCrc) =>
      val expectedData = readNextChunk(in)
      chunk.getData == expectedData && chunk.getCrc == expectedCrc
    }
  }

  test("Chunked Artifact - largeJar.jar") {
    val artifactPath = artifactFilePath.resolve("largeJar.jar")
    artifactManager.addArtifact(artifactPath.toString)
    val largeJarSize = Files.size(artifactPath)
    val expectedChunks = ((largeJarSize + CHUNK_SIZE - 1) / CHUNK_SIZE).toInt
    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    assert(receivedRequests.size == expectedChunks)
    assert(receivedRequests.head.hasBeginChunk)
    val beginChunkRequest = receivedRequests.head.getBeginChunk
    assert(beginChunkRequest.getName == "jars/largeJar.jar")
    assert(beginChunkRequest.getTotalBytes == largeJarSize)
    assert(beginChunkRequest.getNumChunks == expectedChunks)
    val dataChunks = Seq(beginChunkRequest.getInitialChunk) ++
      receivedRequests.drop(1).map(_.getChunk)
    checkChunksDataAndCrc(artifactPath, dataChunks)
  }

  test("Batched SingleChunkArtifacts") {
    val path1 = artifactFilePath.resolve("smallClassFile.class")
    val file1 = path1.toUri
    val path2 = artifactFilePath.resolve("smallJar.jar")
    val file2 = path2.toUri
    artifactManager.addArtifacts(Seq(file1, file2))
    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    // Single request containing 2 artifacts.
    assert(receivedRequests.size == 1)

    val request = receivedRequests.head
    assert(request.hasBatch)

    val batch = request.getBatch
    assert(batch.getArtifactsList.size() == 2)

    val artifacts = batch.getArtifactsList
    assert(artifacts.get(0).getName == "classes/smallClassFile.class")
    assert(artifacts.get(1).getName == "jars/smallJar.jar")

    assertFileDataEquality(artifacts.get(0).getData, Paths.get(file1))
    assertFileDataEquality(artifacts.get(1).getData, Paths.get(file2))
  }

  test("Mix of SingleChunkArtifact and chunked artifact") {
    val path1 = artifactFilePath.resolve("smallClassFile.class")
    val file1 = path1.toUri
    val path2 = artifactFilePath.resolve("largeJar.jar")
    val file2 = path2.toUri
    val path3 = artifactFilePath.resolve("smallClassFileDup.class")
    val file3 = path3.toUri
    val path4 = artifactFilePath.resolve("smallJar.jar")
    val file4 = path4.toUri
    artifactManager.addArtifacts(Seq(file1, file2, file3, file4))
    val largeJarSize = Files.size(path2)
    val largeJarChunks = ((largeJarSize + CHUNK_SIZE - 1) / CHUNK_SIZE).toInt
    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    // The 1st request contains a single artifact - smallClassFile.class (There are no
    // other artifacts batched with it since the next one is large multi-chunk artifact)
    // Next `largeJarChunks` requests belong to the transfer of largeJar.jar.
    // The last request contains both smallClassFileDup.class and smallJar.jar batched together.
    assert(receivedRequests.size == 1 + largeJarChunks + 1)

    val firstReqBatch = receivedRequests.head.getBatch.getArtifactsList
    assert(firstReqBatch.size() == 1)
    assert(firstReqBatch.get(0).getName == "classes/smallClassFile.class")
    assertFileDataEquality(firstReqBatch.get(0).getData, Paths.get(file1))

    val secondReq = receivedRequests(1)
    assert(secondReq.hasBeginChunk)
    val beginChunkRequest = secondReq.getBeginChunk
    assert(beginChunkRequest.getName == "jars/largeJar.jar")
    assert(beginChunkRequest.getTotalBytes == largeJarSize)
    assert(beginChunkRequest.getNumChunks == largeJarChunks)
    // Large artifact data chunks follow the begin chunk request.
    val dataChunks = Seq(beginChunkRequest.getInitialChunk) ++
      receivedRequests.drop(2).dropRight(1).map(_.getChunk)
    checkChunksDataAndCrc(Paths.get(file2), dataChunks)

    val lastBatch = receivedRequests.last.getBatch
    assert(lastBatch.getArtifactsCount == 2)
    val remainingArtifacts = lastBatch.getArtifactsList
    assert(remainingArtifacts.get(0).getName == "classes/smallClassFileDup.class")
    assert(remainingArtifacts.get(1).getName == "jars/smallJar.jar")

    assertFileDataEquality(remainingArtifacts.get(0).getData, Paths.get(file3))
    assertFileDataEquality(remainingArtifacts.get(1).getData, Paths.get(file4))
  }

  test("cache an artifact and check its presence") {
    val s = "Hello, World!"
    val blob = s.getBytes("UTF-8")
    val expectedHash = sha256Hex(blob)
    assert(artifactManager.isCachedArtifact(expectedHash) === false)
    val actualHash = artifactManager.cacheArtifact(blob)
    assert(actualHash === expectedHash)
    assert(artifactManager.isCachedArtifact(expectedHash) === true)

    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    assert(receivedRequests.size == 1)
  }

  test("resolve ivy") {
    val main = new MavenCoordinate("my.artifactsuite.lib", "mylib", "0.1")
    val dep = "my.artifactsuite.dep:mydep:0.5"
    IvyTestUtils.withRepository(main, Some(dep), None) { repo =>
      val artifacts =
        Artifact.newIvyArtifacts(URI.create(s"ivy://my.artifactsuite.lib:mylib:0.1?repos=$repo"))
      assert(
        artifacts.exists(_.path.toString.contains("jars/my.artifactsuite.lib_mylib-0.1.jar")))
      // transitive dependency
      assert(
        artifacts.exists(_.path.toString.contains("jars/my.artifactsuite.dep_mydep-0.5.jar")))
    }

  }

  test("artifact with custom target") {
    val artifactPath = artifactFilePath.resolve("smallClassFile.class")
    val target = "sub/package/smallClassFile.class"
    artifactManager.addArtifact(artifactPath.toString, target)
    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    // Single `AddArtifactRequest`
    assert(receivedRequests.size == 1)

    val request = receivedRequests.head
    assert(request.hasBatch)

    val batch = request.getBatch
    // Single artifact in batch
    assert(batch.getArtifactsList.size() == 1)

    val singleChunkArtifact = batch.getArtifacts(0)
    assert(singleChunkArtifact.getName.equals(s"classes/$target"))
    assertFileDataEquality(singleChunkArtifact.getData, artifactPath)
  }

  test("in-memory artifact with custom target") {
    val artifactPath = artifactFilePath.resolve("smallClassFile.class")
    val artifactBytes = Files.readAllBytes(artifactPath)
    val target = "sub/package/smallClassFile.class"
    artifactManager.addArtifact(artifactBytes, target)
    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    // Single `AddArtifactRequest`
    assert(receivedRequests.size == 1)

    val request = receivedRequests.head
    assert(request.hasBatch)

    val batch = request.getBatch
    // Single artifact in batch
    assert(batch.getArtifactsList.size() == 1)

    val singleChunkArtifact = batch.getArtifacts(0)
    assert(singleChunkArtifact.getName.equals(s"classes/$target"))
    assert(singleChunkArtifact.getData.getData == ByteString.copyFrom(artifactBytes))
  }

  test(
    "When both source and target paths are given, extension conditions are checked " +
      "on target path") {
    val artifactPath = artifactFilePath.resolve("smallClassFile.class")
    assertThrows[UnsupportedOperationException] {
      artifactManager.addArtifact(artifactPath.toString, "dummy.extension")
    }
  }
}
