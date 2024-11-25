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

import java.io.InputStream
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.apache.commons.codec.digest.DigestUtils.sha256Hex
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.connect.proto.AddArtifactsRequest
import org.apache.spark.sql.Artifact
import org.apache.spark.sql.connect.client.SparkConnectClient.Configuration
import org.apache.spark.sql.test.ConnectFunSuite
import org.apache.spark.util.IvyTestUtils
import org.apache.spark.util.MavenUtils.MavenCoordinate

class ArtifactSuite extends ConnectFunSuite with BeforeAndAfterEach {

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
  protected def artifactFilePath: Path = commonResourcePath.resolve("artifact-tests")
  protected def artifactCrcPath: Path = artifactFilePath.resolve("crc")

  private def getCrcValues(filePath: Path): Seq[Long] = {
    val fileName = filePath.getFileName.toString
    val crcFileName = fileName.split('.').head + ".txt"
    Files
      .readAllLines(artifactCrcPath.resolve(crcFileName))
      .asScala
      .map(_.toLong)
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

  test("Chunked Artifact - junitLargeJar.jar") {
    val artifactPath = artifactFilePath.resolve("junitLargeJar.jar")
    artifactManager.addArtifact(artifactPath.toString)
    // Expected chunks = roundUp( file_size / chunk_size) = 12
    // File size of `junitLargeJar.jar` is 384581 bytes.
    val expectedChunks = (384581 + (CHUNK_SIZE - 1)) / CHUNK_SIZE
    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    assert(384581 == Files.size(artifactPath))
    assert(receivedRequests.size == expectedChunks)
    assert(receivedRequests.head.hasBeginChunk)
    val beginChunkRequest = receivedRequests.head.getBeginChunk
    assert(beginChunkRequest.getName == "jars/junitLargeJar.jar")
    assert(beginChunkRequest.getTotalBytes == 384581)
    assert(beginChunkRequest.getNumChunks == expectedChunks)
    val dataChunks = Seq(beginChunkRequest.getInitialChunk) ++
      receivedRequests.drop(1).map(_.getChunk)
    checkChunksDataAndCrc(artifactPath, dataChunks)
  }

  test("Batched SingleChunkArtifacts") {
    val file1 = artifactFilePath.resolve("smallClassFile.class").toUri
    val file2 = artifactFilePath.resolve("smallJar.jar").toUri
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
    val file1 = artifactFilePath.resolve("smallClassFile.class").toUri
    val file2 = artifactFilePath.resolve("junitLargeJar.jar").toUri
    val file3 = artifactFilePath.resolve("smallClassFileDup.class").toUri
    val file4 = artifactFilePath.resolve("smallJar.jar").toUri
    artifactManager.addArtifacts(Seq(file1, file2, file3, file4))
    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    // There are a total of 14 requests.
    // The 1st request contains a single artifact - smallClassFile.class (There are no
    // other artifacts batched with it since the next one is large multi-chunk artifact)
    // Requests 2-13 (1-indexed) belong to the transfer of junitLargeJar.jar. This includes
    // the first "beginning chunk" and the subsequent data chunks.
    // The last request (14) contains both smallClassFileDup.class and smallJar.jar batched
    // together.
    assert(receivedRequests.size == 1 + 12 + 1)

    val firstReqBatch = receivedRequests.head.getBatch.getArtifactsList
    assert(firstReqBatch.size() == 1)
    assert(firstReqBatch.get(0).getName == "classes/smallClassFile.class")
    assertFileDataEquality(firstReqBatch.get(0).getData, Paths.get(file1))

    val secondReq = receivedRequests(1)
    assert(secondReq.hasBeginChunk)
    val beginChunkRequest = secondReq.getBeginChunk
    assert(beginChunkRequest.getName == "jars/junitLargeJar.jar")
    assert(beginChunkRequest.getTotalBytes == 384581)
    assert(beginChunkRequest.getNumChunks == 12)
    // Large artifact data chunks are requests number 3 to 13.
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
