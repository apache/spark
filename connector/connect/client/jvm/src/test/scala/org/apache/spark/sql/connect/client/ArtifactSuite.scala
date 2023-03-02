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
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import java.util.zip.{CheckedInputStream, CRC32}

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.AddArtifactsRequest
import org.apache.spark.sql.connect.client.util.ConnectFunSuite

class ArtifactSuite extends ConnectFunSuite with BeforeAndAfterEach {

  private var client: SparkConnectClient = _
  private var service: DummySparkConnectService = _
  private var server: Server = _
  private var artifactManager: ArtifactManager = _
  private var channel: ManagedChannel = _

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
    artifactManager = new ArtifactManager(proto.UserContext.newBuilder().build(), channel)
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
  protected def artifactFilePath: Path = baseResourcePath.resolve("artifact-tests")

  /**
   * Check if the data sent to the server (stored in `artifactChunk`) is equivalent to the local
   * data at `localPath`.
   * @param artifactChunk
   * @param localPath
   */
  private def assertFileDataEquality(
      artifactChunk: AddArtifactsRequest.ArtifactChunk,
      localPath: Path): Unit = {
    val in = new CheckedInputStream(Files.newInputStream(localPath), new CRC32)
    val localData = ByteString.readFrom(in)
    assert(artifactChunk.getData == localData)
    assert(artifactChunk.getCrc == in.getChecksum.getValue)
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
   * Read data in a chunk of `CHUNK_SIZE` bytes from `in` and verify equality with server-side
   * data stored in `chunk`.
   * @param in
   * @param chunk
   * @return
   */
  private def checkChunkDataAndCrc(
      in: CheckedInputStream,
      chunk: AddArtifactsRequest.ArtifactChunk): Boolean = {
    val expectedData = readNextChunk(in)
    val expectedCRC = in.getChecksum.getValue
    chunk.getData == expectedData && chunk.getCrc == expectedCRC
  }

  test("Chunked Artifact - junitLargeJar.jar") {
    val artifactPath = artifactFilePath.resolve("junitLargeJar.jar")
    artifactManager.addArtifact(artifactPath.toString)
    val in = new CheckedInputStream(Files.newInputStream(artifactPath), new CRC32)

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
    checkChunkDataAndCrc(in, beginChunkRequest.getInitialChunk)
    // Check remaining `ArtifactChunk`s for data equality.
    receivedRequests.drop(1).forall(r => r.hasChunk && checkChunkDataAndCrc(in, r.getChunk))
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
    val file2in = new CheckedInputStream(Files.newInputStream(Paths.get(file2)), new CRC32)
    checkChunkDataAndCrc(file2in, beginChunkRequest.getInitialChunk)
    // Large artifact data chunks are requests number 3 to 13.
    receivedRequests
      .drop(2)
      .dropRight(1)
      .forall(r => r.hasChunk && checkChunkDataAndCrc(file2in, r.getChunk))

    val lastBatch = receivedRequests.last.getBatch
    assert(lastBatch.getArtifactsCount == 2)
    val remainingArtifacts = lastBatch.getArtifactsList
    assert(remainingArtifacts.get(0).getName == "classes/smallClassFileDup.class")
    assert(remainingArtifacts.get(1).getName == "jars/smallJar.jar")

    assertFileDataEquality(remainingArtifacts.get(0).getData, Paths.get(file3))
    assertFileDataEquality(remainingArtifacts.get(1).getData, Paths.get(file4))
  }
}
