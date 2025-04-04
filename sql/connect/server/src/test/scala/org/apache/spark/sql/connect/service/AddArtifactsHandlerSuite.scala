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
package org.apache.spark.sql.connect.service

import java.io.InputStream
import java.nio.file.{Files, Path}
import java.util.UUID

import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import com.google.rpc.ErrorInfo
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AddArtifactsRequest, AddArtifactsResponse}
import org.apache.spark.sql.connect.ResourceHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

class AddArtifactsHandlerSuite extends SharedSparkSession with ResourceHelper {

  private val CHUNK_SIZE: Int = 32 * 1024

  private val sessionId = UUID.randomUUID.toString()

  class DummyStreamObserver(p: Promise[AddArtifactsResponse])
      extends StreamObserver[AddArtifactsResponse] {
    override def onNext(v: AddArtifactsResponse): Unit = p.success(v)
    override def onError(throwable: Throwable): Unit = throw throwable
    override def onCompleted(): Unit = {}
  }

  class TestAddArtifactsHandler(responseObserver: StreamObserver[AddArtifactsResponse])
      extends SparkConnectAddArtifactsHandler(responseObserver) {

    // Stop the staged artifacts from being automatically deleted
    override protected def cleanUpStagedArtifacts(): Unit = {}

    private val finalArtifacts = mutable.Buffer.empty[String]

    // Record the artifacts that are sent out for final processing.
    override protected def addStagedArtifactToArtifactManager(artifact: StagedArtifact): Unit = {
      finalArtifacts.append(artifact.name)
    }

    def getFinalArtifacts: Seq[String] = finalArtifacts.toSeq
    def stagingDirectory: Path = this.stagingDir
    def forceCleanUp(): Unit = super.cleanUpStagedArtifacts()
  }

  protected val inputFilePath: Path = commonResourcePath.resolve("artifact-tests")
  protected val crcPath: Path = inputFilePath.resolve("crc")

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

  private def getDataChunks(filePath: Path): Seq[ByteString] = {
    val in = Files.newInputStream(filePath)
    var chunkData: ByteString = readNextChunk(in)
    val dataChunks = mutable.ListBuffer.empty[ByteString]
    while (chunkData != ByteString.empty()) {
      dataChunks.append(chunkData)
      chunkData = readNextChunk(in)
    }
    dataChunks.toSeq
  }

  private def getCrcValues(filePath: Path): Seq[Long] = {
    val fileName = filePath.getFileName.toString
    val crcFileName = fileName.split('.').head + ".txt"
    Files
      .readAllLines(crcPath.resolve(crcFileName))
      .asScala
      .map(_.toLong)
      .toSeq
  }

  private def addSingleChunkArtifact(
      handler: SparkConnectAddArtifactsHandler,
      name: String,
      artifactPath: Path): Unit = {
    val dataChunks = getDataChunks(artifactPath)
    assert(dataChunks.size == 1)
    val bytes = dataChunks.head
    val context = proto.UserContext
      .newBuilder()
      .setUserId("c1")
      .build()
    val fileNameNoExtension = artifactPath.getFileName.toString.split('.').head
    val singleChunkArtifact = proto.AddArtifactsRequest.SingleChunkArtifact
      .newBuilder()
      .setName(name)
      .setData(
        proto.AddArtifactsRequest.ArtifactChunk
          .newBuilder()
          .setData(bytes)
          .setCrc(getCrcValues(crcPath.resolve(fileNameNoExtension + ".txt")).head)
          .build())
      .build()

    val singleChunkArtifactRequest = AddArtifactsRequest
      .newBuilder()
      .setSessionId(sessionId)
      .setUserContext(context)
      .setBatch(
        proto.AddArtifactsRequest.Batch.newBuilder().addArtifacts(singleChunkArtifact).build())
      .build()

    handler.onNext(singleChunkArtifactRequest)
  }

  private def addSingleChunkArtifacts(
      handler: SparkConnectAddArtifactsHandler,
      names: Seq[String],
      artifactPaths: Seq[Path]): Unit = {
    names.zip(artifactPaths).foreach { case (name, path) =>
      addSingleChunkArtifact(handler, name, path)
    }
  }

  private def addChunkedArtifact(
      handler: SparkConnectAddArtifactsHandler,
      name: String,
      artifactPath: Path): Unit = {
    val dataChunks = getDataChunks(artifactPath)
    val crcs = getCrcValues(artifactPath)
    assert(dataChunks.size == crcs.size)
    val artifactChunks = dataChunks.zip(crcs).map { case (chunk, crc) =>
      proto.AddArtifactsRequest.ArtifactChunk.newBuilder().setData(chunk).setCrc(crc).build()
    }

    val context = proto.UserContext
      .newBuilder()
      .setUserId("c1")
      .build()
    val beginChunkedArtifact = proto.AddArtifactsRequest.BeginChunkedArtifact
      .newBuilder()
      .setName(name)
      .setNumChunks(artifactChunks.size)
      .setTotalBytes(Files.size(artifactPath))
      .setInitialChunk(artifactChunks.head)
      .build()

    val requestBuilder = AddArtifactsRequest
      .newBuilder()
      .setSessionId(sessionId)
      .setUserContext(context)
      .setBeginChunk(beginChunkedArtifact)

    handler.onNext(requestBuilder.build())
    requestBuilder.clearBeginChunk()
    artifactChunks.drop(1).foreach { dataChunk =>
      requestBuilder.setChunk(dataChunk)
      handler.onNext(requestBuilder.build())
    }
  }

  test("single chunk artifact") {
    val promise = Promise[AddArtifactsResponse]()
    val handler = new TestAddArtifactsHandler(new DummyStreamObserver(promise))
    try {
      val name = "classes/smallClassFile.class"
      val artifactPath = inputFilePath.resolve("smallClassFile.class")
      assume(artifactPath.toFile.exists)
      addSingleChunkArtifact(handler, name, artifactPath)
      handler.onCompleted()
      val response = ThreadUtils.awaitResult(promise.future, 5.seconds)
      val summaries = response.getArtifactsList.asScala.toSeq
      assert(summaries.size == 1)
      assert(summaries.head.getName == name)
      assert(summaries.head.getIsCrcSuccessful)

      val writtenFile = handler.stagingDirectory.resolve(name)
      assert(writtenFile.toFile.exists())
      val writtenBytes = ByteString.readFrom(Files.newInputStream(writtenFile))
      val expectedBytes = ByteString.readFrom(Files.newInputStream(artifactPath))
      assert(writtenBytes == expectedBytes)
    } finally {
      handler.forceCleanUp()
    }
  }

  test("Multi chunk artifact") {
    val promise = Promise[AddArtifactsResponse]()
    val handler = new TestAddArtifactsHandler(new DummyStreamObserver(promise))
    try {
      val name = "jars/junitLargeJar.jar"
      val artifactPath = inputFilePath.resolve("junitLargeJar.jar")
      assume(artifactPath.toFile.exists)
      addChunkedArtifact(handler, name, artifactPath)
      handler.onCompleted()
      val response = ThreadUtils.awaitResult(promise.future, 5.seconds)
      val summaries = response.getArtifactsList.asScala.toSeq
      assert(summaries.size == 1)
      assert(summaries.head.getName == name)
      assert(summaries.head.getIsCrcSuccessful)

      val writtenFile = handler.stagingDirectory.resolve(name)
      assert(writtenFile.toFile.exists())
      val writtenBytes = ByteString.readFrom(Files.newInputStream(writtenFile))
      val expectedByes = ByteString.readFrom(Files.newInputStream(artifactPath))
      assert(writtenBytes == expectedByes)
    } finally {
      handler.forceCleanUp()
    }
  }

  test("Mix of single-chunk and chunked artifacts") {
    val promise = Promise[AddArtifactsResponse]()
    val handler = new TestAddArtifactsHandler(new DummyStreamObserver(promise))
    try {
      val names = Seq(
        "classes/smallClassFile.class",
        "jars/junitLargeJar.jar",
        "classes/smallClassFileDup.class",
        "jars/smallJar.jar")

      val artifactPaths = Seq(
        inputFilePath.resolve("smallClassFile.class"),
        inputFilePath.resolve("junitLargeJar.jar"),
        inputFilePath.resolve("smallClassFileDup.class"),
        inputFilePath.resolve("smallJar.jar"))
      artifactPaths.foreach(p => assume(p.toFile.exists))

      addSingleChunkArtifact(handler, names.head, artifactPaths.head)
      addChunkedArtifact(handler, names(1), artifactPaths(1))
      addSingleChunkArtifacts(handler, names.drop(2), artifactPaths.drop(2))
      handler.onCompleted()
      val response = ThreadUtils.awaitResult(promise.future, 5.seconds)
      val summaries = response.getArtifactsList.asScala.toSeq
      assert(summaries.size == 4)
      summaries.zip(names).foreach { case (summary, name) =>
        assert(summary.getName == name)
        assert(summary.getIsCrcSuccessful)
      }

      val writtenFiles = names.map(name => handler.stagingDirectory.resolve(name))
      writtenFiles.zip(artifactPaths).foreach { case (writtenFile, artifactPath) =>
        assert(writtenFile.toFile.exists())
        val writtenBytes = ByteString.readFrom(Files.newInputStream(writtenFile))
        val expectedByes = ByteString.readFrom(Files.newInputStream(artifactPath))
        assert(writtenBytes == expectedByes)
      }
    } finally {
      handler.forceCleanUp()
    }
  }

  test("Artifacts that fail CRC are not added to the artifact manager") {
    val promise = Promise[AddArtifactsResponse]()
    val handler = new TestAddArtifactsHandler(new DummyStreamObserver(promise))
    try {
      val name = "classes/smallClassFile.class"
      val artifactPath = inputFilePath.resolve("smallClassFile.class")
      assume(artifactPath.toFile.exists)
      val dataChunks = getDataChunks(artifactPath)
      assert(dataChunks.size == 1)
      val bytes = dataChunks.head
      val context = proto.UserContext
        .newBuilder()
        .setUserId("c1")
        .build()
      val singleChunkArtifact = proto.AddArtifactsRequest.SingleChunkArtifact
        .newBuilder()
        .setName(name)
        .setData(
          proto.AddArtifactsRequest.ArtifactChunk
            .newBuilder()
            .setData(bytes)
            // Set a dummy CRC value
            .setCrc(12345)
            .build())
        .build()

      val singleChunkArtifactRequest = AddArtifactsRequest
        .newBuilder()
        .setSessionId(sessionId)
        .setUserContext(context)
        .setBatch(
          proto.AddArtifactsRequest.Batch.newBuilder().addArtifacts(singleChunkArtifact).build())
        .build()

      handler.onNext(singleChunkArtifactRequest)
      handler.onCompleted()
      val response = ThreadUtils.awaitResult(promise.future, 5.seconds)
      val summaries = response.getArtifactsList.asScala.toSeq
      assert(summaries.size == 1)
      assert(summaries.head.getName == name)
      assert(!summaries.head.getIsCrcSuccessful)

      assert(handler.getFinalArtifacts.isEmpty)
    } finally {
      handler.forceCleanUp()
    }
  }

  private def createDummyArtifactRequests(name: String): Seq[proto.AddArtifactsRequest] = {
    val bytes = ByteString.EMPTY
    val context = proto.UserContext
      .newBuilder()
      .setUserId("c1")
      .build()

    val singleChunkArtifact = proto.AddArtifactsRequest.SingleChunkArtifact
      .newBuilder()
      .setName(name)
      .setData(
        proto.AddArtifactsRequest.ArtifactChunk
          .newBuilder()
          .setData(bytes)
          // Set a dummy CRC value
          .setCrc(12345)
          .build())
      .build()

    val singleChunkArtifactRequest = AddArtifactsRequest
      .newBuilder()
      .setSessionId(sessionId)
      .setUserContext(context)
      .setBatch(
        proto.AddArtifactsRequest.Batch.newBuilder().addArtifacts(singleChunkArtifact).build())
      .build()

    val beginChunkedArtifact = proto.AddArtifactsRequest.BeginChunkedArtifact
      .newBuilder()
      .setName(name)
      .setNumChunks(1)
      .setTotalBytes(1)
      .setInitialChunk(
        proto.AddArtifactsRequest.ArtifactChunk.newBuilder().setData(bytes).setCrc(12345).build())
      .build()

    val beginChunkArtifactRequest = AddArtifactsRequest
      .newBuilder()
      .setSessionId(sessionId)
      .setUserContext(context)
      .setBeginChunk(beginChunkedArtifact)
      .build()

    Seq(singleChunkArtifactRequest, beginChunkArtifactRequest)
  }

  test("Artifacts names are not allowed to be absolute paths") {
    val promise = Promise[AddArtifactsResponse]()
    val handler = new TestAddArtifactsHandler(new DummyStreamObserver(promise))
    try {
      val name = "/absolute/path/"
      val request = createDummyArtifactRequests(name)
      request.foreach { req =>
        val e = intercept[StatusRuntimeException] {
          handler.onNext(req)
        }
        assert(e.getStatus.getCode == Code.INTERNAL)
        val statusProto = StatusProto.fromThrowable(e)
        assert(statusProto.getDetailsCount == 1)
        val details = statusProto.getDetails(0)
        val info = details.unpack(classOf[ErrorInfo])
        assert(info.getReason.contains("java.lang.IllegalArgumentException"))
      }
      handler.onCompleted()
    } finally {
      handler.forceCleanUp()
    }
  }

  test("Artifact name/paths cannot reference parent/sibling/nephew directories") {
    val promise = Promise[AddArtifactsResponse]()
    val handler = new TestAddArtifactsHandler(new DummyStreamObserver(promise))
    try {
      val names = Seq("..", "../sibling", "../nephew/directory", "a/../../b", "x/../y/../..")
      val request = names.flatMap(createDummyArtifactRequests)
      request.foreach { req =>
        val e = intercept[StatusRuntimeException] {
          handler.onNext(req)
        }
        assert(e.getStatus.getCode == Code.INTERNAL)
        val statusProto = StatusProto.fromThrowable(e)
        assert(statusProto.getDetailsCount == 1)
        val details = statusProto.getDetails(0)
        val info = details.unpack(classOf[ErrorInfo])
        assert(info.getReason.contains("java.lang.IllegalArgumentException"))
      }
      handler.onCompleted()
    } finally {
      handler.forceCleanUp()
    }
  }

}
