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

import java.io.File
import java.net.{URI, URISyntaxException}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import java.util.zip.{CheckedOutputStream, CRC32}

import scala.collection.mutable
import scala.util.control.NonFatal

import io.grpc.Context
import io.grpc.stub.StreamObserver

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AddArtifactsRequest, AddArtifactsResponse}
import org.apache.spark.connect.proto.AddArtifactsResponse.ArtifactSummary
import org.apache.spark.sql.Artifact
import org.apache.spark.sql.artifact.ArtifactManager
import org.apache.spark.sql.connect.utils.ErrorUtils
import org.apache.spark.sql.util.ArtifactUtils
import org.apache.spark.util.RuntimeDependencyResolver.RejectRequestedRepositories
import org.apache.spark.util.Utils

/**
 * Handles [[AddArtifactsRequest]]s for the [[SparkConnectService]].
 *
 * @param responseObserver
 */
class SparkConnectAddArtifactsHandler(val responseObserver: StreamObserver[AddArtifactsResponse])
    extends StreamObserver[AddArtifactsRequest] {

  // Temporary directory where artifacts are rebuilt from the bytes sent over the wire.
  protected val stagingDir: Path = Utils.createTempDir().toPath
  private sealed trait PendingArtifact
  private case class PendingStagedArtifact(artifact: StagedArtifact) extends PendingArtifact
  private case class PendingMavenDependency(uri: URI) extends PendingArtifact
  private val pendingArtifacts = mutable.Buffer.empty[PendingArtifact]
  private val grpcContext = Context.current()
  // If not null, indicates the currently active chunked artifact that is being rebuilt from
  // several [[AddArtifactsRequest]]s.
  private var chunkedArtifact: StagedChunkedArtifact = _
  private var holder: SessionHolder = _

  override def onNext(req: AddArtifactsRequest): Unit = try {
    if (this.holder == null) {
      val previousSessionId = req.hasClientObservedServerSideSessionId match {
        case true => Some(req.getClientObservedServerSideSessionId)
        case false => None
      }
      this.holder = SparkConnectService.getOrCreateIsolatedSession(
        req.getUserContext.getUserId,
        req.getSessionId,
        previousSessionId)
    }

    if (req.hasBeginChunk) {
      // The beginning of a multi-chunk artifact.
      require(chunkedArtifact == null)
      chunkedArtifact = writeArtifactToFile(req.getBeginChunk)
    } else if (req.hasChunk) {
      // We are currently processing a multi-chunk artifact
      require(chunkedArtifact != null && !chunkedArtifact.isFinished)
      chunkedArtifact.write(req.getChunk)

      if (chunkedArtifact.isFinished) {
        chunkedArtifact.close()
        // Unset the currently active chunked artifact.
        chunkedArtifact = null
      }
    } else if (req.hasBatch) {
      val batch = req.getBatch
      if (batch.getArtifactsCount > 0 && batch.getEntriesCount > 0) {
        throw SparkException.internalError(
          "AddArtifacts batch cannot contain both legacy artifacts and ordered entries")
      }
      batch.getArtifactsList.forEach(artifact => writeArtifactToFile(artifact).close())
      batch.getEntriesList.forEach { entry =>
        entry.getValueCase match {
          case proto.AddArtifactsRequest.ArtifactEntry.ValueCase.ARTIFACT =>
            writeArtifactToFile(entry.getArtifact).close()
          case proto.AddArtifactsRequest.ArtifactEntry.ValueCase.MAVEN_DEPENDENCY =>
            addMavenDependency(entry.getMavenDependency)
          case _ =>
            throw SparkException.internalError("AddArtifacts ordered entry has no value")
        }
      }
    } else {
      throw new UnsupportedOperationException(s"Unsupported data transfer request: $req")
    }
  } catch {
    ErrorUtils.handleError(
      "addArtifacts.onNext",
      responseObserver,
      req.getUserContext.getUserId,
      req.getSessionId,
      None,
      false,
      Some(() => {
        cleanUpStagedArtifacts()
      }))
  }

  override def onError(throwable: Throwable): Unit = {
    cleanUpStagedArtifacts()
    responseObserver.onError(throwable)
  }

  private def addMavenDependency(dependency: proto.AddArtifactsRequest.MavenDependency): Unit = {
    val value = dependency.getUri
    val uri =
      try new URI(value)
      catch {
        case _: URISyntaxException => null
      }
    if (uri == null || uri.getScheme != "ivy") {
      throw SparkException.internalError(s"Maven dependency must be an ivy URI: $value")
    }
    pendingArtifacts += PendingMavenDependency(uri)
  }

  protected def addStagedArtifactToArtifactManager(artifact: StagedArtifact): Unit = {
    require(holder != null)
    holder.addArtifact(artifact.path, artifact.stagedPath, artifact.fragment)
  }

  protected def resolveMavenDependency(
      uri: URI,
      connectTimeoutMs: Int,
      readTimeoutMs: Int,
      isCancelled: () => Boolean): Seq[Artifact] = {
    holder.artifactManager.resolveArtifacts(
      uri,
      RejectRequestedRepositories,
      connectTimeoutMs,
      readTimeoutMs,
      isCancelled)
  }

  private def resolveMavenDependencies(dependencies: Seq[URI]): Map[URI, Seq[Artifact]] = {
    dependencies.map { uri =>
      val timeoutCapMs = Option(grpcContext.getDeadline)
        .map(_.timeRemaining(TimeUnit.MILLISECONDS))
        .map(math.min(_, Int.MaxValue.toLong))
        .getOrElse(Int.MaxValue.toLong)
        .toInt
      if (timeoutCapMs <= 0) {
        throw SparkException.internalError(
          "AddArtifacts deadline expired before Maven resolution")
      }
      val connectTimeoutMs = math.min(timeoutCapMs, holder.artifactManager.ivyConnectTimeoutMs)
      val readTimeoutMs = math.min(timeoutCapMs, holder.artifactManager.ivyReadTimeoutMs)
      uri -> resolveMavenDependency(
        uri,
        connectTimeoutMs,
        readTimeoutMs,
        () => grpcContext.isCancelled || Thread.currentThread().isInterrupted)
    }.toMap
  }

  private def stageResolvedArtifact(artifact: Artifact): StagedArtifact = {
    val localFile = artifact.storage match {
      case file: Artifact.LocalFile => file.path
      case other =>
        throw SparkException.internalError(
          s"Resolved Maven artifact has unexpected storage: ${other.getClass.getName}")
    }
    val physicalPath = Files.createTempFile(stagingDir, "resolved-maven-", ".jar")
    val staged = new StagedArtifact(artifact.path.toString, Some(physicalPath))
    Utils.tryWithSafeFinally {
      staged.writeFrom(localFile)
      staged
    }(staged.close())
  }

  private def prepareArtifacts(): Seq[StagedArtifact] = {
    val dependencies = pendingArtifacts
      .collect { case PendingMavenDependency(uri) =>
        uri
      }
      .distinct
      .toSeq
    val resolved = resolveMavenDependencies(dependencies)
    pendingArtifacts.flatMap {
      case PendingStagedArtifact(artifact) => Seq(artifact)
      case PendingMavenDependency(uri) => resolved(uri).map(stageResolvedArtifact)
    }.toSeq
  }

  /**
   * Process all the staged artifacts built in this stream.
   *
   * @return
   */
  protected def flushStagedArtifacts(): Seq[ArtifactSummary] = {
    val failedArtifactExceptions = mutable.ListBuffer[SparkRuntimeException]()

    // Resolve the complete ordered batch before mutating session state.
    val summaries = prepareArtifacts().map { artifact =>
      try {
        // We do not store artifacts that fail the CRC. The failure is reported in the artifact
        // summary and it is up to the client to decide whether to retry sending the artifact.
        if (artifact.getCrcStatus.contains(true)) {
          if (artifact.path.startsWith(ArtifactManager.forwardToFSPrefix + File.separator)) {
            holder.artifactManager.uploadArtifactToFs(artifact.path, artifact.stagedPath)
          } else {
            addStagedArtifactToArtifactManager(artifact)
          }
        }
      } catch {
        case e: SparkRuntimeException if e.getCondition == "ARTIFACT_ALREADY_EXISTS" =>
          failedArtifactExceptions += e
      }
      artifact.summary()
    }.toSeq

    if (failedArtifactExceptions.nonEmpty) {
      throw ArtifactUtils.mergeExceptionsWithSuppressed(failedArtifactExceptions.toSeq)
    }

    summaries
  }

  protected def cleanUpStagedArtifacts(): Unit = Utils.deleteRecursively(stagingDir.toFile)

  override def onCompleted(): Unit = {
    try {
      val artifactSummaries = flushStagedArtifacts()
      // Add the artifacts to the session and return the summaries to the client.
      val builder = proto.AddArtifactsResponse.newBuilder()
      builder.setSessionId(holder.sessionId)
      builder.setServerSideSessionId(holder.serverSessionId)
      artifactSummaries.foreach(summary => builder.addArtifacts(summary))
      // Delete temp dir
      cleanUpStagedArtifacts()

      // Send the summaries and close
      responseObserver.onNext(builder.build())
      responseObserver.onCompleted()
    } catch {
      ErrorUtils.handleError(
        "addArtifacts.onComplete",
        responseObserver,
        holder.userId,
        holder.sessionId,
        None,
        false,
        Some(() => {
          cleanUpStagedArtifacts()
        }))
    }
  }

  /**
   * Create a (temporary) file for a single-chunk artifact.
   */
  private def writeArtifactToFile(
      artifact: proto.AddArtifactsRequest.SingleChunkArtifact): StagedArtifact = {
    val stagedDep = new StagedArtifact(artifact.getName)
    pendingArtifacts += PendingStagedArtifact(stagedDep)
    stagedDep.write(artifact.getData)
    stagedDep
  }

  /**
   * Create a (temporary) file for the multi-chunk artifact and write the initial chunk. Further
   * chunks can be appended to the file.
   */
  private def writeArtifactToFile(
      artifact: proto.AddArtifactsRequest.BeginChunkedArtifact): StagedChunkedArtifact = {
    val stagedChunkedArtifact =
      new StagedChunkedArtifact(artifact.getName, artifact.getNumChunks, artifact.getTotalBytes)
    pendingArtifacts += PendingStagedArtifact(stagedChunkedArtifact)
    stagedChunkedArtifact.write(artifact.getInitialChunk)
    stagedChunkedArtifact
  }

  /**
   * Handles rebuilding an artifact from bytes sent over the wire.
   */
  class StagedArtifact(val name: String, physicalPath: Option[Path] = None) {
    // Workaround to keep the fragment.
    val (canonicalFileName: String, fragment: Option[String]) =
      if (name.startsWith(s"archives${File.separator}")) {
        val splits = name.split("#")
        assert(splits.length <= 2, "'#' in the path is not supported for adding an archive.")
        if (splits.length == 2) {
          (splits(0), Some(splits(1)))
        } else {
          (splits(0), None)
        }
      } else {
        (name, None)
      }

    val path: Path = Paths.get(canonicalFileName)
    if (path.isAbsolute) {
      throw new SparkRuntimeException(
        errorClass = "INVALID_ARTIFACT_PATH",
        messageParameters = Map("name" -> name))
    }
    val stagedPath: Path = physicalPath.getOrElse {
      val requestedPath =
        try {
          ArtifactUtils.concatenatePaths(stagingDir, path)
        } catch {
          case _: IllegalArgumentException =>
            throw new SparkRuntimeException(
              errorClass = "INVALID_ARTIFACT_PATH",
              messageParameters = Map("name" -> name))
          case NonFatal(e) => throw e
        }
      if (Files.exists(requestedPath)) {
        Files.createTempFile(stagingDir, "duplicate-artifact-", ".tmp")
      } else {
        requestedPath
      }
    }

    Files.createDirectories(stagedPath.getParent)

    private val fileOut = Files.newOutputStream(stagedPath)
    private val checksumOut = new CheckedOutputStream(fileOut, new CRC32)
    private val overallChecksum = new CRC32()

    private val builder = ArtifactSummary.newBuilder().setName(name)
    private var artifactSummary: ArtifactSummary = _
    protected var isCrcSuccess: Boolean = _

    protected def updateCrc(isSuccess: Boolean): Unit = {
      isCrcSuccess = isSuccess
    }

    def getCrcStatus: Option[Boolean] = Option(isCrcSuccess)

    def getCrc: Long = overallChecksum.getValue

    def write(dataChunk: proto.AddArtifactsRequest.ArtifactChunk): Unit = {
      try dataChunk.getData.writeTo(checksumOut)
      catch {
        case NonFatal(e) =>
          close()
          throw e
      }

      val bytes = dataChunk.getData.toByteArray
      overallChecksum.update(bytes)
      updateCrc(checksumOut.getChecksum.getValue == dataChunk.getCrc)
      checksumOut.getChecksum.reset()
    }

    def writeFrom(file: Path): Unit = {
      val in = Files.newInputStream(file)
      try {
        val buffer = new Array[Byte](64 * 1024)
        var read = in.read(buffer)
        while (read != -1) {
          checksumOut.write(buffer, 0, read)
          overallChecksum.update(buffer, 0, read)
          read = in.read(buffer)
        }
        updateCrc(isSuccess = true)
      } finally {
        in.close()
      }
    }

    def close(): Unit = {
      if (artifactSummary == null) {
        checksumOut.close()
        artifactSummary = builder
          .setName(name)
          .setIsCrcSuccessful(getCrcStatus.getOrElse(false))
          .build()
      }
    }

    def summary(): ArtifactSummary = {
      require(artifactSummary != null)
      artifactSummary
    }
  }

  /**
   * Extends [[StagedArtifact]] to handle multi-chunk artifacts.
   *
   * @param name
   * @param numChunks
   * @param totalBytes
   */
  class StagedChunkedArtifact(name: String, numChunks: Long, totalBytes: Long)
      extends StagedArtifact(name) {

    private var remainingChunks = numChunks
    private var totalBytesProcessed = 0L
    private var isFirstCrcUpdate = true

    def isFinished: Boolean = remainingChunks == 0

    override protected def updateCrc(isSuccess: Boolean): Unit = {
      // The overall artifact CRC is a success if and only if all the individual chunk CRCs match.
      isCrcSuccess = isSuccess && (isCrcSuccess || isFirstCrcUpdate)
      isFirstCrcUpdate = false
    }

    override def write(dataChunk: proto.AddArtifactsRequest.ArtifactChunk): Unit = {
      if (remainingChunks == 0) {
        throw new RuntimeException(
          s"Excessive data chunks for artifact: $name, " +
            s"expected $numChunks chunks in total. Processed $totalBytesProcessed bytes out of" +
            s" $totalBytes bytes.")
      }
      super.write(dataChunk)
      totalBytesProcessed += dataChunk.getData.size()
      remainingChunks -= 1
    }

    override def close(): Unit = {
      if (remainingChunks != 0 || totalBytesProcessed != totalBytes) {
        throw new RuntimeException(
          s"Missing data chunks for artifact: $name. Expected " +
            s"$numChunks chunks and received ${numChunks - remainingChunks} chunks. Processed" +
            s" $totalBytesProcessed bytes out of $totalBytes bytes.")
      }
      super.close()
    }
  }
}
