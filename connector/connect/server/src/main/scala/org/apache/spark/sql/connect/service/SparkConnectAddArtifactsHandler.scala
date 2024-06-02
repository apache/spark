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
import java.nio.file.{Files, Path, Paths}
import java.util.zip.{CheckedOutputStream, CRC32}

import scala.collection.mutable
import scala.util.control.NonFatal

import com.google.common.io.CountingOutputStream
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AddArtifactsRequest, AddArtifactsResponse}
import org.apache.spark.connect.proto.AddArtifactsResponse.ArtifactSummary
import org.apache.spark.sql.artifact.ArtifactManager
import org.apache.spark.sql.artifact.util.ArtifactUtils
import org.apache.spark.sql.connect.utils.ErrorUtils
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
  protected val stagedArtifacts: mutable.Buffer[StagedArtifact] =
    mutable.Buffer.empty[StagedArtifact]
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
      // Each artifact in the batch is single-chunked.
      req.getBatch.getArtifactsList.forEach(artifact => writeArtifactToFile(artifact).close())
    } else {
      throw new UnsupportedOperationException(s"Unsupported data transfer request: $req")
    }
  } catch {
    ErrorUtils.handleError(
      "addArtifacts.onNext",
      responseObserver,
      holder.userId,
      holder.sessionId,
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

  protected def addStagedArtifactToArtifactManager(artifact: StagedArtifact): Unit = {
    require(holder != null)
    holder.addArtifact(artifact.path, artifact.stagedPath, artifact.fragment)
  }

  /**
   * Process all the staged artifacts built in this stream.
   *
   * @return
   */
  protected def flushStagedArtifacts(): Seq[ArtifactSummary] = {
    // Non-lazy transformation when using Buffer.
    stagedArtifacts.map { artifact =>
      // We do not store artifacts that fail the CRC. The failure is reported in the artifact
      // summary and it is up to the client to decide whether to retry sending the artifact.
      if (artifact.getCrcStatus.contains(true)) {
        if (artifact.path.startsWith(ArtifactManager.forwardToFSPrefix + File.separator)) {
          holder.artifactManager.uploadArtifactToFs(artifact.path, artifact.stagedPath)
        } else {
          addStagedArtifactToArtifactManager(artifact)
        }
      }
      artifact.summary()
    }.toSeq
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
    stagedArtifacts += stagedDep
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
    stagedArtifacts += stagedChunkedArtifact
    stagedChunkedArtifact.write(artifact.getInitialChunk)
    stagedChunkedArtifact
  }

  /**
   * Handles rebuilding an artifact from bytes sent over the wire.
   */
  class StagedArtifact(val name: String) {
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
    val stagedPath: Path =
      try {
        ArtifactUtils.concatenatePaths(stagingDir, path)
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Artifact with name: $name is invalid. The `name` " +
              s"must be a relative path and cannot reference parent/sibling/nephew directories.")
        case NonFatal(e) => throw e
      }

    Files.createDirectories(stagedPath.getParent)

    private val fileOut = Files.newOutputStream(stagedPath)
    private val countingOut = new CountingOutputStream(fileOut)
    private val checksumOut = new CheckedOutputStream(countingOut, new CRC32)

    private val builder = ArtifactSummary.newBuilder().setName(name)
    private var artifactSummary: ArtifactSummary = _
    protected var isCrcSuccess: Boolean = _

    protected def updateCrc(isSuccess: Boolean): Unit = {
      isCrcSuccess = isSuccess
    }

    def getCrcStatus: Option[Boolean] = Option(isCrcSuccess)

    def write(dataChunk: proto.AddArtifactsRequest.ArtifactChunk): Unit = {
      try dataChunk.getData.writeTo(checksumOut)
      catch {
        case NonFatal(e) =>
          close()
          throw e
      }
      updateCrc(checksumOut.getChecksum.getValue == dataChunk.getCrc)
      checksumOut.getChecksum.reset()
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
