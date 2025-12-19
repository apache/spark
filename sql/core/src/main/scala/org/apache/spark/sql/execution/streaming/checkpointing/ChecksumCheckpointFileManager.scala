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
package org.apache.spark.sql.execution.streaming.checkpointing

import java.io.{FileNotFoundException, InputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.zip.{CheckedInputStream, CheckedOutputStream, CRC32C}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.io.Source

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import org.apache.hadoop.fs._
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CHECKSUM, NUM_BYTES, PATH, TIMEOUT}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager.CancellableFSDataOutputStream
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.Utils

/** Information about the creator of the checksum file. Useful for debugging */
case class ChecksumFileCreatorInfo(
    executorId: String,
    taskInfo: String)

object ChecksumFileCreatorInfo {
  def apply(): ChecksumFileCreatorInfo = {
    val executorId = Option(SparkEnv.get).map(_.executorId).getOrElse("")
    val taskInfo = Option(TaskContext.get()).map(tc =>
      s"Task= ${tc.partitionId()}.${tc.attemptNumber()}, " +
        s"Stage= ${tc.stageId()}.${tc.stageAttemptNumber()}").getOrElse("")
    new ChecksumFileCreatorInfo(executorId, taskInfo)
  }
}

/** This is the content of the checksum file.
 * Holds the checksum value and additional information */
case class Checksum(
    algorithm: String,
    // We can make this a byte array later to be agnostic of algorithm used.
    value: Int,
    mainFileSize: Long,
    timestampMs: Long,
    creator: ChecksumFileCreatorInfo) {

  import Checksum._

  def json(): String = {
    mapper.writeValueAsString(this)
  }
}

object Checksum {
  implicit val format: Formats = Serialization.formats(NoTypeHints)

  /** Used to convert between class and JSON. */
  lazy val mapper = {
    val _mapper = new ObjectMapper with ClassTagExtensions
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }

  def fromJson(json: String): Checksum = {
    Serialization.read[Checksum](json)
  }
}

/** Holds the path of the checksum file and knows the main file path. */
case class ChecksumFile(path: Path) {
  import ChecksumCheckpointFileManager._
  assert(isChecksumFile(path), "path is not a checksum file")

  val mainFilePath = new Path(path.toString.stripSuffix(CHECKSUM_FILE_SUFFIX))

  /** The name of the file without any extensions e.g. my-file.txt.crc returns my-file */
  val baseName: String = path.getName.split("\\.").head
}

/**
 * A [[CheckpointFileManager]] that creates a checksum file for the main file.
 * This wraps another [[CheckpointFileManager]] and adds checksum functionality on top of it.
 * Under the hood, when a file is created, it also creates a checksum file with the same name as
 * the main file but adds a suffix. It returns [[ChecksumCancellableFSDataOutputStream]]
 * which handles the writing of the main file and checksum file.
 *
 * When a file is opened, it returns [[ChecksumFSDataInputStream]], which handles reading
 * the main file and checksum file and does the checksum verification.
 *
 * In order to reduce the impact of reading/writing 2 files instead of 1, it uses a threadpool
 * to read/write both files concurrently.
 *
 * @note
 * It is able to read files written by other [[CheckpointFileManager]], that don't have checksum.
 * It automatically deletes the checksum file when the main file is deleted.
 * If you delete the main file with a different type of manager, then the checksum file will be
 * left behind (i.e. orphan checksum file), since they don't know about it. It would be your
 * responsibility to delete the orphan checksum files.
 *
 * @param underlyingFileMgr The file manager to use under the hood
 * @param allowConcurrentDelete If true, allows deleting the main and checksum file concurrently.
 *                              This is a perf optimization, but can potentially lead to
 *                              orphan checksum files. If using this, it is your responsibility
 *                              to clean up the potential orphan checksum files.
 * @param numThreads This is the number of threads to use for the thread pool, for reading/writing
 *                   files. To avoid blocking, if the file manager instance is being used by a
 *                   single thread, then you can set this to 2 (one thread for main file, another
 *                   for checksum file).
 *                   If file manager is shared by multiple threads, you can set it to
 *                   number of threads using file manager * 2.
 *                   Setting this differently can lead to file operation being blocked waiting for
 *                   a free thread.
 * @param skipCreationIfFileMissingChecksum (ES-1629547): If true, when a file already exists
 *                   but its checksum file does not exist, fall back to using the underlying
 *                   file manager directly instead of creating with checksum. This is useful
 *                   for compatibility with files created before checksums were enabled. Consider
 *                   the case when a batch fails but state files are written. If on the next run,
 *                   we try to upload both a new file and a checksum file, the file could fail to be
 *                   uploaded but the checksum file is uploaded successfully. This would lead to a
 *                   situation where the old file could be loaded and compared with the new file
 *                   checksum, which would fail the checksum verification.
 */
class ChecksumCheckpointFileManager(
    private val underlyingFileMgr: CheckpointFileManager,
    val allowConcurrentDelete: Boolean = false,
    val numThreads: Int,
    val skipCreationIfFileMissingChecksum: Boolean)
  extends CheckpointFileManager with Logging {
  assert(numThreads % 2 == 0, "numThreads must be a multiple of 2, we need 1 for the main file" +
    "and another for the checksum file")

  import ChecksumCheckpointFileManager._

  // This allows us to concurrently read/write the main file and checksum file
  private val threadPool = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonFixedThreadPool(numThreads, s"${this.getClass.getSimpleName}-Thread"))

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    underlyingFileMgr.list(path, filter)
  }

  override def list(path: Path): Array[FileStatus] = {
    underlyingFileMgr.list(path)
  }

  override def mkdirs(path: Path): Unit = {
    underlyingFileMgr.mkdirs(path)
  }

  private def shouldSkipChecksumCreation(path: Path): Boolean = {
    skipCreationIfFileMissingChecksum &&
      underlyingFileMgr.exists(path) && !underlyingFileMgr.exists(getChecksumPath(path))
  }

  override def createAtomic(path: Path,
      overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    if (shouldSkipChecksumCreation(path)) {
      underlyingFileMgr.createAtomic(path, overwriteIfPossible)
    } else {
      createWithChecksum(path, underlyingFileMgr.createAtomic(_, overwriteIfPossible))
    }
  }

  private def createWithChecksum(path: Path,
      createFunc: Path => CancellableFSDataOutputStream): ChecksumCancellableFSDataOutputStream = {
    assert(!isChecksumFile(path), "Cannot directly create a checksum file")

    val mainFileFuture = Future {
      createFunc(path)
    }(threadPool)

    val checksumFileFuture = Future {
      createFunc(getChecksumPath(path))
    }(threadPool)

    new ChecksumCancellableFSDataOutputStream(
      awaitResult(mainFileFuture, Duration.Inf),
      path,
      awaitResult(checksumFileFuture, Duration.Inf),
      threadPool
    )
  }

  override def open(path: Path): FSDataInputStream = {
    assert(!isChecksumFile(path), "Cannot directly open a checksum file")

    val checksumInputStreamFuture = Future {
      try {
        Some(underlyingFileMgr.open(getChecksumPath(path)))
      } catch {
        // In case the client previously had file checksum disabled.
        // Then previously created files won't have checksum.
        case _: FileNotFoundException =>
          logWarning(log"No checksum file found for ${MDC(PATH, path)}, " +
            log"hence no checksum verification.")
          None
      }
    }(threadPool)

    val mainInputStreamFuture = Future {
      underlyingFileMgr.open(path)
    }(threadPool)

    val mainStream = awaitResult(mainInputStreamFuture, Duration.Inf)
    val checksumStream = awaitResult(checksumInputStreamFuture, Duration.Inf)

    checksumStream.map { chkStream =>
      new ChecksumFSDataInputStream(mainStream, path, chkStream, threadPool)
    }.getOrElse(mainStream)
  }

  override def exists(path: Path): Boolean = underlyingFileMgr.exists(path)

  override def delete(path: Path): Unit = {
    // Allowing directly deleting the checksum file for orphan checksum file scenario
    if (isChecksumFile(path)) {
      deleteChecksumFile(path)
    } else if (allowConcurrentDelete) {
      // Ideally, we should first try to delete the checksum file
      // before the main file, to avoid a situation where the main file is deleted but the
      // checksum file deletion failed. The client might not call delete again if the main file
      // no longer exists.
      // But if allowConcurrentDelete is enabled, then we can do it concurrently for perf.
      // But the client would be responsible for cleaning up potential orphan checksum files
      // if it happens.
      val checksumInputStreamFuture = Future {
        deleteChecksumFile(getChecksumPath(path))
      }(threadPool)

      val mainInputStreamFuture = Future {
        underlyingFileMgr.delete(path)
      }(threadPool)

      awaitResult(mainInputStreamFuture, Duration.Inf)
      awaitResult(checksumInputStreamFuture, Duration.Inf)
    } else {
      // First delete the checksum file, then main file
      deleteChecksumFile(getChecksumPath(path))
      underlyingFileMgr.delete(path)
    }
  }

  private def deleteChecksumFile(checksumPath: Path): Unit = {
    // delete call doesn't throw exception if file doesn't exist.
    // In situation where it has already been deleted
    // or the main file was created initially without checksum
    underlyingFileMgr.delete(checksumPath)
    logDebug(log"Deleted checksum file ${MDC(PATH, checksumPath)}")
  }

  override def isLocal: Boolean = underlyingFileMgr.isLocal

  override def createCheckpointDirectory(): Path = {
    underlyingFileMgr.createCheckpointDirectory()
  }

  override def close(): Unit = {
    threadPool.shutdown()
    // Wait a bit for it to finish up in case there is any ongoing work
    // Can consider making this timeout configurable, if needed
    val timeoutMs = 500
    if (!threadPool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
      logWarning(log"Thread pool did not shutdown after ${MDC(TIMEOUT, timeoutMs)} ms," +
        log" forcing shutdown")
      threadPool.shutdownNow() // stop the executing tasks

      // Wait a bit for the threads to respond
      if (!threadPool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
        logError(log"Thread pool did not terminate")
      }
    }
  }
}

private[streaming] object ChecksumCheckpointFileManager {
  val CHECKSUM_FILE_SUFFIX = ".crc"

  def awaitResult[T](future: Future[T], atMost: Duration): T = {
    try {
      ThreadUtils.awaitResult(future, atMost)
    } catch {
      // awaitResult wraps the exception. Unwrap it, and throw the actual error
      case e: SparkException if e.getMessage.contains("Exception thrown in awaitResult") =>
        throw e.getCause
    }
  }

  def getChecksumPath(mainFilePath: Path): Path = {
    new Path(mainFilePath.toString + CHECKSUM_FILE_SUFFIX)
  }

  def isChecksumFile(path: Path): Boolean = {
    path.getName.endsWith(CHECKSUM_FILE_SUFFIX)
  }
}

/** An implementation of [[FSDataInputStream]] that calculates the checksum of the file
 * that the client is reading (main file) incrementally, while it is being read.
 * It then does checksum verification on close, to verify that the computed checksum
 * matches the expected checksum in the checksum file.
 *
 * Computing the checksum incrementally and doing the verification after file read is complete
 * is for better performance, instead of first reading the entire file and doing verification
 * before the client starts reading the file.
 *
 * @param mainStream Input stream for the main file the client wants to read
 * @param path The path of the main file
 * @param expectedChecksumStream The input stream for the checksum file
 * @param threadPool Thread pool to use for concurrently operating on the main and checksum file
 * */
class ChecksumFSDataInputStream(
    private val mainStream: FSDataInputStream,
    path: Path,
    private val expectedChecksumStream: FSDataInputStream,
    private val threadPool: ExecutionContext)
  extends FSDataInputStream(new CheckedSequentialInputStream(mainStream)) with Logging {

  import ChecksumCheckpointFileManager._

  @volatile private var verified = false
  @volatile private var closed = false

  override def close(): Unit = {
    if (!closed) {
      // We verify the checksum only when the client is done reading.
      try {
        verifyChecksum()
      } finally {
        closeInternal()
      }
    }
  }

  /** This is used to skip checksum verification on close.
   * Avoid using this, and it is only used for a situation where the file is opened, read,
   * then closed multiple times, and we want to avoid doing verification each time
   * and only want to do it once.
   * */
  def closeWithoutChecksumVerification(): Unit = {
    if (!closed) {
      // Ideally this should be warning, but if a file is doing this frequently
      // it will cause unnecessary noise in the logs. This can be changed later.
      logDebug(log"Closing file ${MDC(PATH, path)} without checksum verification")
      closeInternal()
    }
  }

  private def closeInternal(): Unit = {
    closed = true

    val mainCloseFuture = Future {
      super.close() // close the main file
    }(threadPool)

    val checksumCloseFuture = Future {
      expectedChecksumStream.close() // close the checksum file
    }(threadPool)

    awaitResult(mainCloseFuture, Duration.Inf)
    awaitResult(checksumCloseFuture, Duration.Inf)
  }

  private def verifyChecksum(): Unit = {
    if (!verified) {
      // It is possible the file was not read till the end by the reader,
      // but we need the entire file content for checksum verification.
      // Hence, we will read the file till the end from where reader stopped.
      var remainingBytesRead = 0
      val buffer = new Array[Byte](1024)
      var bytesRead = 0
      while ({ bytesRead = super.read(buffer); bytesRead != -1 }) {
        remainingBytesRead += bytesRead
      }

      // we are at the end position so that tells us the size
      val computedFileSize = mainStream.getPos

      if (remainingBytesRead > 0) {
        // Making this debug log since most of our files are not read exactly to the end
        // and don't want to cause unnecessary noise in the logs.
        logDebug(log"File ${MDC(PATH, path)} was not read till the end by reader. " +
          log"Finished reading the rest of the file for checksum verification. " +
          log"Remaining bytes read: ${MDC(NUM_BYTES, remainingBytesRead)}, " +
          log"total size: ${MDC(NUM_BYTES, computedFileSize)}.")
      }

      // Read the expected checksum from the checksum file
      val expectedChecksumJson = Source.fromInputStream(
        expectedChecksumStream, StandardCharsets.UTF_8.name()).mkString
      val expectedChecksum = Checksum.fromJson(expectedChecksumJson)
      // Get what we computed while the main file was being read locally
      // `in` is the CheckedSequentialInputStream we created
      val computedChecksumValue = in.asInstanceOf[CheckedInputStream].getChecksum.getValue.toInt

      logInfo(log"Verifying checksum for file ${MDC(PATH, path)}, " +
        log"remainingBytesRead= ${MDC(NUM_BYTES, remainingBytesRead)}. " +
        log"Computed(checksum= ${MDC(CHECKSUM, computedChecksumValue)}, " +
        log"fileSize= ${MDC(NUM_BYTES, computedFileSize)})." +
        log"Checksum file content: ${MDC(CHECKSUM, expectedChecksumJson)}")

      verified = true

      // Compare file size too, in case of collision
      if (expectedChecksum.value != computedChecksumValue ||
        expectedChecksum.mainFileSize != computedFileSize) {
        throw QueryExecutionErrors.checkpointFileChecksumVerificationFailed(
          path,
          expectedSize = expectedChecksum.mainFileSize,
          expectedChecksum.value,
          computedSize = computedFileSize,
          computedChecksumValue)
      }
    }
  }
}

/** This implements [[CheckedInputStream]] that allows us to compute the checksum as the file
 * is being read. [[FSDataInputStream]] needs the passed in input stream to implement Seekable
 * and PositionedReadable. We want the file to be read sequentially only to allow us to correctly
 * compute the checksum. This is blocking the seekable apis from being used in the underlying stream
 * */
private class CheckedSequentialInputStream(data: InputStream)
  // TODO(SPARK-52009): Make the checksum algo configurable
  extends CheckedInputStream(data, new CRC32C())
    with Seekable
    with PositionedReadable {

  // Seekable methods
  override def seek(pos: Long): Unit = {
    throw new UnsupportedOperationException("Seek not supported")
  }

  override def getPos: Long = {
    throw new UnsupportedOperationException("getPos not supported")
  }

  override def seekToNewSource(targetPos: Long): Boolean = {
    throw new UnsupportedOperationException("seekToNewSource not supported")
  }

  // PositionedReadable methods
  def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
    throw new UnsupportedOperationException("read from position not supported")
  }

  def readFully(position: Long, buffer: Array[Byte], offset: Int, length: Int): Unit = {
    throw new UnsupportedOperationException("readFully from position not supported")
  }

  def readFully(position: Long, buffer: Array[Byte]): Unit = {
    throw new UnsupportedOperationException("readFully from position not supported")
  }
}

/** An implementation of [[CancellableFSDataOutputStream]] that calculates the checksum of the file
 * that the client is writing (main file) incrementally, while it is being written.
 * It then writes the main file and an additional checksum file, which will be used for verification
 * by [[ChecksumFSDataInputStream]] on file read.
 *
 * @param mainStream Output stream for the main file the client wants to write to
 * @param path The path of the main file
 * @param checksumStream Output stream for the checksum file to write the computed checksum
 * @param uploadThreadPool Thread pool used to concurrently upload the main and checksum file
 * */
class ChecksumCancellableFSDataOutputStream(
    private val mainStream: CancellableFSDataOutputStream,
    path: Path,
    private val checksumStream: CancellableFSDataOutputStream,
    private val uploadThreadPool: ExecutionContext)
  // TODO(SPARK-52009): make the checksum algo configurable
  // CheckedOutputStream creates the checksum value as we write to the stream
  extends CancellableFSDataOutputStream(new CheckedOutputStream(mainStream, new CRC32C()))
    with Logging {

  import ChecksumCheckpointFileManager._

  @volatile private var closed = false

  override def cancel(): Unit = {
    // Cancel both streams synchronously rather than using futures. If the current thread is
    // interrupted and we call this method, scheduling work on futures would immediately throw
    // InterruptedException leaving the streams in an inconsistent state.
    Utils.tryWithSafeFinally {
      mainStream.cancel()
    } {
      checksumStream.cancel()
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true

      // Create this within the caller thread
      val creator = ChecksumFileCreatorInfo()
      val chkValue = underlyingStream.asInstanceOf[CheckedOutputStream].getChecksum.getValue.toInt
      val mainFileSize = mainStream.getPos

      val mainFuture = Future {
        super.close() // close the main file
      }(uploadThreadPool)

      val checksumFuture = Future {
        val json = Checksum(
          algorithm = "CRC32C",
          value = chkValue,
          mainFileSize = mainFileSize,
          timestampMs = System.currentTimeMillis(),
          creator = creator).json()

        logInfo(log"Created checksum for file ${MDC(PATH, path)}: ${MDC(CHECKSUM, json)}")
        checksumStream.write(json.getBytes(StandardCharsets.UTF_8))
        checksumStream.close()
      }(uploadThreadPool)

      awaitResult(mainFuture, Duration.Inf)
      awaitResult(checksumFuture, Duration.Inf)
    }
  }
}
