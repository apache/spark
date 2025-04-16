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
package org.apache.spark.sql.execution.streaming.state

import java.io._
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.{CancellableFSDataOutputStream, RenameBasedFSDataOutputStream}
import org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager

/**
 * A wrapper file output stream that will throw exception in close() and put the underlying
 * stream to FailureInjectionFileSystem.delayedStreams
 * @param stream stream to be wrapped
 */
class DelayCloseFSDataOutputStreamWrapper(
    stream: CancellableFSDataOutputStream,
    injectionState: FailureInjectionState)
  extends CancellableFSDataOutputStream(stream.getWrappedStream) with Logging {
  val originalStream: CancellableFSDataOutputStream = stream

  var closed: Boolean = false

  override def close(): Unit = {
    if (!closed) {
      closed = true
      injectionState.delayedStreams = injectionState.delayedStreams :+ originalStream
      throw new IOException("Fake File Stream Close Failure")
    }
  }

  /** Cancel is not needed in unit tests */
  override def cancel(): Unit = {}
}

/**
 * A wrapper checkpoint file manager that might inject failures in some function calls.
 * Used in unit tests to simulate failure scenarios.
 * This can be put into SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS to provide failure
 * injection behavior.
 * Requirement: when this file manager is created, `path` should already be registered using
 * FailureInjectionFileSystem.registerTempPath(path)
 *
 * @param path The path to the checkpoint directory, passing to the parent class
 * @param hadoopConf  hadoop conf that will be passed to the parent class
 */
class FailureInjectionCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends FileSystemBasedCheckpointFileManager(path, hadoopConf) with Logging {

  // Injection state for the path
  private val injectionState = FailureInjectionFileSystem.getInjectionState(path.toString)

  override def createAtomic(path: Path,
                            overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    injectionState.failureCreateAtomicRegex.foreach { pattern =>
      if (path.toString.matches(pattern)) {
        throw new IOException("Fake File System Create Atomic Failure")
      }
    }

    var shouldDelay = false
    injectionState.createAtomicDelayCloseRegex.foreach { pattern =>
      if (path.toString.matches(pattern)) {
        shouldDelay = true
      }
    }
    val ret = new RenameBasedFSDataOutputStream(this, path, overwriteIfPossible)
    if (shouldDelay) {
      new DelayCloseFSDataOutputStreamWrapper(ret, injectionState)
    } else {
      ret
    }
  }

  override def renameTempFile(srcPath: Path, dstPath: Path, overwriteIfPossible: Boolean): Unit = {
    if (injectionState.allowOverwriteInRename || !fs.exists(dstPath)) {
      super.renameTempFile(srcPath, dstPath, overwriteIfPossible)
    } else {
      logWarning(s"Skip renaming temp file $srcPath to $dstPath because it already exists.")
    }
  }

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    super.list(path, filter)
  }

  override def exists(path: Path): Boolean = {
    if (injectionState.shouldFailExist) {
      throw new IOException("Fake File Exists Failure")
    }
    super.exists(path)
  }
}

/**
 * A class that contains the failure injection state for a path.
 * Variables in this class cannot be updated concurrently by two threads, but can have multiple
 * readers
 */
class FailureInjectionState {
  // File names matching this regex will cause the copyFromLocalFile to fail
  @volatile
  var failPreCopyFromLocalFileNameRegex: Seq[String] = Seq.empty
  // File names matching this regex will cause the close() to fail and put the streams in
  // `delayedStreams`
  @volatile
  var createAtomicDelayCloseRegex: Seq[String] = Seq.empty
  // File names matching this regex will cause the createAtomic() to fail
  @volatile
  var failureCreateAtomicRegex: Seq[String] = Seq.empty
  // If true, Exists() call will fail
  @volatile
  var shouldFailExist: Boolean = false
  // If true, simulate a case where rename() will not overwrite an existing file.
  @volatile
  var allowOverwriteInRename: Boolean = true

  // List of streams that are delayed in close() based on `createAtomicDelayCloseRegex`
  @volatile
  var delayedStreams: Seq[CancellableFSDataOutputStream] = Seq.empty
}

/**
 * Contains a list of variables for failure ingestion conditions.
 * These are singleton instances accessed by all instances of FailureInjectionCheckpointFileManager
 * and FailureInjectionFileSystem. This allows a unit test to have a global control of failure
 * and access to the delayed streams.
 */
object FailureInjectionFileSystem {
  // A map from a temp path to its failure injection state.
  var tempPathToInjectionState: Map[String, FailureInjectionState] = Map.empty

  /**
   * Create a new FailureInjectionState for a temp path and add it to the map.
   * @param path  the temp path
   * @return  the newly created failure injection state
   */
  def registerTempPath(path: String): FailureInjectionState = synchronized {
    // Throw exception if the path already exists in the map
    assert(!tempPathToInjectionState.contains(path), s"Path $path already exists in the map")
    tempPathToInjectionState = tempPathToInjectionState + (path -> new FailureInjectionState)
    tempPathToInjectionState(path)
  }

  /**
   * Clean up a temp path and its failure injection state
   * @param path the temp path to be cle
   */
  def removePathFromTempToInjectionState(path: String): Unit = synchronized {
    // if we can find the injection state of the path, cancel all the delayed streams
    tempPathToInjectionState.get(path).foreach { state =>
      state.delayedStreams.foreach(_.cancel())
    }
    tempPathToInjectionState = tempPathToInjectionState - path
  }

  /**
   * find injection state based on temp dir as prefix
   * @param path a path with temp dir as prefix
   * @return the injection state if the path is in the map. Exception if there is no match for
   *         prefix
   */
  def getInjectionState(path: String): FailureInjectionState = synchronized {
    // remove "file://" prefix from path
    val cleanedPath = path.replace("file:/", "/")
    // return the injection state if the path in the map is prefix of path
    val pathPrefix = tempPathToInjectionState.keys.find(cleanedPath.startsWith)
    assert(pathPrefix.isDefined)
    tempPathToInjectionState.get(pathPrefix.get).get
  }
}

/**
 * A wrapper FileSystem that inject some failures. This class can used to replace the
 * FileSystem in RocksDBFileManager.
 * @param innerFs  the FileSystem to be wrapped
 */
class FailureInjectionFileSystem(innerFs: FileSystem) extends FileSystem {

  override def getConf: Configuration = innerFs.getConf

  override def mkdirs(f: Path, permission: FsPermission): Boolean = innerFs.mkdirs(f, permission)

  override def rename(src: Path, dst: Path): Boolean = innerFs.rename(src, dst)

  override def getUri: URI = innerFs.getUri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = innerFs.open(f, bufferSize)

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    innerFs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    innerFs.append(f, bufferSize, progress)

  override def delete(f: Path, recursive: Boolean): Boolean = innerFs.delete(f, recursive)

  override def listStatus(f: Path): Array[FileStatus] = innerFs.listStatus(f)

  override def setWorkingDirectory(new_dir: Path): Unit = innerFs.setWorkingDirectory(new_dir)

  override def getWorkingDirectory: Path = innerFs.getWorkingDirectory

  override def getFileStatus(f: Path): FileStatus = innerFs.getFileStatus(f)

  override def copyFromLocalFile(src: Path, dst: Path): Unit = {
    // Find injection state based on the destination path
    val injectionState = FailureInjectionFileSystem.getInjectionState(dst.toString)

    injectionState.failPreCopyFromLocalFileNameRegex.foreach { pattern =>
      if (src.toString.matches(pattern)) {
        throw new IOException(s"Injected failure due to source path matching pattern: $pattern")
      }
    }

    innerFs.copyFromLocalFile(src, dst)
  }
}

/**
 * A wrapper RocksDB State Store Provider that replaces FileSystem used in RocksDBFileManager
 * to FailureInjectionFileSystem.
 */
class FailureInjectionRocksDBStateStoreProvider extends RocksDBStateStoreProvider {
  override def createRocksDB(
      dfsRootDir: String,
      conf: RocksDBConf,
      localRootDir: File,
      hadoopConf: Configuration,
      loggingId: String,
      useColumnFamilies: Boolean,
      enableStateStoreCheckpointIds: Boolean,
      partitionId: Int,
      eventForwarder: Option[RocksDBEventForwarder] = None): RocksDB = {
    FailureInjectionRocksDBStateStoreProvider.createRocksDBWithFaultInjection(
      dfsRootDir,
      conf,
      localRootDir,
      hadoopConf,
      loggingId,
      useColumnFamilies,
      enableStateStoreCheckpointIds,
      partitionId,
      eventForwarder)
  }
}

object FailureInjectionRocksDBStateStoreProvider {
  /**
   * RocksDBFieManager is created by RocksDB class where it creates a default FileSystem.
   * We make RocksDB create a RocksDBFileManager that uses a different FileSystem here.
   * */
  def createRocksDBWithFaultInjection(
      dfsRootDir: String,
      conf: RocksDBConf,
      localRootDir: File,
      hadoopConf: Configuration,
      loggingId: String,
      useColumnFamilies: Boolean,
      enableStateStoreCheckpointIds: Boolean,
      partitionId: Int,
      eventForwarder: Option[RocksDBEventForwarder]): RocksDB = {
    new RocksDB(
      dfsRootDir,
      conf = conf,
      localRootDir = localRootDir,
      hadoopConf = hadoopConf,
      loggingId = loggingId,
      useColumnFamilies = useColumnFamilies,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      partitionId = partitionId,
      eventForwarder = eventForwarder
    ) {
      override def createFileManager(
          dfsRootDir: String,
          localTempDir: File,
          hadoopConf: Configuration,
          codecName: String,
          loggingId: String): RocksDBFileManager = {
        new RocksDBFileManager(
          dfsRootDir,
          localTempDir,
          hadoopConf,
          codecName,
          loggingId = loggingId) {
          override def getFileSystem(
              myDfsRootDir: String,
              myHadoopConf: Configuration): FileSystem = {
            new FailureInjectionFileSystem(new Path(myDfsRootDir).getFileSystem(myHadoopConf))
          }
        }
      }
    }
  }
}
