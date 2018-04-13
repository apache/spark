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
package org.apache.spark.sql.execution.streaming

import java.io.{FileNotFoundException, IOException, OutputStream}
import java.util.{EnumSet, UUID}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.local.{LocalFs, RawLocalFs}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.RenameHelperMethods
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * An interface to abstract out all operation related to streaming checkpoints. Most importantly,
 * the key operation this interface provides is `createAtomic(path, overwrite)` which returns a
 * `CancellableFSDataOutputStream`. This method is used by [[HDFSMetadataLog]] and
 * [[org.apache.spark.sql.execution.streaming.state.StateStore StateStore]] implementations
 * to write a complete checkpoint file atomically (i.e. no partial file will be visible), with or
 * without overwrite.
 *
 * This higher-level interface above the Hadoop FileSystem is necessary because
 * different implementation of FileSystem/FileContext may have different combination of operations
 * to provide the desired atomic guarantees (e.g. write-to-temp-file-and-rename,
 * direct-write-and-cancel-on-failure) and this abstraction allow different implementations while
 * keeping the usage simple (`createAtomic` -> `close` or `cancel`).
 */
trait CheckpointFileManager {

  import org.apache.spark.sql.execution.streaming.CheckpointFileManager._

  /**
   * Create a file and make its contents available atomically after the output stream is closed.
   *
   * @param path                Path to create
   * @param overwriteIfPossible If true, then the implementations must do a best-effort attempt to
   *                            overwrite the file if it already exists. It should not throw
   *                            any exception if the file exists. However, if false, then the
   *                            implementation must not overwrite if the file alraedy exists and
   *                            must throw `FileAlreadyExistsException` in that case.
   */
  def createAtomic(path: Path, overwriteIfPossible: Boolean): CancellableFSDataOutputStream

  /** Open a file for reading, or throw exception if it does not exist. */
  def open(path: Path): FSDataInputStream

  /** List the files in a path that match a filter. */
  def list(path: Path, filter: PathFilter): Array[FileStatus]

  /** List all the files in a path. */
  def list(path: Path): Array[FileStatus] = {
    list(path, new PathFilter { override def accept(path: Path): Boolean = true })
  }

  /** Make directory at the give path and all its parent directories as needed. */
  def mkdirs(path: Path): Unit

  /** Whether path exists */
  def exists(path: Path): Boolean

  /** Recursively delete a path if it exists. Should not throw exception if file doesn't exist. */
  def delete(path: Path): Unit

  /** Is the default file system this implementation is operating on the local file system. */
  def isLocal: Boolean
}

object CheckpointFileManager extends Logging {

  /**
   * Additional methods in CheckpointFileManager implementations that allows
   * [[RenameBasedFSDataOutputStream]] get atomicity by write-to-temp-file-and-rename
   */
  sealed trait RenameHelperMethods { self => CheckpointFileManager
    /** Create a file with overwrite. */
    def createTempFile(path: Path): FSDataOutputStream

    /**
     * Rename a file.
     *
     * @param srcPath             Source path to rename
     * @param dstPath             Destination path to rename to
     * @param overwriteIfPossible If true, then the implementations must do a best-effort attempt to
     *                            overwrite the file if it already exists. It should not throw
     *                            any exception if the file exists. However, if false, then the
     *                            implementation must not overwrite if the file alraedy exists and
     *                            must throw `FileAlreadyExistsException` in that case.
     */
    def renameTempFile(srcPath: Path, dstPath: Path, overwriteIfPossible: Boolean): Unit
  }

  /**
   * An interface to add the cancel() operation to [[FSDataOutputStream]]. This is used
   * mainly by `CheckpointFileManager.createAtomic` to write a file atomically.
   *
   * @see [[CheckpointFileManager]].
   */
  abstract class CancellableFSDataOutputStream(protected val underlyingStream: OutputStream)
    extends FSDataOutputStream(underlyingStream, null) {
    /** Cancel the `underlyingStream` and ensure that the output file is not generated. */
    def cancel(): Unit
  }

  /**
   * An implementation of [[CancellableFSDataOutputStream]] that writes a file atomically by writing
   * to a temporary file and then renames.
   */
  sealed class RenameBasedFSDataOutputStream(
      fm: CheckpointFileManager with RenameHelperMethods,
      finalPath: Path,
      tempPath: Path,
      overwriteIfPossible: Boolean)
    extends CancellableFSDataOutputStream(fm.createTempFile(tempPath)) {

    def this(fm: CheckpointFileManager with RenameHelperMethods, path: Path, overwrite: Boolean) = {
      this(fm, path, generateTempPath(path), overwrite)
    }

    logInfo(s"Writing atomically to $finalPath using temp file $tempPath")
    @volatile private var terminated = false

    override def close(): Unit = synchronized {
      try {
        if (terminated) return
        underlyingStream.close()
        try {
          fm.renameTempFile(tempPath, finalPath, overwriteIfPossible)
        } catch {
          case fe: FileAlreadyExistsException =>
            logWarning(
              s"Failed to rename temp file $tempPath to $finalPath because file exists", fe)
            if (!overwriteIfPossible) throw fe
        }
        logInfo(s"Renamed temp file $tempPath to $finalPath")
      } finally {
        terminated = true
      }
    }

    override def cancel(): Unit = synchronized {
      try {
        if (terminated) return
        underlyingStream.close()
        fm.delete(tempPath)
      } catch {
        case NonFatal(e) =>
          logWarning(s"Error cancelling write to $finalPath", e)
      } finally {
        terminated = true
      }
    }
  }


  /** Create an instance of [[CheckpointFileManager]] based on the path and configuration. */
  def create(path: Path, hadoopConf: Configuration): CheckpointFileManager = {
    val fileManagerClass = hadoopConf.get(
      SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key)
    if (fileManagerClass != null) {
      return Utils.classForName(fileManagerClass)
        .getConstructor(classOf[Path], classOf[Configuration])
        .newInstance(path, hadoopConf)
        .asInstanceOf[CheckpointFileManager]
    }
    try {
      // Try to create a manager based on `FileContext` because HDFS's `FileContext.rename()
      // gives atomic renames, which is what we rely on for the default implementation
      // `CheckpointFileManager.createAtomic`.
      new FileContextBasedCheckpointFileManager(path, hadoopConf)
    } catch {
      case e: UnsupportedFileSystemException =>
        logWarning(
          "Could not use FileContext API for managing Structured Streaming checkpoint files at " +
            s"$path. Using FileSystem API instead for managing log files. If the implementation " +
            s"of FileSystem.rename() is not atomic, then the correctness and fault-tolerance of" +
            s"your Structured Streaming is not guaranteed.")
        new FileSystemBasedCheckpointFileManager(path, hadoopConf)
    }
  }

  private def generateTempPath(path: Path): Path = {
    val tc = org.apache.spark.TaskContext.get
    val tid = if (tc != null) ".TID" + tc.taskAttemptId else ""
    new Path(path.getParent, s".${path.getName}.${UUID.randomUUID}${tid}.tmp")
  }
}


/** An implementation of [[CheckpointFileManager]] using Hadoop's [[FileSystem]] API. */
class FileSystemBasedCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends CheckpointFileManager with RenameHelperMethods with Logging {

  import CheckpointFileManager._

  protected val fs = path.getFileSystem(hadoopConf)

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    fs.listStatus(path, filter)
  }

  override def mkdirs(path: Path): Unit = {
    fs.mkdirs(path, FsPermission.getDirDefault)
  }

  override def createTempFile(path: Path): FSDataOutputStream = {
    fs.create(path, true)
  }

  override def createAtomic(
      path: Path,
      overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    new RenameBasedFSDataOutputStream(this, path, overwriteIfPossible)
  }

  override def open(path: Path): FSDataInputStream = {
    fs.open(path)
  }

  override def exists(path: Path): Boolean = {
    try
      return fs.getFileStatus(path) != null
    catch {
      case e: FileNotFoundException =>
        return false
    }
  }

  override def renameTempFile(srcPath: Path, dstPath: Path, overwriteIfPossible: Boolean): Unit = {
    if (!overwriteIfPossible && fs.exists(dstPath)) {
      throw new FileAlreadyExistsException(
        s"Failed to rename $srcPath to $dstPath as destination already exists")
    }

    if (!fs.rename(srcPath, dstPath)) {
      // FileSystem.rename() returning false is very ambiguous as it can be for many reasons.
      // This tries to make a best effort attempt to return the most appropriate exception.
      if (fs.exists(dstPath)) {
        if (!overwriteIfPossible) {
          throw new FileAlreadyExistsException(s"Failed to rename as $dstPath already exists")
        }
      } else if (!fs.exists(srcPath)) {
        throw new FileNotFoundException(s"Failed to rename as $srcPath was not found")
      } else {
        val msg = s"Failed to rename temp file $srcPath to $dstPath as rename returned false"
        logWarning(msg)
        throw new IOException(msg)
      }
    }
  }

  override def delete(path: Path): Unit = {
    try {
      fs.delete(path, true)
    } catch {
      case e: FileNotFoundException =>
        logInfo(s"Failed to delete $path as it does not exist")
        // ignore if file has already been deleted
    }
  }

  override def isLocal: Boolean = fs match {
    case _: LocalFileSystem | _: RawLocalFileSystem => true
    case _ => false
  }
}


/** An implementation of [[CheckpointFileManager]] using Hadoop's [[FileContext]] API. */
class FileContextBasedCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends CheckpointFileManager with RenameHelperMethods with Logging {

  import CheckpointFileManager._

  private val fc = if (path.toUri.getScheme == null) {
    FileContext.getFileContext(hadoopConf)
  } else {
    FileContext.getFileContext(path.toUri, hadoopConf)
  }

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    fc.util.listStatus(path, filter)
  }

  override def mkdirs(path: Path): Unit = {
    fc.mkdir(path, FsPermission.getDirDefault, true)
  }

  override def createTempFile(path: Path): FSDataOutputStream = {
    import CreateFlag._
    import Options._
    fc.create(
      path, EnumSet.of(CREATE, OVERWRITE), CreateOpts.checksumParam(ChecksumOpt.createDisabled()))
  }

  override def createAtomic(
      path: Path,
      overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    new RenameBasedFSDataOutputStream(this, path, overwriteIfPossible)
  }

  override def open(path: Path): FSDataInputStream = {
    fc.open(path)
  }

  override def exists(path: Path): Boolean = {
    fc.util.exists(path)
  }

  override def renameTempFile(srcPath: Path, dstPath: Path, overwriteIfPossible: Boolean): Unit = {
    import Options.Rename._
    fc.rename(srcPath, dstPath, if (overwriteIfPossible) OVERWRITE else NONE)
  }


  override def delete(path: Path): Unit = {
    try {
      fc.delete(path, true)
    } catch {
      case e: FileNotFoundException =>
      // ignore if file has already been deleted
    }
  }

  override def isLocal: Boolean = fc.getDefaultFileSystem match {
    case _: LocalFs | _: RawLocalFs => true // LocalFs = RawLocalFs + ChecksumFs
    case _ => false
  }
}

