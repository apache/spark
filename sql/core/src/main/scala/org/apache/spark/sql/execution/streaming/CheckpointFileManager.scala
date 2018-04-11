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

import java.io.{FileSystem => _, _}
import java.util.{EnumSet, UUID}

import scala.util.control.NonFatal

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.local.{LocalFs, RawLocalFs}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.internal.Logging
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

  /** Create a file. */
  def create(path: Path, overwrite: Boolean): FSDataOutputStream

  /** Create a file and make its contents available atomically after the output stream is closed. */
  def createAtomic(path: Path, overwrite: Boolean): CancellableFSDataOutputStream

  /** Open a file for reading, or throw exception if it does not exist. */
  def open(path: Path): FSDataInputStream

  /** Rename a file. */
  def rename(srcPath: Path, dstPath: Path, overwrite: Boolean): Unit

  /** Recursively delete a path if it exists. Should not throw exception if file doesn't exist. */
  def delete(path: Path): Unit

  /** Copy a local file to a remote file. */
  def copyFromLocalFile(localSrcFile: File, destPath: Path): Unit

  /** Copy a remote file to the local file. */
  def copyToLocalFile(srcPath: Path, localDestFile: File): Unit

  /** Is the default file system this implementation is operating on the local file system. */
  def isLocal: Boolean
}

object CheckpointFileManager extends Logging {

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
      fm: CheckpointFileManager,
      finalPath: Path,
      tempPath: Path,
      overwrite: Boolean)
    extends CancellableFSDataOutputStream(fm.create(tempPath, overwrite)) {

    def this(fm: CheckpointFileManager, path: Path, overwrite: Boolean) = {
      this(fm, path, generateTempPath(path), overwrite)
    }

    logInfo(s"Writing atomically to $finalPath using temp file $tempPath")
    @volatile private var terminated = false

    override def close(): Unit = synchronized {
      try {
        if (terminated) return
        super.close()
        fm.rename(tempPath, finalPath, overwrite)
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
          "Could not use FileContext API for managing metadata log files at path " +
            s"$path. Using FileSystem API instead for managing log files. The log may be " +
            s"inconsistent under failures.")
        new FileSystemBasedCheckpointFileManager(path, hadoopConf)
    }
    new FileSystemBasedCheckpointFileManager(path, hadoopConf)
  }

  private def generateTempPath(path: Path): Path = {
    val tc = org.apache.spark.TaskContext.get
    val tid = if (tc != null) ".TID" + tc.taskAttemptId else ""
    new Path(path.getParent, s".${path.getName}.${UUID.randomUUID}${tid}.tmp")
  }
}


/** An implementation of [[CheckpointFileManager]] using Hadoop's [[FileSystem]] API. */
class FileSystemBasedCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends CheckpointFileManager with Logging {

  import CheckpointFileManager._

  protected val fs = path.getFileSystem(hadoopConf)

  fs.setVerifyChecksum(false)
  fs.setWriteChecksum(false)

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    fs.listStatus(path, filter)
  }

  override def mkdirs(path: Path): Unit = {
    fs.mkdirs(path, FsPermission.getDirDefault)
  }

  override def create(path: Path, overwrite: Boolean): FSDataOutputStream = {
    fs.create(path, overwrite)
  }

  override def createAtomic(path: Path, overwrite: Boolean): CancellableFSDataOutputStream = {
    new RenameBasedFSDataOutputStream(this, path, overwrite)
  }

  override def open(path: Path): FSDataInputStream = {
    fs.open(path)
  }

  override def exists(path: Path): Boolean = {
    fs.exists(path)
  }

  override def rename(srcPath: Path, dstPath: Path, overwrite: Boolean): Unit = {
    if (!overwrite && fs.exists(dstPath)) {
      throw new FileAlreadyExistsException(
        s"Failed to rename $srcPath to $dstPath as destination already exists")
    }

    def deleteAndRename(prevException: Exception): Unit = {
      if (overwrite) {
        try {
          if (fs.delete(dstPath, true)) {
            logWarning(s"Failed to delete $dstPath before second attempt to rename")
          }
          if (!fs.rename(srcPath, dstPath)) {
            val msg = s"Failed to rename temp file $srcPath to $dstPath as second attempt to" +
              s"rename (after delete) returned false"
            logWarning(msg)
            val e = new IOException(msg)
            e.addSuppressed(prevException)
            throw e
          }
        } catch {
          case NonFatal(e) =>
            logError(s"Failed to write atomically to $dstPath", e)
            if (prevException != null) e.addSuppressed(prevException)
            throw e
        }
      } else {
        throw prevException
      }
    }

    try {
      if (!fs.rename(srcPath, dstPath)) {
        val msg = s"Failed to rename temp file $srcPath to $dstPath as rename returned false"
        logWarning(msg)
        deleteAndRename(new IOException(msg))
      }
    } catch {
      case fe: FileAlreadyExistsException =>
        logWarning(s"Failed to rename temp file $srcPath to $dstPath because file exists", fe)
        deleteAndRename(fe)
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

  override def copyFromLocalFile(localSrcFile: File, destPath: Path): Unit = {
    fs.copyFromLocalFile(new Path(localSrcFile.getAbsoluteFile.toURI), destPath)
  }

  override def copyToLocalFile(srcPath: Path, localDestFile: File): Unit = {
    fs.copyToLocalFile(srcPath, new Path(localDestFile.getAbsoluteFile.toURI))
  }

  override def isLocal: Boolean = fs match {
    case _: LocalFileSystem | _: RawLocalFileSystem => true
    case _ => false
  }
}


/** An implementation of [[CheckpointFileManager]] using Hadoop's [[FileContext]] API. */
class FileContextBasedCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends CheckpointFileManager with Logging {

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

  override def create(path: Path, overwrite: Boolean): FSDataOutputStream = {
    import CreateFlag._
    val flags = if (overwrite) EnumSet.of(CREATE, OVERWRITE) else EnumSet.of(CREATE)
    fc.create(path, flags)
  }

  override def createAtomic(path: Path, overwrite: Boolean): CancellableFSDataOutputStream = {
    new RenameBasedFSDataOutputStream(this, path, overwrite)
  }

  override def open(path: Path): FSDataInputStream = {
    fc.open(path)
  }

  override def exists(path: Path): Boolean = {
    fc.util.exists(path)
  }

  override def rename(srcPath: Path, dstPath: Path, overwrite: Boolean): Unit = {
    import Options.Rename._
    fc.rename(srcPath, dstPath, if (overwrite) OVERWRITE else NONE)
  }


  override def delete(path: Path): Unit = {
    try {
      fc.delete(path, true)
    } catch {
      case e: FileNotFoundException =>
      // ignore if file has already been deleted
    }
  }

  override def copyFromLocalFile(localSrcFile: File, destPath: Path): Unit = {
    val localFc = FileContext.getLocalFSFileContext
    var in: InputStream = null
    var out: OutputStream = null
    try {
      in = localFc.open(new Path(localSrcFile.getAbsoluteFile.toURI))
      out = fc.create(destPath, EnumSet.of(CreateFlag.CREATE))
      IOUtils.copyLarge(in, out)
    } finally {
      if (in != null) in.close()
      if (out != null) out.close()
    }
  }

  override def copyToLocalFile(srcPath: Path, localDstFile: File): Unit = {
    val localFc = FileContext.getLocalFSFileContext
    var in: InputStream = null
    var out: OutputStream = null
    try {
      in = fc.open(srcPath)
      out = localFc.create(
        new Path(localDstFile.getAbsoluteFile.toURI), EnumSet.of(CreateFlag.CREATE))
      IOUtils.copyLarge(in, out)
    } finally {
      if (in != null) in.close()
      if (out != null) out.close()
    }
  }

  override def isLocal: Boolean = fc.getDefaultFileSystem match {
    case _: LocalFs | _: RawLocalFs => true // LocalFs = RawLocalFs + ChecksumFs
    case _ => false
  }
}

