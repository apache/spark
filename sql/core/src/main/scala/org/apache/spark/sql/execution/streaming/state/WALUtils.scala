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
import java.util.Locale

import com.google.common.io.ByteStreams
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, FSError, Path}
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.io.LZ4CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream
import org.apache.spark.sql.types.StructType

object WALUtils {

  case class StoreFile(version: Long, path: Path, isSnapshot: Boolean)
  val EOF_MARKER = -1

  /** Files needed to recover the given version of the store */
  def filesForVersion(allFiles: Seq[StoreFile], version: Long): Seq[StoreFile] = {
    require(version >= 0)
    require(allFiles.exists(_.version == version))

    val latestSnapshotFileBeforeVersion = allFiles
      .filter(_.isSnapshot == true)
      .takeWhile(_.version <= version)
      .lastOption
    val deltaBatchFiles = latestSnapshotFileBeforeVersion match {
      case Some(snapshotFile) =>
        val deltaFiles = allFiles.filter { file =>
          file.version > snapshotFile.version && file.version <= version
        }.toList
        verify(
          deltaFiles.size == version - snapshotFile.version,
          s"Unexpected list of delta files for version $version for $this: $deltaFiles")
        deltaFiles

      case None =>
        allFiles.takeWhile(_.version <= version)
    }
    latestSnapshotFileBeforeVersion.toSeq ++ deltaBatchFiles
  }

  /** Fetch all the files that back the store */
  def fetchFiles(fm: CheckpointFileManager, baseDir: Path): Seq[StoreFile] = {
    val files: Seq[FileStatus] = try {
      fm.list(baseDir)
    } catch {
      case _: java.io.FileNotFoundException =>
        Seq.empty
    }
    val versionToFiles = new mutable.HashMap[Long, StoreFile]
    files.foreach { status =>
      val path = status.getPath
      val nameParts = path.getName.split("\\.")
      if (nameParts.size == 2) {
        val version = nameParts(0).toLong
        nameParts(1).toLowerCase(Locale.ROOT) match {
          case "delta" =>
            // ignore the file otherwise, snapshot file already exists for that batch id
            if (!versionToFiles.contains(version)) {
              versionToFiles.put(version, StoreFile(version, path, isSnapshot = false))
            }
          case "snapshot" =>
            versionToFiles.put(version, StoreFile(version, path, isSnapshot = true))
          case _ =>
          // logWarning(s"Could not identify file $path for $this")
        }
      }
    }
    val storeFiles = versionToFiles.values.toSeq.sortBy(_.version)
    storeFiles
  }

  def compressStream(outputStream: DataOutputStream, sparkConf: SparkConf): DataOutputStream = {
    val compressed = new LZ4CompressionCodec(sparkConf).compressedOutputStream(outputStream)
    new DataOutputStream(compressed)
  }

  def decompressStream(inputStream: DataInputStream, sparkConf: SparkConf): DataInputStream = {
    val compressed = new LZ4CompressionCodec(sparkConf).compressedInputStream(inputStream)
    new DataInputStream(compressed)
  }

  def writeUpdateToDeltaFile(output: DataOutputStream, key: UnsafeRow, value: UnsafeRow): Unit = {
    val keyBytes = key.getBytes()
    val valueBytes = value.getBytes()
    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(valueBytes.size)
    output.write(valueBytes)
  }

  def writeRemoveToDeltaFile(output: DataOutputStream, key: UnsafeRow): Unit = {
    val keyBytes = key.getBytes()
    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(EOF_MARKER)
  }

  def finalizeDeltaFile(output: DataOutputStream): Unit = {
    output.writeInt(EOF_MARKER) // Write this magic number to signify end of file
    output.close()
  }

  def updateFromDeltaFile(
      fm: CheckpointFileManager,
      fileToRead: Path,
      keySchema: StructType,
      valueSchema: StructType,
      newRocksDb: OptimisticTransactionDbInstance,
      sparkConf: SparkConf): Unit = {
    var input: DataInputStream = null
    val sourceStream = try {
      fm.open(fileToRead)
    } catch {
      case f: FileNotFoundException =>
        throw new IllegalStateException(
          s"Error reading delta file $fileToRead of $this: $fileToRead does not exist",
          f)
    }
    try {
      input = decompressStream(sourceStream, sparkConf)
      var eof = false

      while (!eof) {
        val keySize = input.readInt()
        if (keySize == EOF_MARKER) {
          eof = true
        } else if (keySize < 0) {
          newRocksDb.abort
          newRocksDb.close()
          throw new IOException(
            s"Error reading delta file $fileToRead of $this: key size cannot be $keySize")
        } else {
          val keyRowBuffer = new Array[Byte](keySize)
          ByteStreams.readFully(input, keyRowBuffer, 0, keySize)

          val keyRow = new UnsafeRow(keySchema.fields.length)
          keyRow.pointTo(keyRowBuffer, keySize)

          val valueSize = input.readInt()
          if (valueSize < 0) {
            newRocksDb.remove(key = keyRow)
          } else {
            val valueRowBuffer = new Array[Byte](valueSize)
            ByteStreams.readFully(input, valueRowBuffer, 0, valueSize)
            val valueRow = new UnsafeRow(valueSchema.fields.length)
            // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
            // This is a workaround for the following:
            // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
            // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
            valueRow.pointTo(valueRowBuffer, (valueSize / 8) * 8)
            newRocksDb.put(keyRow, valueRow)
          }
        }
      }
    } finally {
      if (input != null) input.close()
    }
  }

  /*
   * Try to cancel the underlying stream and safely close the compressed stream.
   *
   * @param compressedStream the compressed stream.
   * @param rawStream the underlying stream which needs to be cancelled.
   */
  def cancelDeltaFile(
      compressedStream: DataOutputStream,
      rawStream: CancellableFSDataOutputStream): Unit = {
    try {
      if (rawStream != null) rawStream.cancel()
      IOUtils.closeQuietly(compressedStream)
    } catch {
      case e: FSError if e.getCause.isInstanceOf[IOException] =>
      // Closing the compressedStream causes the stream to write/flush flush data into the
      // rawStream. Since the rawStream is already closed, there may be errors.
      // Usually its an IOException. However, Hadoop's RawLocalFileSystem wraps
      // IOException into FSError.
    }
  }

  def uploadFile(
      fm: CheckpointFileManager,
      sourceFile: Path,
      targetFile: Path,
      sparkConf: SparkConf): Unit = {
    var output: CancellableFSDataOutputStream = null
    var in: BufferedInputStream = null
    try {
      in = new BufferedInputStream(new FileInputStream(sourceFile.toString))
      output = fm.createAtomic(targetFile, overwriteIfPossible = true)
      val buffer = new Array[Byte](1024)
      var len = in.read(buffer)
      while (len > 0) {
        output.write(buffer, 0, len)
        len = in.read(buffer)
      }
      output.close()
    } catch {
      case e: Throwable =>
        if (output != null) output.cancel()
        throw e
    } finally {
      if (in != null) {
        in.close()
      }
    }
  }

  def downloadFile(
      fm: CheckpointFileManager,
      sourceFile: Path,
      targetFile: Path,
      sparkConf: SparkConf): Boolean = {
    var in: FSDataInputStream = null
    var output: BufferedOutputStream = null
    try {
      in = fm.open(sourceFile)
      output = new BufferedOutputStream(new FileOutputStream(targetFile.toString))
      val buffer = new Array[Byte](1024)
      var eof = false
      while (!eof) {
        val len = in.read(buffer)
        if (len > 0) {
          output.write(buffer, 0, len)
        } else {
          eof = true
        }
      }
      output.close()
    } catch {
      case e: Throwable =>
        new File(targetFile.toString).delete()
        throw e
    } finally {
      output.close()
      if (in != null) {
        in.close()
      }
    }
    return true
  }

  def deltaFile(baseDir: Path, version: Long): Path = {
    new Path(baseDir, s"$version.delta")
  }

  def snapshotFile(baseDir: Path, version: Long): Path = {
    new Path(baseDir, s"$version.snapshot")
  }

  def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(msg)
    }
  }

}
