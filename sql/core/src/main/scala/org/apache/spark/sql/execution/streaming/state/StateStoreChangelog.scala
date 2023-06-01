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

import java.io.{DataInputStream, DataOutputStream, FileNotFoundException, IOException}

import scala.util.control.NonFatal

import com.google.common.io.ByteStreams
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSError, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream
import org.apache.spark.util.NextIterator

/**
 * Write changes to the key value state store instance to a changelog file.
 * There are 2 types of records, put and delete.
 * A put record is written as: | key length | key content | value length | value content |
 * A delete record is written as: | key length | key content | -1 |
 * Write an Int -1 to signal the end of file.
 * The overall changelog format is: | put record | delete record | ... | put record | -1 |
 */
class StateStoreChangelogWriter(
    fm: CheckpointFileManager,
    file: Path,
    compressionCodec: CompressionCodec) extends Logging {

  private def compressStream(outputStream: DataOutputStream): DataOutputStream = {
    val compressed = compressionCodec.compressedOutputStream(outputStream)
    new DataOutputStream(compressed)
  }

  private var backingFileStream: CancellableFSDataOutputStream =
    fm.createAtomic(file, overwriteIfPossible = true)
  private var compressedStream: DataOutputStream = compressStream(backingFileStream)
  var size = 0

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    assert(compressedStream != null)
    compressedStream.writeInt(key.size)
    compressedStream.write(key)
    compressedStream.writeInt(value.size)
    compressedStream.write(value)
    size += 1
  }

  def delete(key: Array[Byte]): Unit = {
    assert(compressedStream != null)
    compressedStream.writeInt(key.size)
    compressedStream.write(key)
    // -1 in the value field means record deletion.
    compressedStream.writeInt(-1)
    size += 1
  }

  def abort(): Unit = {
    try {
      if (backingFileStream != null) backingFileStream.cancel()
      if (compressedStream != null) IOUtils.closeQuietly(compressedStream)
    } catch {
      // Closing the compressedStream causes the stream to write/flush flush data into the
      // rawStream. Since the rawStream is already closed, there may be errors.
      // Usually its an IOException. However, Hadoop's RawLocalFileSystem wraps
      // IOException into FSError.
      case e: FSError if e.getCause.isInstanceOf[IOException] =>
      case NonFatal(ex) =>
        logInfo(s"Failed to cancel changelog file $file for state store provider " +
          s"with exception=$ex")
    } finally {
      backingFileStream = null
      compressedStream = null
    }
  }

  def commit(): Unit = {
    try {
      // -1 in the key length field mean EOF.
      compressedStream.writeInt(-1)
      compressedStream.close()
    } catch {
      case e: Throwable =>
        abort()
        logError(s"Fail to commit changelog file $file because of exception $e")
        throw e
    } finally {
      backingFileStream = null
      compressedStream = null
    }
  }
}


/**
 * Read an iterator of change record from the changelog file.
 * A record is represented by ByteArrayPair(key: Array[Byte], value: Array[Byte])
 * A put record is returned as a ByteArrayPair(key, value)
 * A delete record is return as a ByteArrayPair(key, null)
 */
class StateStoreChangelogReader(
    fm: CheckpointFileManager,
    fileToRead: Path,
    compressionCodec: CompressionCodec)
  extends NextIterator[(Array[Byte], Array[Byte])] with Logging {

  private def decompressStream(inputStream: DataInputStream): DataInputStream = {
    val compressed = compressionCodec.compressedInputStream(inputStream)
    new DataInputStream(compressed)
  }

  private val sourceStream = try {
    fm.open(fileToRead)
  } catch {
    case f: FileNotFoundException =>
      throw new IllegalStateException(
        s"Error reading streaming state file of $fileToRead does not exist. " +
          "If the stream job is restarted with a new or updated state operation, please" +
          " create a new checkpoint location or clear the existing checkpoint location.", f)
  }
  private val input: DataInputStream = decompressStream(sourceStream)

  def close(): Unit = { if (input != null) input.close() }

  override def getNext(): (Array[Byte], Array[Byte]) = {
    val keySize = input.readInt()
    // A -1 key size mean end of file.
    if (keySize == -1) {
      finished = true
      null
    } else if (keySize < 0) {
      throw new IOException(
        s"Error reading streaming state file $fileToRead: key size cannot be $keySize")
    } else {
      // TODO: reuse the key buffer and value buffer across records.
      val keyBuffer = new Array[Byte](keySize)
      ByteStreams.readFully(input, keyBuffer, 0, keySize)
      val valueSize = input.readInt()
      if (valueSize < 0) {
        // A deletion record
        (keyBuffer, null)
      } else {
        val valueBuffer = new Array[Byte](valueSize)
        ByteStreams.readFully(input, valueBuffer, 0, valueSize)
        // A put record.
        (keyBuffer, valueBuffer)
      }
    }
  }
}
