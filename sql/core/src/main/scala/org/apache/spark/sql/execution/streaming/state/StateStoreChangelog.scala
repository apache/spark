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
import org.json4s._
import org.json4s.jackson.Serialization

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream
import org.apache.spark.sql.execution.streaming.state.RecordType.RecordType
import org.apache.spark.util.NextIterator

/**
 * Enum used to write record types to changelog files used with RocksDBStateStoreProvider.
 */
object RecordType extends Enumeration {
  type RecordType = Value

  val EOF_RECORD = Value("eof_record")
  val PUT_RECORD = Value("put_record")
  val DELETE_RECORD = Value("delete_record")
  val MERGE_RECORD = Value("merge_record")

  // Generate byte representation of each record type
  def getRecordTypeAsByte(recordType: RecordType): Byte = {
    recordType match {
      case EOF_RECORD => 0x00.toByte
      case PUT_RECORD => 0x01.toByte
      case DELETE_RECORD => 0x10.toByte
      case MERGE_RECORD => 0x11.toByte
    }
  }

  def getRecordTypeAsString(recordType: RecordType): String = {
    recordType match {
      case PUT_RECORD => "update"
      case DELETE_RECORD => "delete"
      case MERGE_RECORD => "append"
      case _ => throw StateStoreErrors.unsupportedOperationException(
        "getRecordTypeAsString", recordType.toString)
    }
  }

  // Generate record type from byte representation
  def getRecordTypeFromByte(byte: Byte): RecordType = {
    byte match {
      case 0x00 => EOF_RECORD
      case 0x01 => PUT_RECORD
      case 0x10 => DELETE_RECORD
      case 0x11 => MERGE_RECORD
      case _ => throw new RuntimeException(s"Found invalid record type for value=$byte")
    }
  }
}

/**
 * Class for lineage item for checkpoint format V2.
 */
case class LineageItem(
    version: Long,
    checkpointUniqueId: String
)

/**
 * Base class for state store changelog writer
 * @param fm - checkpoint file manager used to manage streaming query checkpoint
 * @param file - name of file to use to write changelog
 * @param compressionCodec - compression method using for writing changelog file
 */
abstract class StateStoreChangelogWriter(
    fm: CheckpointFileManager,
    file: Path,
    compressionCodec: CompressionCodec) extends Logging {

  implicit val formats: Formats = DefaultFormats

  private def compressStream(outputStream: DataOutputStream): DataOutputStream = {
    val compressed = compressionCodec.compressedOutputStream(outputStream)
    new DataOutputStream(compressed)
  }

  protected var backingFileStream: CancellableFSDataOutputStream =
    fm.createAtomic(file, overwriteIfPossible = true)
  protected var compressedStream: DataOutputStream = compressStream(backingFileStream)

  protected def writeVersion(): Unit = {
    compressedStream.writeUTF(s"v${version}")
  }

  protected def writeLineage(stateStoreCheckpointIdLineage: Array[LineageItem]): Unit = {
    assert(version >= 3,
      "writeLineage should only be invoked with state store checkpoint id enabled (version >= 3)")
    val lineageStr = Serialization.write(stateStoreCheckpointIdLineage)
    compressedStream.writeUTF(lineageStr)
  }

  def version: Short

  def put(key: Array[Byte], value: Array[Byte]): Unit

  def delete(key: Array[Byte]): Unit

  def merge(key: Array[Byte], value: Array[Byte]): Unit

  def abort(): Unit = {
    try {
      if (backingFileStream != null) backingFileStream.cancel()
      if (compressedStream != null) IOUtils.closeQuietly(compressedStream)
    } catch {
      // Closing the compressedStream causes the stream to write/flush data into the
      // rawStream. Since the rawStream is already closed, there may be errors.
      // Usually it's an IOException. However, Hadoop's RawLocalFileSystem wraps
      // IOException into FSError.
      case e: FSError if e.getCause.isInstanceOf[IOException] =>
      case NonFatal(ex) =>
        logInfo(log"Failed to cancel changelog file ${MDC(FILE_NAME, file)} " +
          log"for state store provider " +
          log"with exception=${MDC(ERROR, ex)}")
    } finally {
      backingFileStream = null
      compressedStream = null
    }
  }

  def commit(): Unit
}

/**
 * Write changes to the key value state store instance to a changelog file.
 * There are 2 types of records, put and delete.
 * A put record is written as: | key length | key content | value length | value content |
 * A delete record is written as: | key length | key content | -1 |
 * Write an Int -1 to signal the end of file.
 * The overall changelog format is: | put record | delete record | ... | put record | -1 |
 */
class StateStoreChangelogWriterV1(
    fm: CheckpointFileManager,
    file: Path,
    compressionCodec: CompressionCodec)
  extends StateStoreChangelogWriter(fm, file, compressionCodec) {

  // Note that v1 does not record this value in the changelog file
  override def version: Short = 1

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    assert(compressedStream != null)
    compressedStream.writeInt(key.length)
    compressedStream.write(key)
    compressedStream.writeInt(value.length)
    compressedStream.write(value)
  }

  override def delete(key: Array[Byte]): Unit = {
    assert(compressedStream != null)
    compressedStream.writeInt(key.length)
    compressedStream.write(key)
    // -1 in the value field means record deletion.
    compressedStream.writeInt(-1)
  }

  override def merge(key: Array[Byte], value: Array[Byte]): Unit = {
    throw new UnsupportedOperationException("Operation not supported with state " +
      "changelog writer v1")
  }

  override def commit(): Unit = {
    try {
      // -1 in the key length field mean EOF.
      compressedStream.writeInt(-1)
      compressedStream.close()
    } catch {
      case e: Throwable =>
        abort()
        logError(log"Fail to commit changelog file ${MDC(PATH, file)} because of exception", e)
        throw e
    } finally {
      backingFileStream = null
      compressedStream = null
    }
  }
}

/**
 * Write changes to the key value state store instance to a changelog file.
 * There are 3 types of data records, put, merge and delete.
 * A put record or merge record is written as: | record type | key length
 *    | key content | value length | value content | -1 |
 * A delete record is written as: | record type | key length | key content | -1
 * Write an EOF_RECORD to signal the end of file.
 * The overall changelog format is:  version | put record | delete record
 *                                   | ... | put record | eof record |
 */
class StateStoreChangelogWriterV2(
    fm: CheckpointFileManager,
    file: Path,
    compressionCodec: CompressionCodec)
  extends StateStoreChangelogWriter(fm, file, compressionCodec) {

  override def version: Short = 2

  // append the version field to the changelog file
  writeVersion()

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    writePutOrMergeRecord(key, value, RecordType.PUT_RECORD)
  }

  override def delete(key: Array[Byte]): Unit = {
    assert(compressedStream != null)
    compressedStream.write(RecordType.getRecordTypeAsByte(RecordType.DELETE_RECORD))
    compressedStream.writeInt(key.length)
    compressedStream.write(key)
    // -1 in the value field means record deletion.
    compressedStream.writeInt(-1)
  }

  override def merge(key: Array[Byte], value: Array[Byte]): Unit = {
    writePutOrMergeRecord(key, value, RecordType.MERGE_RECORD)
  }

  private def writePutOrMergeRecord(key: Array[Byte],
      value: Array[Byte],
      recordType: RecordType): Unit = {
    assert(recordType == RecordType.PUT_RECORD || recordType == RecordType.MERGE_RECORD)
    assert(compressedStream != null)
    compressedStream.write(RecordType.getRecordTypeAsByte(recordType))
    compressedStream.writeInt(key.length)
    compressedStream.write(key)
    compressedStream.writeInt(value.length)
    compressedStream.write(value)
  }

  def commit(): Unit = {
    try {
      // write EOF_RECORD to signal end of file
      compressedStream.write(RecordType.getRecordTypeAsByte(RecordType.EOF_RECORD))
      compressedStream.close()
    } catch {
      case e: Throwable =>
        abort()
        logError(log"Fail to commit changelog file ${MDC(PATH, file)} because of exception", e)
        throw e
    } finally {
      backingFileStream = null
      compressedStream = null
    }
  }
}

/**
 * Write changes to the key value state store instance to a changelog file.
 * There are 2 types of records, put and delete.
 * A put record is written as: | key length | key content | value length | value content |
 * A delete record is written as: | key length | key content | -1 |
 * Write an Int -1 to signal the end of file.
 * The overall changelog format is: | put record | delete record | ... | put record | -1 |
 * V3 is a extension of V1 for writing changelogs with version
 * in the first line and lineage in the second line.
 */
class StateStoreChangelogWriterV3(
     fm: CheckpointFileManager,
     file: Path,
     compressionCodec: CompressionCodec,
     stateStoreCheckpointIdLineage: Array[LineageItem])
  extends StateStoreChangelogWriterV1(fm, file, compressionCodec) {

  override def version: Short = 3

  // append the version field to the changelog file
  writeVersion()

  // Also write lineage information to the changelog, it should appear
  // in the second line for v3 because the first line is the version
  writeLineage(stateStoreCheckpointIdLineage)
}

/**
 * Write changes to the key value state store instance to a changelog file.
 * There are 3 types of data records, put, merge and delete.
 * A put record or merge record is written as: | record type | key length
 *    | key content | value length | value content | -1 |
 * A delete record is written as: | record type | key length | key content | -1
 * Write an EOF_RECORD to signal the end of file.
 * The overall changelog format is:  version | put record | delete record
 *                                   | ... | put record | eof record |
 * V4 is a extension of V2 for writing changelogs with version
 * in the first line and lineage in the second line.
 */
class StateStoreChangelogWriterV4(
     fm: CheckpointFileManager,
     file: Path,
     compressionCodec: CompressionCodec,
     stateStoreCheckpointIdLineage: Array[LineageItem])
  extends StateStoreChangelogWriterV2(fm, file, compressionCodec) {

  override def version: Short = 4

  // Also write lineage information to the changelog, it should appear
  // in the second line for v4 because the first line is the version
  writeLineage(stateStoreCheckpointIdLineage)
}

/**
 * A factory class for constructing state store readers by reading the first line
 * of the change log file, which stores the version.
 * Note that for changelog version 1, there is no version written.
 * @param fm - checkpoint file manager used to manage streaming query checkpoint
 * @param fileToRead - name of file to use to read changelog
 * @param compressionCodec - de-compression method using for reading changelog file
 */
class StateStoreChangelogReaderFactory(
    fm: CheckpointFileManager,
    fileToRead: Path,
    compressionCodec: CompressionCodec) extends Logging {

  private def decompressStream(inputStream: DataInputStream): DataInputStream = {
    val compressed = compressionCodec.compressedInputStream(inputStream)
    new DataInputStream(compressed)
  }

  private lazy val sourceStream = try {
    fm.open(fileToRead)
  } catch {
    case f: FileNotFoundException =>
      throw QueryExecutionErrors.failedToReadStreamingStateFileError(fileToRead, f)
  }
  protected val input: DataInputStream = decompressStream(sourceStream)

  private lazy val changeLogVersion: Short = {
    try {
      val versionStr = input.readUTF()
      // Versions in the first line are prefixed with "v", e.g. "v2"
      // Since there is no version written for version 1,
      // return 1 if first line doesn't start with "v"
      if (!versionStr.startsWith("v")) {
        1
      } else {
        versionStr.stripPrefix("v").toShort
      }
    } catch {
      // When there is no record being written in the changelog file in V1,
      // the file contains a single int -1 meaning EOF, then the above readUTF()
      // throws with EOFException and we return version 1.
      case _: java.io.EOFException => 1
    }
  }

  /**
   * Construct the change log reader based on the version stored in changelog file
   * @return StateStoreChangelogReader
   */
  def constructChangelogReader(): StateStoreChangelogReader = {
    var reader: StateStoreChangelogReader = null
    try {
      reader = changeLogVersion match {
        case 1 => new StateStoreChangelogReaderV1(fm, fileToRead, compressionCodec)
        case 2 => new StateStoreChangelogReaderV2(fm, fileToRead, compressionCodec)
        case 3 => new StateStoreChangelogReaderV3(fm, fileToRead, compressionCodec)
        case 4 => new StateStoreChangelogReaderV4(fm, fileToRead, compressionCodec)
        case version => throw QueryExecutionErrors.invalidChangeLogReaderVersion(version)
      }
    } finally {
      if (input != null) {
        input.close()
        // input is not set to null because it is effectively lazy.
      }
    }
    reader
  }
}

/**
 * Base class for state store changelog reader
 * @param fm - checkpoint file manager used to manage streaming query checkpoint
 * @param fileToRead - name of file to use to read changelog
 * @param compressionCodec - de-compression method using for reading changelog file
 */
abstract class StateStoreChangelogReader(
    fm: CheckpointFileManager,
    fileToRead: Path,
    compressionCodec: CompressionCodec)
  extends NextIterator[(RecordType.Value, Array[Byte], Array[Byte])] with Logging {

  implicit val formats: Formats = DefaultFormats

  private def decompressStream(inputStream: DataInputStream): DataInputStream = {
    val compressed = compressionCodec.compressedInputStream(inputStream)
    new DataInputStream(compressed)
  }

  private lazy val sourceStream = try {
    fm.open(fileToRead)
  } catch {
    case f: FileNotFoundException =>
      throw QueryExecutionErrors.failedToReadStreamingStateFileError(fileToRead, f)
  }
  protected val input: DataInputStream = decompressStream(sourceStream)

  // This function is valid only when called upon initialization,
  // because version is written in the first line only for version >= 2.
  protected def readVersion(): String = input.readUTF()

  protected def verifyVersion(): Unit = {
    // ensure that the version read is correct, also updates file position
    val changelogVersionStr = readVersion()
    assert(changelogVersionStr == s"v${version}",
      s"Changelog version mismatch: $changelogVersionStr != v${version}")
  }

  private def readLineage(): Array[LineageItem] = {
    assert(version >= 3,
      "readLineage should only be invoked with state store checkpoint id enabled (version >= 3)")
    val lineageStr = input.readUTF()
    Serialization.read[Array[LineageItem]](lineageStr)
  }

  lazy val lineage: Array[LineageItem] = readLineage()

  def version: Short

  override protected def close(): Unit = { if (input != null) input.close() }

  override def getNext(): (RecordType.Value, Array[Byte], Array[Byte])
}

/**
 * Read an iterator of change record from the changelog file.
 * A record is represented by tuple(recordType: RecordType.Value,
 *  key: Array[Byte], value: Array[Byte])
 * A put record is returned as a tuple(recordType, key, value)
 * A delete record is return as a tuple(recordType, key, null)
 */
class StateStoreChangelogReaderV1(
    fm: CheckpointFileManager,
    fileToRead: Path,
    compressionCodec: CompressionCodec)
  extends StateStoreChangelogReader(fm, fileToRead, compressionCodec) {

  // Note that v1 does not record this value in the changelog file
  override def version: Short = 1

  override def getNext(): (RecordType.Value, Array[Byte], Array[Byte]) = {
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
        (RecordType.DELETE_RECORD, keyBuffer, null)
      } else {
        val valueBuffer = new Array[Byte](valueSize)
        ByteStreams.readFully(input, valueBuffer, 0, valueSize)
        // A put record.
        (RecordType.PUT_RECORD, keyBuffer, valueBuffer)
      }
    }
  }
}

/**
 * Read an iterator of change record from the changelog file.
 * A record is represented by tuple(recordType: RecordType.Value,
 * key: Array[Byte], value: Array[Byte])
 * A put or merge record is returned as a tuple(recordType, key, value)
 * A delete record is return as a tuple(recordType, key, null)
 */
class StateStoreChangelogReaderV2(
    fm: CheckpointFileManager,
    fileToRead: Path,
    compressionCodec: CompressionCodec)
  extends StateStoreChangelogReader(fm, fileToRead, compressionCodec) {

  private def parseBuffer(input: DataInputStream): Array[Byte] = {
    val blockSize = input.readInt()
    val blockBuffer = new Array[Byte](blockSize)
    ByteStreams.readFully(input, blockBuffer, 0, blockSize)
    blockBuffer
  }

  override def version: Short = 2

  verifyVersion()

  override def getNext(): (RecordType.Value, Array[Byte], Array[Byte]) = {
    val recordType = RecordType.getRecordTypeFromByte(input.readByte())
    // A EOF_RECORD means end of file.
    if (recordType == RecordType.EOF_RECORD) {
      finished = true
      null
    } else {
      recordType match {
        case RecordType.PUT_RECORD =>
          val keyBuffer = parseBuffer(input)
          val valueBuffer = parseBuffer(input)
          (RecordType.PUT_RECORD, keyBuffer, valueBuffer)

        case RecordType.DELETE_RECORD =>
          val keyBuffer = parseBuffer(input)
          val valueSize = input.readInt()
          assert(valueSize == -1)
          (RecordType.DELETE_RECORD, keyBuffer, null)

        case RecordType.MERGE_RECORD =>
          val keyBuffer = parseBuffer(input)
          val valueBuffer = parseBuffer(input)
          (RecordType.MERGE_RECORD, keyBuffer, valueBuffer)

        case _ =>
          throw new IOException("Failed to process unknown record type")
      }
    }
  }
}

/**
 * Read an iterator of change record from the changelog file.
 * A record is represented by tuple(recordType: RecordType.Value,
 *  key: Array[Byte], value: Array[Byte])
 * A put record is returned as a tuple(recordType, key, value)
 * A delete record is return as a tuple(recordType, key, null)
 * V3 is a extension of V1 for reading changelogs with version
 * in the first line and lineage in the second line.
 */
class StateStoreChangelogReaderV3(
     fm: CheckpointFileManager,
     fileToRead: Path,
     compressionCodec: CompressionCodec)
  extends StateStoreChangelogReaderV1(fm, fileToRead, compressionCodec) {

  override def version: Short = 3

  verifyVersion()

  // If the changelogFile is written when state store checkpoint unique id is enabled
  // the first line would be the version and the second line would be the lineage.
  // We should update the file position by reading from the lineage during
  // the reader initialization.
  lineage
}

/**
 * Read an iterator of change record from the changelog file.
 * A record is represented by tuple(recordType: RecordType.Value,
 * key: Array[Byte], value: Array[Byte])
 * A put or merge record is returned as a tuple(recordType, key, value)
 * A delete record is return as a tuple(recordType, key, null)
 * V4 is a extension of V2 for reading changelogs with version
 * in the first line and lineage in the second line.
 */
class StateStoreChangelogReaderV4(
     fm: CheckpointFileManager,
     fileToRead: Path,
     compressionCodec: CompressionCodec)
  extends StateStoreChangelogReaderV2(fm, fileToRead, compressionCodec) {

  override def version: Short = 4

  // If the changelogFile is written when state store checkpoint unique id is enabled
  // the first line would be the version and the second line would be the lineage.
  // We should update the file position by reading from the lineage during
  // the reader initialization.
  lineage
}

/**
 * Base class representing a iterator that iterates over a range of changelog files in a state
 * store. In each iteration, it will return a tuple of (changeType: [[RecordType]],
 * nested key: [[UnsafeRow]], nested value: [[UnsafeRow]], batchId: [[Long]])
 *
 * @param fm checkpoint file manager used to manage streaming query checkpoint
 * @param stateLocation location of the state store
 * @param startVersion start version of the changelog file to read
 * @param endVersion end version of the changelog file to read
 * @param compressionCodec de-compression method using for reading changelog file
 * @param colFamilyNameOpt optional column family name to read from
 */
abstract class StateStoreChangeDataReader(
    fm: CheckpointFileManager,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    compressionCodec: CompressionCodec,
    colFamilyNameOpt: Option[String] = None)
  extends NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)] with Logging {

  assert(startVersion >= 1)
  assert(endVersion >= startVersion)

  /**
   * Iterator that iterates over the changelog files in the state store.
   */
  private class ChangeLogFileIterator extends Iterator[Path] {

    private var currentVersion = StateStoreChangeDataReader.this.startVersion - 1

    /** returns the version of the changelog returned by the latest [[next]] function call */
    def getVersion: Long = currentVersion

    override def hasNext: Boolean = currentVersion < StateStoreChangeDataReader.this.endVersion

    override def next(): Path = {
      currentVersion += 1
      getChangelogPath(currentVersion)
    }

    private def getChangelogPath(version: Long): Path =
      new Path(
        StateStoreChangeDataReader.this.stateLocation,
        s"$version.${StateStoreChangeDataReader.this.changelogSuffix}")
  }

  /** file format of the changelog files */
  protected var changelogSuffix: String
  private lazy val fileIterator = new ChangeLogFileIterator
  private var changelogReader: StateStoreChangelogReader = null

  /**
   * Get a changelog reader that has at least one record left to read. If there is no readers left,
   * return null.
   */
  protected def currentChangelogReader(): StateStoreChangelogReader = {
    while (changelogReader == null || !changelogReader.hasNext) {
      if (changelogReader != null) {
        changelogReader.closeIfNeeded()
        changelogReader = null
      }
      if (!fileIterator.hasNext) {
        finished = true
        return null
      }

      changelogReader = if (colFamilyNameOpt.isDefined) {
        new StateStoreChangelogReaderV2(fm, fileIterator.next(), compressionCodec)
      } else {
        new StateStoreChangelogReaderV1(fm, fileIterator.next(), compressionCodec)
      }
    }
    changelogReader
  }

  /** get the version of the current changelog reader */
  protected def currentChangelogVersion: Long = fileIterator.getVersion

  override def close(): Unit = {
    if (changelogReader != null) {
      changelogReader.closeIfNeeded()
    }
  }
}
