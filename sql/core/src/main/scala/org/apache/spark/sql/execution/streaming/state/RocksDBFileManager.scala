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

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import scala.collection.Seq

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
 * Classes to represent metadata of checkpoints saved to DFS. Since this is converted to JSON, any
 * changes to this MUST be backward-compatible.
 */
case class RocksDBCheckpointMetadata(
    sstFiles: Seq[RocksDBSstFile],
    logFiles: Seq[RocksDBLogFile],
    numKeys: Long) {
  import RocksDBCheckpointMetadata._

  def json: String = {
    // We turn this field into a null to avoid write a empty logFiles field in the json.
    val nullified = if (logFiles.isEmpty) this.copy(logFiles = null) else this
    mapper.writeValueAsString(nullified)
  }

  def prettyJson: String = Serialization.writePretty(this)(RocksDBCheckpointMetadata.format)

  def writeToFile(metadataFile: File): Unit = {
    val writer = Files.newBufferedWriter(metadataFile.toPath, UTF_8)
    try {
      writer.write(s"v$VERSION\n")
      writer.write(this.json)
    } finally {
      writer.close()
    }
  }

  def immutableFiles: Seq[RocksDBImmutableFile] = sstFiles ++ logFiles
}

/** Helper class for [[RocksDBCheckpointMetadata]] */
object RocksDBCheckpointMetadata {
  val VERSION = 1

  implicit val format = Serialization.formats(NoTypeHints)

  /** Used to convert between classes and JSON. */
  lazy val mapper = {
    val _mapper = new ObjectMapper with ScalaObjectMapper
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }

  def readFromFile(metadataFile: File): RocksDBCheckpointMetadata = {
    val reader = Files.newBufferedReader(metadataFile.toPath, UTF_8)
    try {
      val versionLine = reader.readLine()
      if (versionLine != s"v$VERSION") {
        throw new IllegalStateException(
          s"Cannot read RocksDB checkpoint metadata of version $versionLine")
      }
      Serialization.read[RocksDBCheckpointMetadata](reader)
    } finally {
      reader.close()
    }
  }

  def apply(rocksDBFiles: Seq[RocksDBImmutableFile], numKeys: Long): RocksDBCheckpointMetadata = {
    val sstFiles = rocksDBFiles.collect { case file: RocksDBSstFile => file }
    val logFiles = rocksDBFiles.collect { case file: RocksDBLogFile => file }

    RocksDBCheckpointMetadata(sstFiles, logFiles, numKeys)
  }
}

/**
 * A RocksDBImmutableFile maintains a mapping between a local RocksDB file name and the name of
 * its copy on DFS. Since these files are immutable, their DFS copies can be reused.
 */
sealed trait RocksDBImmutableFile {
  def localFileName: String
  def dfsFileName: String
  def sizeBytes: Long

  /**
   * Whether another local file is same as the file described by this class.
   * A file is same only when the name and the size are same.
   */
  def isSameFile(otherFile: File): Boolean = {
    otherFile.getName == localFileName && otherFile.length() == sizeBytes
  }
}

/**
 * Class to represent a RocksDB SST file. Since this is converted to JSON,
 * any changes to these MUST be backward-compatible.
 */
private[sql] case class RocksDBSstFile(
    localFileName: String,
    dfsSstFileName: String,
    sizeBytes: Long) extends RocksDBImmutableFile {

  override def dfsFileName: String = dfsSstFileName
}

/**
 * Class to represent a RocksDB Log file. Since this is converted to JSON,
 * any changes to these MUST be backward-compatible.
 */
private[sql] case class RocksDBLogFile(
    localFileName: String,
    dfsLogFileName: String,
    sizeBytes: Long) extends RocksDBImmutableFile {

  override def dfsFileName: String = dfsLogFileName
}

object RocksDBImmutableFile {
  val SST_FILES_DFS_SUBDIR = "SSTs"
  val LOG_FILES_DFS_SUBDIR = "logs"
  val LOG_FILES_LOCAL_SUBDIR = "archive"

  def apply(localFileName: String, dfsFileName: String, sizeBytes: Long): RocksDBImmutableFile = {
    if (isSstFile(localFileName)) {
      RocksDBSstFile(localFileName, dfsFileName, sizeBytes)
    } else if (isLogFile(localFileName)) {
      RocksDBLogFile(localFileName, dfsFileName, sizeBytes)
    } else {
      null
    }
  }

  def isSstFile(fileName: String): Boolean = fileName.endsWith(".sst")

  def isLogFile(fileName: String): Boolean = fileName.endsWith(".log")

  private def isArchivedLogFile(file: File): Boolean =
    isLogFile(file.getName) && file.getParentFile.getName == LOG_FILES_LOCAL_SUBDIR

  def isImmutableFile(file: File): Boolean = isSstFile(file.getName) || isArchivedLogFile(file)
}
